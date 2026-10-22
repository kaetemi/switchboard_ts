/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Spawns a peer process, waits for its READY line and speaks the JSON-lines
// protocol with it (README, "Process protocol"). Implementation neutral: the
// Dart peer and the TS peer are both a command line.

import { type ChildProcessByStdio, spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import type { Readable, Writable } from 'node:stream';
import {
  type PeerConfig,
  PeerError,
  type PeerErrorInfo,
  type PeerEvent,
  peerProtocolVersion,
  type ReadyInfo,
  type Response,
  type Transport,
} from './protocol.ts';

/** How to run a peer: an executable and its arguments. */
export interface PeerCommand {
  /** A name for messages, such as `dart` or `ts`. */
  readonly label: string;
  readonly command: string;
  readonly args?: readonly string[];
}

/** Options of {@link Peer.start}. */
export interface StartOptions {
  /** The peer's configuration (`--config`). */
  config?: PeerConfig;
  /** A log level for the peer's stderr, such as `FINE` (Dart `package:logging` names). */
  log?: string;
  /** How long the peer may live before its watchdog ends it; default 600 s. */
  watchdogSeconds?: number;
  /** How long to wait for READY; default 60 s. */
  readyTimeoutMs?: number;
}

/** What an event must be: its name, a predicate, or fields it must carry. */
export type EventMatch =
  | string
  | ((event: PeerEvent) => boolean)
  | { readonly event: string; readonly [field: string]: unknown };

/** Whether [event] satisfies [match]. */
export function eventMatches(event: PeerEvent, match: EventMatch): boolean {
  if (typeof match === 'string') {
    return event.event === match;
  }
  if (typeof match === 'function') {
    return match(event);
  }
  return Object.entries(match).every(
    ([key, value]) => JSON.stringify(event[key]) === JSON.stringify(value),
  );
}

/** The events a peer reported, in order, and waiting for the next one. */
export class EventLog {
  readonly #events: PeerEvent[] = [];
  readonly #waiters = new Set<{
    match: EventMatch;
    resolve: (e: PeerEvent) => void;
    reject: (e: Error) => void;
  }>();
  #closed: Error | undefined;

  /** Records [event] and wakes the waiters it satisfies. */
  push(event: PeerEvent): void {
    this.#events.push(event);
    for (const w of this.#waiters) {
      if (eventMatches(event, w.match)) {
        this.#waiters.delete(w);
        w.resolve(event);
      }
    }
  }

  /** Every event so far. */
  get all(): readonly PeerEvent[] {
    return this.#events;
  }

  /** The events so far that satisfy [match]; `T` names their shape. */
  filter<T extends PeerEvent = PeerEvent>(match: EventMatch): T[] {
    return this.#events.filter((e) => eventMatches(e, match)) as T[];
  }

  /**
   * The first event that satisfies [match], already logged or yet to come;
   * `T` names its shape. Rejects after [timeoutMs] or when the peer exits
   * first.
   */
  waitFor<T extends PeerEvent = PeerEvent>(match: EventMatch, timeoutMs = 10_000): Promise<T> {
    const found = this.#events.find((e) => eventMatches(e, match));
    if (found !== undefined) {
      return Promise.resolve(found as T);
    }
    if (this.#closed !== undefined) {
      return Promise.reject(this.#closed);
    }
    return new Promise((resolve, reject) => {
      const waiter = {
        match,
        resolve: (e: PeerEvent) => {
          clearTimeout(timer);
          resolve(e as T);
        },
        reject: (e: Error) => {
          clearTimeout(timer);
          reject(e);
        },
      };
      const timer = setTimeout(() => {
        this.#waiters.delete(waiter);
        reject(new Error(`no event ${describe(match)} within ${timeoutMs} ms`));
      }, timeoutMs);
      this.#waiters.add(waiter);
    });
  }

  /** Waits until [count] events satisfy [match] and answers them. */
  async waitForCount<T extends PeerEvent = PeerEvent>(
    match: EventMatch,
    count: number,
    timeoutMs = 10_000,
  ): Promise<T[]> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const got = this.filter<T>(match);
      if (got.length >= count) {
        return got.slice(0, count);
      }
      const left = deadline - Date.now();
      if (left <= 0) {
        throw new Error(
          `${got.length} of ${count} events ${describe(match)} within ${timeoutMs} ms`,
        );
      }
      const seen = got.length;
      await this.waitFor((e) => eventMatches(e, match) && this.filter(match).length > seen, left);
    }
  }

  /** Forgets the events so far. */
  clear(): void {
    this.#events.length = 0;
  }

  /** Fails the waiters, and later waits, with [error]. */
  close(error: Error): void {
    this.#closed = error;
    for (const w of this.#waiters) {
      w.reject(error);
    }
    this.#waiters.clear();
  }
}

function describe(match: EventMatch): string {
  return typeof match === 'function' ? '(predicate)' : JSON.stringify(match);
}

const maxStderr = 256 * 1024;

/** A running peer process. */
export class Peer {
  /** What the peer printed in READY. */
  readonly ready: ReadyInfo;
  /** The events it reported. */
  readonly events: EventLog;
  /** The command it runs. */
  readonly command: PeerCommand;

  readonly #child: ChildProcessByStdio<Writable, Readable, Readable>;
  readonly #pending: Map<number, { op: string; settle: (r: Response) => void }>;
  readonly #exited: Promise<number | null>;
  readonly #stderr: { text: string };
  readonly #noise: string[];
  #nextId = 1;
  #exitCode: number | null | undefined;

  private constructor(
    command: PeerCommand,
    child: ChildProcessByStdio<Writable, Readable, Readable>,
    ready: ReadyInfo,
    state: {
      events: EventLog;
      pending: Map<number, { op: string; settle: (r: Response) => void }>;
      exited: Promise<number | null>;
      stderr: { text: string };
      noise: string[];
    },
  ) {
    this.command = command;
    this.#child = child;
    this.ready = ready;
    this.events = state.events;
    this.#pending = state.pending;
    this.#exited = state.exited;
    this.#stderr = state.stderr;
    this.#noise = state.noise;
    void this.#exited.then((code) => {
      this.#exitCode = code;
    });
  }

  /** Starts [command] and resolves once it printed READY. */
  static start(command: PeerCommand, options: StartOptions = {}): Promise<Peer> {
    const args = [...(command.args ?? [])];
    if (options.config !== undefined) {
      args.push('--config', JSON.stringify(options.config));
    }
    if (options.log !== undefined) {
      args.push('--log', options.log);
    }
    args.push('--watchdog', String(options.watchdogSeconds ?? 600));
    const child = spawn(command.command, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    const events = new EventLog();
    const pending = new Map<number, { op: string; settle: (r: Response) => void }>();
    const stderr = { text: '' };
    const noise: string[] = [];
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk: string) => {
      stderr.text = (stderr.text + chunk).slice(-maxStderr);
    });
    const exited = new Promise<number | null>((resolve) => {
      child.on('exit', (code) => resolve(code));
      child.on('error', () => resolve(null));
    });
    return new Promise<Peer>((resolve, reject) => {
      let peer: Peer | undefined;
      const timer = setTimeout(() => {
        child.kill('SIGKILL');
        reject(
          new Error(
            `${command.label} peer: no READY within ${options.readyTimeoutMs ?? 60_000} ms\n${stderr.text}`,
          ),
        );
      }, options.readyTimeoutMs ?? 60_000);
      child.on('error', (e) => {
        clearTimeout(timer);
        reject(new Error(`${command.label} peer: cannot run ${command.command}: ${e.message}`));
      });
      void exited.then((code) => {
        const error = new Error(
          `${command.label} peer exited with code ${code}\n${stderr.text.slice(-4096)}`,
        );
        clearTimeout(timer);
        if (peer === undefined) {
          reject(error);
        }
        events.close(error);
        for (const [id, p] of pending) {
          pending.delete(id);
          p.settle({
            id,
            ok: false,
            error: { kind: 'other', type: 'PeerExited', message: error.message },
          });
        }
      });
      const lines = createInterface({ input: child.stdout });
      lines.on('line', (line) => {
        if (peer === undefined) {
          // `dart run` prints build progress without a newline before READY.
          const at = line.indexOf('READY ');
          if (at < 0) {
            noise.push(line);
            return;
          }
          const ready = JSON.parse(line.slice(at + 6)) as ReadyInfo;
          if (ready.protocol !== peerProtocolVersion) {
            child.kill('SIGKILL');
            reject(
              new Error(
                `${command.label} peer speaks protocol ${ready.protocol}, not ${peerProtocolVersion}`,
              ),
            );
            return;
          }
          clearTimeout(timer);
          peer = new Peer(command, child, ready, { events, pending, exited, stderr, noise });
          resolve(peer);
          return;
        }
        if (!line.startsWith('{')) {
          noise.push(line);
          return;
        }
        const message = JSON.parse(line) as Response | PeerEvent;
        if ('event' in message) {
          events.push(message);
          return;
        }
        const p = pending.get(message.id);
        if (p !== undefined) {
          pending.delete(message.id);
          p.settle(message);
        }
      });
    });
  }

  /** The implementation that answered READY. */
  get implementation(): ReadyInfo['implementation'] {
    return this.ready.implementation;
  }

  /** The endpoint the peer listens on for [transport]. */
  endpoint(transport: Transport): string {
    const e = this.ready[transport];
    if (e === undefined) {
      throw new Error(`${this.command.label} peer does not listen on ${transport}`);
    }
    return e;
  }

  /**
   * Sends command [op] with [args] and resolves with its value; rejects with
   * a {@link PeerError} when the peer answers with an error.
   */
  rpc<T = unknown>(op: string, args: Record<string, unknown> = {}, timeoutMs = 30_000): Promise<T> {
    const id = this.#nextId++;
    return new Promise<T>((resolve, reject) => {
      if (this.#exitCode !== undefined) {
        reject(new Error(`${this.command.label} peer has exited (${this.#exitCode})`));
        return;
      }
      const timer = setTimeout(() => {
        this.#pending.delete(id);
        reject(new Error(`${this.command.label} peer: ${op} unanswered after ${timeoutMs} ms`));
      }, timeoutMs);
      this.#pending.set(id, {
        op,
        settle: (r) => {
          clearTimeout(timer);
          if (r.ok) {
            resolve(r.value as T);
          } else {
            reject(new PeerError(op, r.error));
          }
        },
      });
      this.#child.stdin.write(`${JSON.stringify({ ...args, id, op })}\n`);
    });
  }

  /**
   * Sends [op] and resolves with the error the peer answers; rejects when
   * the command succeeds instead.
   */
  async rpcError(op: string, args: Record<string, unknown> = {}): Promise<PeerErrorInfo> {
    try {
      const value = await this.rpc(op, args);
      throw new Error(`${op} succeeded with ${JSON.stringify(value)}, an error was expected`);
    } catch (e) {
      if (e instanceof PeerError) {
        return e.info;
      }
      throw e;
    }
  }

  /**
   * Closes what the previous test created and forgets the events: the
   * `reset` command, which closes the connections the peer dialled and only
   * forgets those it accepted, leaving them to their dialler.
   */
  async reset(): Promise<void> {
    await this.rpc('reset');
    this.events.clear();
  }

  /** What the peer wrote to stderr and to stdout besides the protocol. */
  diagnostics(): string {
    const noise = this.#noise.length > 0 ? `\nstdout:\n${this.#noise.join('\n')}` : '';
    return `${this.command.label} peer (pid ${this.ready.pid}) stderr:\n${this.#stderr.text}${noise}`;
  }

  /** The `peer.error` events: errors no command answered. */
  get errors(): PeerEvent[] {
    return this.events.filter('peer.error');
  }

  /**
   * Stops the peer: `shutdown`, then the end of stdin, then SIGKILL after
   * [graceMs]. Resolves with the exit code.
   */
  async stop(graceMs = 5_000): Promise<number | null> {
    if (this.#exitCode === undefined) {
      this.rpc('shutdown', {}, graceMs).catch(() => {});
      this.#child.stdin.end();
      const timer = setTimeout(() => this.#child.kill('SIGKILL'), graceMs);
      await this.#exited;
      clearTimeout(timer);
    }
    return this.#exited;
  }
}
