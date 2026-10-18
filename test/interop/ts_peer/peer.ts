/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The peer process: the node with its listeners, `READY`, and the command
// loop on stdin (README, "Process protocol"), as dart_peer/lib/src/peer.dart
// runs it. Everything goes through the package's public Node entry.

import { createInterface } from 'node:readline';
import {
  type Logger,
  type LogLevel,
  type MuxConnection,
  nativeQuic,
  nativeQuicAvailability,
  QuicBindingConnection,
  QuicIdentityRules,
  StatusCode,
  Switchboard,
  selfSignedCertificate,
} from '../../../src/node/index.ts';
import { Client } from './client.ts';
import { type PeerConfig, peerConfig } from './config.ts';
import { PeerContext } from './context.ts';
import { channelPolicy, nodeIdentity } from './identity.ts';
import { MeshServices } from './mesh.ts';
import { decodeKind, encodeKind, oracleKinds } from './oracle.ts';
import {
  ArgumentProblem,
  arrayArg,
  bytesOf,
  errorJson,
  type Json,
  Output,
  objectArg,
  peerProtocolVersion,
  requiredString,
  statusArg,
} from './protocol.ts';
import { registerService } from './services.ts';

const usage = `Usage: ts_peer [--config <json>] [--log <level>] [--watchdog <seconds>]

The TS Switchboard as a test peer: prints READY {json} with its endpoints,
then answers JSON-lines commands on stdin (one response line each) and
reports events. Exits when stdin closes. See test/interop/README.md.`;

/** The log levels by their package:logging names (as the Dart peer takes them) and our own. */
const levels: Record<string, LogLevel> = {
  all: 'debug',
  finest: 'debug',
  finer: 'debug',
  fine: 'debug',
  config: 'debug',
  debug: 'debug',
  info: 'info',
  warning: 'warn',
  warn: 'warn',
  severe: 'error',
  shout: 'error',
  error: 'error',
};

/** A logger writing to stderr from [minimum] up; stdout carries the protocol only. */
function stderrLogger(minimum: LogLevel): Logger {
  const order: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };
  return {
    log(level, name, message, error) {
      if (order[level] < order[minimum]) {
        return;
      }
      const detail = error === undefined ? '' : ` ${error instanceof Error ? error.stack : error}`;
      process.stderr.write(
        `${new Date().toISOString()} ${level.toUpperCase()} ${name}: ${message}${detail}\n`,
      );
    },
  };
}

/** Runs the peer with the command line [args]. */
export async function runPeer(args: readonly string[]): Promise<void> {
  let configJson = '{}';
  let logLevel: string | undefined;
  let watchdogSeconds = 600;
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const value = (): string => {
      const v = args[++i];
      if (v === undefined) {
        throw new ArgumentProblem(`${arg} needs a value`);
      }
      return v;
    };
    switch (arg) {
      case 'serve':
        break;
      case '--config':
        configJson = value();
        break;
      case '--log':
        logLevel = value();
        break;
      case '--watchdog':
        watchdogSeconds = Number(value());
        break;
      case '--help':
      case '-h':
        process.stdout.write(`${usage}\n`);
        return;
      default:
        process.stderr.write(`ts_peer: unknown argument ${arg}\n${usage}\n`);
        process.exit(64);
    }
  }
  let logger: Logger | undefined;
  if (logLevel !== undefined) {
    const level = levels[logLevel.toLowerCase()];
    if (level === undefined) {
      throw new ArgumentProblem(`unknown log level ${logLevel}`);
    }
    logger = stderrLogger(level);
  }
  const config = peerConfig(JSON.parse(configJson) as Json);
  const ctx = new PeerContext(new Output(), config, logger);
  process.on('unhandledRejection', (e) => ctx.reportError(e));
  process.on('uncaughtException', (e) => ctx.reportError(e));
  await new Peer(ctx, watchdogSeconds).run();
}

class Peer {
  readonly #ctx: PeerContext;
  readonly #watchdogSeconds: number;
  readonly #client: Client;
  #node!: Switchboard;
  #mesh: MeshServices | undefined;
  #ready: Json = {};
  #stopping = false;
  /** The connections the listeners accepted that have not ended, in the order accepted. */
  readonly #accepted = new Set<MuxConnection>();

  constructor(ctx: PeerContext, watchdogSeconds: number) {
    this.#ctx = ctx;
    this.#watchdogSeconds = watchdogSeconds;
    this.#client = new Client(ctx);
  }

  async run(): Promise<void> {
    const watchdog = setTimeout(() => {
      process.stderr.write(`ts_peer: watchdog after ${this.#watchdogSeconds} s, exiting\n`);
      process.exit(3);
    }, this.#watchdogSeconds * 1000);
    watchdog.unref();
    const ctx = this.#ctx;
    const id = ctx.config.identity;
    const identity = await nodeIdentity(id);
    const node = new Switchboard({
      localConnections: false,
      muxOptions: ctx.config.mux,
      talkOptions: ctx.config.talk,
      credential: identity.credential,
      holderKey: identity.holderKey,
      verifier: identity.verifier,
      identifyOutgoing: id.identifyOutgoing,
      identityTimeout: id.identityTimeout,
      // A proxy, a relay or a naming client of this node dials QUIC and
      // WebTransport endpoints too.
      ...(nativeQuicAvailability().available
        ? { quic: nativeQuic(), quicBinding: { webTransportCertificateVerified: false } }
        : {}),
      ...(ctx.logger === undefined ? {} : { logger: ctx.logger }),
    });
    this.#node = node;
    this.#client.identify(identity);
    // Accepted connections are reported once their peer identified; with
    // identifyInReturn the node identifies in return on those whose peer
    // asked (the QUIC listener does so itself).
    void (async () => {
      for await (const c of node.connections) {
        if (!c.isInitiator) {
          this.#accepted.add(c);
          void c.done.then(() => this.#accepted.delete(c));
        }
        void c.peerIdentified.then(() => ctx.trackConnection(c, node));
        if (
          id.identifyInReturn &&
          identity.credential !== undefined &&
          !(c instanceof QuicBindingConnection)
        ) {
          void c.identityRequested.then(() => node.identifyOn(c).catch(() => {}));
        }
      }
    })();
    for (const s of this.#ctx.config.services) {
      registerService(this.#node, s, this.#ctx, this.#client);
    }
    this.#mesh = new MeshServices(node, ctx.config.mesh);
    this.#ready = {
      protocol: peerProtocolVersion,
      implementation: 'ts',
      ...(await this.#listen(this.#ctx.config)),
      pid: process.pid,
      oracle: oracleKinds,
    };
    this.#mesh.listening(ctx.endpoints);
    this.#ctx.out.line(`READY ${JSON.stringify(this.#ready)}`);
    for (const signal of ['SIGTERM', 'SIGINT'] as const) {
      process.on(signal, () => void this.#stop());
    }
    const lines = createInterface({ input: process.stdin, crlfDelay: Number.POSITIVE_INFINITY });
    lines.on('line', (line) => {
      if (line.trim().length > 0) {
        void this.#handle(line);
      }
    });
    lines.on('close', () => {
      clearTimeout(watchdog);
      void this.#stop();
    });
  }

  async #listen(config: PeerConfig): Promise<Json> {
    const node = this.#node;
    const loopback = '127.0.0.1';
    const endpoints: Json = {};
    const own = this.#ctx.endpoints;
    const policy = channelPolicy(config.identity);
    if (config.listen.includes('tcp')) {
      endpoints['tcp'] = await node.listenTcp(loopback, 0, { policy });
      own.push(endpoints['tcp'] as string);
    }
    if (config.listen.includes('ws')) {
      endpoints['ws'] = await node.listenWebSocket(loopback, 0, { path: config.path, policy });
      own.push(endpoints['ws'] as string);
    }
    if (config.listen.includes('quic') || config.listen.includes('webtransport')) {
      // One self-signed certificate for both, as the Dart peer has; nobody
      // is asked to identify (QuicIdentityRules.none, as the Dart peer).
      const certificate = selfSignedCertificate(loopback);
      const rules = config.identity.quicRules;
      if (rules !== 'none' && rules !== 'listener') {
        throw new ArgumentProblem(`unknown QUIC identity rules ${rules}`);
      }
      const options = {
        certificate,
        provider: nativeQuic(),
        policy,
        identityRules: rules === 'none' ? QuicIdentityRules.none : QuicIdentityRules.listener,
        identifyInReturn: config.identity.identifyInReturn,
      };
      if (config.listen.includes('quic')) {
        endpoints['quic'] = await node.listenQuic(loopback, 0, options);
        own.push(endpoints['quic'] as string);
      }
      if (config.listen.includes('webtransport')) {
        endpoints['webtransport'] = await node.listenWebTransport(loopback, 0, {
          ...options,
          path: config.path,
        });
        own.push(endpoints['webtransport'] as string);
      }
      endpoints['certificateSha256'] = Array.from(certificate.sha256, (b) =>
        b.toString(16).padStart(2, '0'),
      ).join('');
    }
    return endpoints;
  }

  async #handle(line: string): Promise<void> {
    const out = this.#ctx.out;
    let id: unknown;
    try {
      const cmd = JSON.parse(line) as Json;
      id = cmd['id'];
      const op = requiredString(cmd, 'op');
      if (op === 'shutdown') {
        out.respond(id, null);
        await this.#stop();
        return;
      }
      out.respond(id, await this.#command(op, cmd));
    } catch (e) {
      out.fail(id, e);
    }
  }

  async #command(op: string, cmd: Json): Promise<unknown> {
    switch (op) {
      case 'ping':
        return 'pong';
      case 'info':
        return { ...this.#ready, handles: this.#ctx.handles.length };
      case 'reset':
        return { closed: await this.#ctx.closeAll() };
      case 'decode':
        return decodeKind(requiredString(cmd, 'kind'), bytesOf(cmd['hex']));
      case 'decodeMany': {
        const kind = requiredString(cmd, 'kind');
        return (arrayArg(cmd, 'hexes') ?? []).map((h) => {
          try {
            return { ok: true, value: decodeKind(kind, bytesOf(h)) };
          } catch (e) {
            return { ok: false, error: errorJson(e) };
          }
        });
      }
      case 'encode':
        return encodeKind(requiredString(cmd, 'kind'), objectArg(cmd, 'fields') ?? {});
      case 'accepted.goAway': {
        const status = statusArg(cmd, StatusCode.goingAway);
        return { connections: this.#eachAccepted((c) => c.goAway(status)) };
      }
      case 'accepted.close':
        return { connections: this.#eachAccepted((c) => c.close()) };
      default:
        if (Client.answers(op)) {
          return this.#client.command(op, cmd);
        }
        throw new ArgumentProblem(`unknown op ${op}`);
    }
  }

  /**
   * Runs [action] on every open connection the listeners accepted, without
   * waiting for it, and answers their handles: each is tracked from then
   * on, its GOAWAY and its end reported, whether or not its peer
   * identified.
   */
  #eachAccepted(action: (c: MuxConnection) => Promise<void>): string[] {
    const handles: string[] = [];
    for (const c of this.#accepted) {
      if (c.isOpen) {
        handles.push(this.#ctx.trackConnection(c, this.#node));
        // Throws at once for a status a GOAWAY cannot carry; settles once the connection ended.
        action(c).catch(() => {});
      }
    }
    return handles;
  }

  async #stop(): Promise<void> {
    if (this.#stopping) {
      return;
    }
    this.#stopping = true;
    setTimeout(() => process.exit(0), 5000).unref();
    try {
      await this.#ctx.closeAll();
      await this.#client.close();
      await this.#mesh?.close();
      await this.#node.close();
    } catch (e) {
      process.stderr.write(`ts_peer: shutdown: ${e instanceof Error ? e.stack : e}\n`);
    }
    await this.#ctx.out.flush();
    process.exit(0);
  }
}
