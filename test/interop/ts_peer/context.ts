/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The state a peer's commands and services share, as
// dart_peer/lib/src/context.dart has it: the output, the handles and the
// tracking that turns what the node observes into events.

import type {
  Logger,
  MuxChannel,
  MuxConnection,
  TalkChannel,
  TalkStream,
} from '../../../src/node/index.ts';
import type { PeerConfig } from './config.ts';
import { errorJson, Handles, hex, type Output, statusJson } from './protocol.ts';

/** What the handles name, by prefix. */
export interface HandleKinds extends Record<string, object> {
  /** Connections. */
  c: MuxConnection;
  /** Raw channels. */
  ch: MuxChannel;
  /** Talk channels. */
  t: TalkChannel;
  /** Stream requests. */
  s: TalkStream;
}

const closeTimeoutMs = 2000;

/** [promise], or nothing after [ms]; never rejects. */
async function within(promise: Promise<unknown>, ms: number): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  await Promise.race([
    promise.then(
      () => {},
      () => {},
    ),
    new Promise<void>((resolve) => {
      timer = setTimeout(resolve, ms);
    }),
  ]);
  clearTimeout(timer);
}

/** Shared state of one peer process. */
export class PeerContext {
  /** Where responses and events go. */
  readonly out: Output;
  /** The configuration from the command line. */
  readonly config: PeerConfig;
  /** The library's diagnostics, with `--log`. */
  readonly logger: Logger | undefined;
  /** Every object a command or a service created. */
  readonly handles = new Handles<HandleKinds>();
  /** The endpoints this peer listens on, in the order of `listen`. */
  readonly endpoints: string[] = [];

  constructor(out: Output, config: PeerConfig, logger: Logger | undefined) {
    this.out = out;
    this.config = config;
    this.logger = logger;
  }

  /** Registers [connection] and reports its GOAWAY and its end as events. */
  trackConnection(connection: MuxConnection): string {
    const existing = this.handles.nameOf(connection);
    if (existing !== undefined) {
      return existing;
    }
    const handle = this.handles.add('c', connection);
    void connection.peerGoAwayStatus.then((s) =>
      this.out.event('connection.goAway', { connection: handle, ...statusJson(s) }),
    );
    void connection.done.then((s) =>
      this.out.event('connection.closed', { connection: handle, ...statusJson(s) }),
    );
    return handle;
  }

  /**
   * Registers [channel] and reports its end, and with [listen] its
   * subframes, as events. A channel a Talk channel reads is tracked without
   * [listen].
   */
  trackChannel(channel: MuxChannel, listen = true): string {
    const handle = this.handles.add('ch', channel);
    if (listen) {
      this.reportData(channel, handle);
    }
    void channel.done.then((s) =>
      this.out.event('channel.closed', { channel: handle, ...statusJson(s) }),
    );
    return handle;
  }

  /** Reads [channel] and reports each subframe as `channel.data`. */
  reportData(channel: MuxChannel, handle: string): void {
    void (async () => {
      try {
        for await (const subframe of channel.readable) {
          this.out.event('channel.data', { channel: handle, hex: hex(subframe) });
        }
      } catch {
        // The end is reported by channel.closed.
      }
    })();
  }

  /**
   * Reports an error no command answers (a service's, say) as a
   * `peer.error` event and on stderr, so a test can assert there were none.
   */
  reportError(error: unknown): void {
    const stack = error instanceof Error ? (error.stack ?? `${error}`) : String(error);
    process.stderr.write(`ts_peer: unhandled ${stack}\n`);
    this.out.event('peer.error', { error: errorJson(error) });
  }

  /** Closes every handle's object, newest first, and forgets them all. */
  async closeAll(): Promise<number> {
    const objects = this.handles.all;
    this.handles.clear();
    for (const { prefix, object } of objects) {
      try {
        switch (prefix) {
          case 't':
            await within((object as TalkChannel).close(), closeTimeoutMs);
            break;
          case 'ch':
            await within((object as MuxChannel).close(), closeTimeoutMs);
            break;
          case 'c':
            await within((object as MuxConnection).close(), closeTimeoutMs);
            break;
          case 's':
            (object as TalkStream).cancel();
            break;
        }
      } catch {
        // Already closed or failing: nothing left to release.
      }
    }
    return objects.length;
  }
}
