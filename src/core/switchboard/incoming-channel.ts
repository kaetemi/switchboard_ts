/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ChannelAddress } from '../address/index.ts';
import type { PeerIdentity } from '../identity/contract.ts';
import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import { Status, StatusCode } from '../status.ts';
import { type TalkOptions, talkOptions } from '../talk/contract.ts';
import type { TalkChannel } from '../talk/talk-channel.ts';
import type { IncomingChannel as IncomingChannelContract } from './contract.ts';
import { talkOver } from './talk-layer.ts';

/**
 * A channel opened by a peer, as delivered to a `ChannelHandler`: the raw
 * mux channel, the parsed address header with the application payload, and
 * the connection it arrived on (on which the handler may open channels back
 * to the peer: dispatch is symmetric).
 *
 * The handler owns the channel: it reads {@link channel} (or wraps it with
 * {@link talk}) and closes it when done, or rejects it with {@link reject}
 * after inspecting the address and application payload.
 */
export class IncomingChannel implements IncomingChannelContract {
  /** The mux channel. Its subframes wait until read. */
  readonly channel: MuxChannel;

  /** The parsed address header and application payload of the OPEN. */
  readonly address: ChannelAddress;

  /**
   * A description of the remote side, for logs: the remote address of an
   * accepted connection or the endpoint of an initiated one; `local` on the
   * accepted side of a connection the node made to one of its own
   * listeners. Undefined when unknown.
   */
  readonly remote: string | undefined;

  /** Defaults for {@link talk}; undefined: the Talk defaults. */
  readonly talkOptions: Partial<TalkOptions> | undefined;

  #talk: TalkChannel | undefined;

  /**
   * Wraps [channel]. [address] defaults to the parsed OPEN payload of
   * [channel] (throwing `ProtocolError` when it is malformed). The node
   * creates these; the constructor is public so that handlers can be driven
   * directly in tests.
   */
  constructor(
    channel: MuxChannel,
    options: {
      address?: ChannelAddress | undefined;
      talkOptions?: Partial<TalkOptions> | undefined;
      remote?: string | undefined;
    } = {},
  ) {
    this.channel = channel;
    this.address = options.address ?? ChannelAddress.decode(channel.openPayload);
    this.talkOptions = options.talkOptions;
    this.remote = options.remote;
  }

  /** The connection the channel arrived on. */
  get connection(): MuxConnection {
    return this.channel.connection;
  }

  /**
   * Who the peer of {@link connection} proved to be with IDENT. A channel
   * proxied through an endpoint arrives on the proxy's connection, so this
   * is the proxy's identity, not the client's: clients present their
   * credentials in the application payload.
   */
  get peerIdentity(): PeerIdentity | undefined {
    return this.connection.peerIdentity;
  }

  /**
   * The channel wrapped in the Talk layer: wraps on the first call and
   * returns the same Talk channel afterwards; [options] (default: the
   * node's Talk options) only apply to the first call. Do not also read
   * {@link channel} directly once this has been called.
   */
  talk(options?: Partial<TalkOptions>): TalkChannel {
    this.#talk ??= talkOver(this.channel, talkOptions({ ...this.talkOptions, ...options }));
    return this.#talk;
  }

  /**
   * Rejects or ends the channel with CLOSE carrying [status]. If {@link talk}
   * was called, the Talk channel is closed with [status] so that its
   * outstanding requests fail too. Application codes (256 and above) must
   * not appear in a mux CLOSE and are sent as `UNKNOWN`. Does nothing more
   * if the channel is closing already. Resolves when the channel is closed;
   * never rejects.
   */
  reject(status: Status): Promise<void> {
    const wire = status.isApplicationCode
      ? Status.of(
          StatusCode.unknown,
          `application status ${status.code}${status.reason.length === 0 ? '' : `: ${status.reason}`}`,
        )
      : status;
    const talk = this.#talk;
    const closing = talk !== undefined ? talk.close(wire) : this.channel.close(wire);
    return closing.then(
      () => {},
      () => {},
    );
  }

  toString(): string {
    return `IncomingChannel(${this.channel.id}, ${this.address}${this.remote !== undefined ? ` from ${this.remote}` : ''})`;
  }
}
