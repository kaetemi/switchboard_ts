/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// What the scenario files share: opening Talk channels through the client
// peer, payload helpers, and the services every server peer has.

import { createHash } from 'node:crypto';
import { expect } from 'vitest';
import { hexBytes, hexString, utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import type { CellPeers } from '../matrix.ts';
import type { ChannelIncomingEvent, PeerEvent, ServiceConfig, Transport } from '../protocol.ts';

/** [text] as the peers write bytes: upper-case hex. */
export const hex = (text: string): string => hexString(utf8Encode(text));

/** The UTF-8 text of [hexText], bytes as the peers write them. */
export const text = (hexText: string | undefined): string => utf8Decode(hexBytes(hexText ?? ''));

/** The pattern `BIG` answers: byte i is `(i * 31) & 0xFF`. */
export function pattern(length: number): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    out[i] = (i * 31) & 0xff;
  }
  return out;
}

/** The SHA-256 of [bytes], lower-case hex. */
export function sha256(bytes: Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex');
}

/** The default services of a peer: `echo`, `raw` and `sink` at instance 1. */
export const defaultServices: readonly ServiceConfig[] = [
  { type: 'echo', behaviour: 'echo', instance: 1 },
  { type: 'raw', behaviour: 'raw', instance: 1 },
  { type: 'sink', behaviour: 'sink', instance: 1 },
];

/** A Talk channel from the client peer to [type] at the server, on a connection of its own. */
export async function openTalk(
  peers: CellPeers,
  type = 'echo',
  connection?: string,
): Promise<{ talk: string; channel: string; connection: string }> {
  return peers.client.rpc('talk.open', {
    ...(connection === undefined ? { endpoint: peers.endpoint } : { connection }),
    address: { type },
  });
}

/** A connection from the client peer to the server. */
export async function connect(peers: CellPeers): Promise<string> {
  const { connection } = await peers.client.rpc<{ connection: string }>('connect', {
    endpoint: peers.endpoint,
  });
  return connection;
}

/**
 * A Talk channel from the client to the server's `raw` service: the client
 * speaks Talk through its implementation, the server's side is a raw channel
 * whose subframes the test reads and writes byte by byte.
 */
export async function rawTalk(
  peers: CellPeers,
): Promise<{ talk: string; channel: string; far: string; farConnection: string }> {
  const opened = await openTalk(peers, 'raw');
  const incoming = await peers.server.events.waitFor<ChannelIncomingEvent>('channel.incoming');
  return {
    talk: opened.talk,
    channel: opened.channel,
    far: incoming.channel,
    farConnection: incoming.connection,
  };
}

/** The next subframe the server's raw channel [far] received after [seen] of them. */
export async function nextSubframe(peers: CellPeers, far: string, seen: number): Promise<string> {
  const [event] = (
    await peers.server.events.waitForCount({ event: 'channel.data', channel: far }, seen + 1)
  ).slice(seen);
  return String(event?.['hex']);
}

/**
 * Expects the `channel.closed` (or `persistent.closed`) [event] of a channel
 * that was open when the far side ended the connection to carry [code], the
 * connection's status. Over WebTransport it may carry `UNKNOWN` "no
 * application error code" instead: the session's `WT_SESSION_GONE` reset
 * has no code, and a receiver that reads it before the session's close
 * ends the channel `UNKNOWN` (the QUIC binding page: such a reset "reads as
 * UNKNOWN, unless the connection is ending"). The side ending the session
 * holds the resets back until its close is acknowledged, which makes this
 * rare, not impossible: seen in every pairing, Dart to Dart too.
 */
export function expectEndedWith(event: PeerEvent, code: number, transport: Transport): void {
  if (transport === 'webtransport' && event['code'] === StatusCode.unknown) {
    expect(event['reason']).toBe('no application error code');
  } else {
    expect(event['code']).toBe(code);
  }
}
