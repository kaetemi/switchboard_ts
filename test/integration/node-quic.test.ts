/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Two Node nodes end to end over raw QUIC on the loopback, through the
// native provider: listenQuic (ALPN switchboard, a self-signed
// certificate) and the quic option's QuicSwitchboardBinding. Raw
// channels, and Talk requests with a bulk echo.
//
// Without the addon (`npm run build:native`, or SWITCHBOARD_QUIC_ADDON)
// the file is skipped with the reason; with SWITCHBOARD_NATIVE=required a
// missing addon fails it.

import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/index.ts';
import type { MuxConnection } from '../../src/core/mux/contract.ts';
import { Name } from '../../src/core/name.ts';
import { QuicBindingConnection } from '../../src/core/quic/quic-binding-connection.ts';
import { StatusCode } from '../../src/core/status.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import { EndpointResolver } from '../../src/core/switchboard/resolver.ts';
import {
  nativeQuic,
  nativeQuicAvailability,
  selfSignedCertificate,
} from '../../src/node/quic/index.ts';
import { rejectsWith, tagged, tagOf } from '../support/node/harness.ts';
import { echo, exerciseTalk, node, roundTrip, talkService, within } from './node-support.ts';

const availability = nativeQuicAvailability();
if (!availability.available && process.env['SWITCHBOARD_NATIVE'] === 'required') {
  throw new Error(`SWITCHBOARD_NATIVE=required, but ${availability.reason}`);
}

const svc = Name.of('svc');

describe.skipIf(!availability.available)(
  `two nodes over raw QUIC${availability.available ? '' : ` (skipped: ${availability.reason})`}`,
  () => {
    test('a client opens a channel to a service of a server', async () => {
      const server = node({ quic: nativeQuic() });
      server.registerService('echo', echo);
      const endpoint = await server.listenQuic('127.0.0.1', 0);
      expect(endpoint).toMatch(/^quic:\/\/127\.0\.0\.1:\d+$/);
      expect(server.listeningEndpoints).toEqual([endpoint]);
      expect(server.isOwnEndpoint(endpoint)).toBe(true);
      const client = node({ quic: nativeQuic(), resolver: new EndpointResolver(endpoint) });
      const channel = await within(client.openChannel('echo'));
      expect(channel.connection).toBeInstanceOf(QuicBindingConnection);
      expect(await roundTrip(channel, 'over QUIC')).toBe('over QUIC');
      const big = 'x'.repeat(16 * 1024);
      expect(await roundTrip(channel, big)).toBe(big);
      await channel.close();
      expect((await channel.done).isOk).toBe(true);
    });

    test('Talk requests over raw QUIC', async () => {
      const server = node({ quic: nativeQuic() });
      server.registerService('echo', talkService);
      const endpoint = await server.listenQuic('127.0.0.1', 0);
      const client = node({ quic: nativeQuic(), resolver: new EndpointResolver(endpoint) });
      const talk = await within(
        client.openTalk('echo', { talkOptions: { bulkThreshold: 64 * 1024 } }),
      );
      expect(talk.raw?.connection).toBeInstanceOf(QuicBindingConnection);
      await within(exerciseTalk(talk, 1024 * 1024 + 3));
      await talk.close();
    });

    test('a given certificate, an advertised host, the listener policy', async () => {
      const certificate = selfSignedCertificate('node-1');
      const server = node();
      server.registerService(svc, tagged('svc'));
      const endpoint = await server.listenQuic('127.0.0.1', 0, {
        certificate,
        advertisedHost: 'localhost',
        policy: ChannelPolicies.allowTypes([svc]),
      });
      expect(endpoint).toMatch(/^quic:\/\/localhost:\d+$/);
      const client = node({ quic: nativeQuic() });
      const at = endpoint.replace('localhost', '127.0.0.1');
      expect(await tagOf(await client.openChannelAt(at, new ChannelAddress({ type: svc })))).toBe(
        'svc',
      );
      expect(
        await tagOf(await client.openChannelAt(at, new ChannelAddress({ type: Name.of('other') }))),
      ).toBe('closed permissionDenied');
    });

    test('the pool keeps one QUIC connection per endpoint; connections reports both', async () => {
      const server = node();
      const seen: MuxConnection[] = [];
      void (async () => {
        for await (const connection of server.connections) seen.push(connection);
      })();
      const endpoint = await server.listenQuic('127.0.0.1', 0);
      const client = node({ quic: nativeQuic() });
      const connection = await within(client.connect(endpoint));
      expect(await client.connect(endpoint)).toBe(connection);
      await connection.ping();
      expect(seen).toHaveLength(1);
      expect(seen[0]).toBeInstanceOf(QuicBindingConnection);
    });

    test('closing the server: GOAWAY, the listener stopped', async () => {
      const server = node();
      server.registerService(svc, () => {});
      const endpoint = await server.listenQuic('127.0.0.1', 0);
      const client = node({ quic: nativeQuic(), connectTimeout: 2000 });
      const connection = await within(client.connect(endpoint));
      await server.close();
      expect((await within(connection.done)).known).toBe(StatusCode.goingAway);
      expect(server.listeningEndpoints).toEqual([]);
      expect(server.isOwnEndpoint(endpoint)).toBe(false);
      await rejectsWith(client.connect(endpoint), StatusCode.unavailable);
      await rejectsWith(server.listenQuic('127.0.0.1', 0), StatusCode.failedPrecondition);
    });

    test('a node connecting to its own QUIC listener does so in-process', async () => {
      const n = node({ quic: nativeQuic() });
      const remotes: (string | undefined)[] = [];
      n.registerService(svc, (incoming) => {
        remotes.push(incoming.remote);
        tagged('svc')(incoming);
      });
      const endpoint = await n.listenQuic('127.0.0.1', 0);
      expect(await tagOf(await n.openChannelAt(endpoint, new ChannelAddress({ type: svc })))).toBe(
        'svc',
      );
      expect(remotes).toEqual(['local']);
    });
  },
);
