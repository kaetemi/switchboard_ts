/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Node to Node over WebTransport (HTTP/3) on the loopback, through the
// native provider: listenWebTransport at a path, and a client node whose
// quic option dials https:// endpoints, pinning the listener's
// self-signed certificate. Raw channels; the Talk cases wait for the
// node's Talk layer.
//
// Without the addon (`npm run build:native`, or SWITCHBOARD_QUIC_ADDON)
// the file is skipped with the reason; with SWITCHBOARD_NATIVE=required a
// missing addon fails it.

import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/index.ts';
import { Name } from '../../src/core/name.ts';
import { QuicBindingConnection } from '../../src/core/quic/quic-binding-connection.ts';
import { StatusCode } from '../../src/core/status.ts';
import { EndpointResolver } from '../../src/core/switchboard/resolver.ts';
import {
  nativeQuic,
  nativeQuicAvailability,
  selfSignedCertificate,
} from '../../src/node/quic/index.ts';
import { rejectsWith, tagged, tagOf } from '../support/node/harness.ts';
import { echo, node, roundTrip, within } from './node-support.ts';

const availability = nativeQuicAvailability();
if (!availability.available && process.env['SWITCHBOARD_NATIVE'] === 'required') {
  throw new Error(`SWITCHBOARD_NATIVE=required, but ${availability.reason}`);
}

const svc = Name.of('svc');

describe.skipIf(!availability.available)(
  `two nodes over WebTransport${availability.available ? '' : ` (skipped: ${availability.reason})`}`,
  () => {
    test('a client opens a channel to a service of a server, pinning its certificate', async () => {
      const certificate = selfSignedCertificate('127.0.0.1');
      const server = node();
      server.registerService('echo', echo);
      const endpoint = await server.listenWebTransport('127.0.0.1', 0, {
        path: '/sb',
        certificate,
      });
      expect(endpoint).toMatch(/^https:\/\/127\.0\.0\.1:\d+\/sb$/);
      expect(server.listeningEndpoints).toEqual([endpoint]);
      expect(server.isOwnEndpoint(endpoint)).toBe(true);
      expect(server.isOwnEndpoint(endpoint.replace('/sb', '/other'))).toBe(false);
      const client = node({
        quic: nativeQuic(),
        quicBinding: { serverCertificateHashes: [certificate.sha256] },
        resolver: new EndpointResolver(endpoint),
      });
      const channel = await within(client.openChannel('echo'));
      expect(channel.connection).toBeInstanceOf(QuicBindingConnection);
      expect(await roundTrip(channel, 'over WebTransport')).toBe('over WebTransport');
      await channel.close();
      expect((await channel.done).isOk).toBe(true);
    });

    test.todo('Talk requests over WebTransport: the node Talk layer is not wired yet');

    test('without verification, as between nodes with self-signed certificates', async () => {
      const server = node();
      server.registerService(svc, tagged('svc'));
      const endpoint = await server.listenWebTransport('127.0.0.1', 0);
      expect(new URL(endpoint).pathname).toBe('/');
      const client = node({
        quic: nativeQuic(),
        quicBinding: { webTransportCertificateVerified: false },
      });
      expect(
        await tagOf(
          await within(client.openChannelAt(endpoint, new ChannelAddress({ type: svc }))),
        ),
      ).toBe('svc');
    });

    test('another path, or a certificate the client does not trust, is UNAVAILABLE', async () => {
      const server = node();
      const endpoint = await server.listenWebTransport('127.0.0.1', 0, { path: '/sb' });
      const lax = node({
        quic: nativeQuic(),
        quicBinding: { webTransportCertificateVerified: false },
        connectTimeout: 5000,
      });
      await rejectsWith(lax.connect(endpoint.replace('/sb', '/other')), StatusCode.unavailable);
      const strict = node({ quic: nativeQuic(), connectTimeout: 5000 });
      const error = await strict.connect(endpoint).then(
        () => undefined,
        (e: unknown) => e,
      );
      expect(error).toBeDefined();
    });

    test('closing the server: GOAWAY, the listener stopped', async () => {
      const server = node();
      server.registerService(svc, () => {});
      const endpoint = await server.listenWebTransport('127.0.0.1', 0, { path: '/sb' });
      const client = node({
        quic: nativeQuic(),
        quicBinding: { webTransportCertificateVerified: false },
        connectTimeout: 2000,
      });
      const connection = await within(client.connect(endpoint));
      await server.close();
      expect((await within(connection.done)).known).toBe(StatusCode.goingAway);
      expect(server.listeningEndpoints).toEqual([]);
      await rejectsWith(client.connect(endpoint), StatusCode.unavailable);
    });

    test('a path with a query is a TypeError', async () => {
      const server = node();
      await expect(
        server.listenWebTransport('127.0.0.1', 0, { path: '/sb?x=1' }),
      ).rejects.toBeInstanceOf(TypeError);
    });
  },
);
