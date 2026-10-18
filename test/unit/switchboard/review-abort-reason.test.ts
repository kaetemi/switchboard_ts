/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), fixed: the resolving opens
// reject with `signal.reason` when their signal aborts
// (`OpenChannelOptions.signal`), whatever the reason is. `#connectFirst`
// (and `#connectRelay`) took every `SwitchboardError` from `connect` for an
// endpoint that failed and went on to the next endpoint and the next
// instance, so an abort whose reason is a `SwitchboardError` (CANCELLED,
// say, which the library itself aborts with) was reported as UNAVAILABLE
// "no reachable instance", after a futile `connect` per endpoint. They now
// rethrow the reason once the signal has aborted.

import { expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { RelayConfig } from '../../../src/core/switchboard/relay-config.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import { currentRuntime, node, pump, svc } from '../../support/node/harness.ts';

test('an open aborted with a SwitchboardError reason rejects with that reason', async () => {
  const dials: string[] = [];
  const runtime = {
    ...currentRuntime(),
    dialers: {
      // Dials that never answer, so that the abort lands while connecting.
      tcp: (endpoint: string) => {
        dials.push(endpoint);
        return new Promise<FrameTransport>(() => {});
      },
    },
  };
  const resolver = new StaticResolver([
    serviceRecord(new ServiceAddress(svc, 1), { endpoints: ['tcp://a:1', 'tcp://b:1'] }),
    serviceRecord(new ServiceAddress(svc, 2), { endpoints: ['tcp://c:1'] }),
  ]);
  const n = node({ resolver, runtime, connectTimeout: 200 });
  const controller = new AbortController();
  const reason = SwitchboardError.of(StatusCode.cancelled, 'the user gave up');
  const opening = n.openChannel(svc, { signal: controller.signal });
  await pump(5);
  expect(dials).toEqual(['tcp://a:1']);
  controller.abort(reason);
  let error: unknown;
  try {
    await opening;
  } catch (e) {
    error = e;
  }
  // The reason itself, not UNAVAILABLE "no reachable instance of svc
  // (tcp://a:1: cancelled: ...; tcp://b:1: cancelled: ...; ...)", and no
  // further endpoint was tried.
  expect(error).toBe(reason);
  expect(dials).toEqual(['tcp://a:1']);
});

test('an open through the relay aborted with a SwitchboardError reason rejects with that reason', async () => {
  const dials: string[] = [];
  const runtime = {
    ...currentRuntime(),
    dialers: {
      tcp: (endpoint: string) => {
        dials.push(endpoint);
        return new Promise<FrameTransport>(() => {});
      },
    },
  };
  // An instance without endpoints, reached through the relays.
  const resolver = new StaticResolver([serviceRecord(new ServiceAddress(svc, 1))]);
  const relay = new RelayConfig({ endpoints: ['tcp://r:1', 'tcp://s:1'] });
  const n = node({ resolver, runtime, relay, connectTimeout: 200 });
  const controller = new AbortController();
  const reason = SwitchboardError.of(StatusCode.cancelled, 'the user gave up');
  const opening = n.openChannel(svc, { signal: controller.signal });
  await pump(5);
  expect(dials).toEqual(['tcp://r:1']);
  controller.abort(reason);
  let error: unknown;
  try {
    await opening;
  } catch (e) {
    error = e;
  }
  expect(error).toBe(reason);
});
