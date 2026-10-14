/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/mesh_test.dart`: a small mesh without
// a naming service. Node A runs `alpha/1` and `beta/2` and listens twice,
// node B runs `gamma/3` and listens once; every node, and a client node C,
// resolves through the same static table, and calls cross between the
// nodes. As in Dart over TCP and WebSocket (A on TCP and WebSocket, B on
// WebSocket), and again over in-process `mem://` endpoints.

import { beforeEach, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../src/core/address/index.ts';
import { Name } from '../../src/core/name.ts';
import { serviceRecord } from '../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode, SwitchboardError } from '../../src/core/status.ts';
import type { ChannelHandler } from '../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../src/core/switchboard/resolver.ts';
import type { TalkMessage } from '../../src/core/talk/contract.ts';
import { rejectsWith } from '../support/node/harness.ts';
import { listen, rejectsWithOneOf, type Scheme, until } from './mesh-support.ts';
import { node, onCleanup, type Switchboard, within } from './node-support.ts';

const alpha = Name.of('alpha');
const beta = Name.of('beta');
const gamma = Name.of('gamma');

/** Serves Talk requests on every channel of a service with [handle], which returns the reply payload. */
function serve(handle: (m: TalkMessage) => Promise<string>): ChannelHandler {
  return (incoming) => {
    void incoming.talk().serve((message) => {
      handle(message).then(
        (reply) => message.reply(reply),
        (e: unknown) =>
          message.replyAbort(
            e instanceof SwitchboardError ? e.status : Status.of(StatusCode.internal, `${e}`),
          ),
      );
    });
  };
}

/** One request on a fresh channel from [from] to [to]. */
async function call(
  from: Switchboard,
  to: ServiceAddress,
  procedure: string,
  payload = '',
): Promise<string> {
  const talk = await from.openTalk(to);
  try {
    return (await talk.request(procedure, payload)).text();
  } finally {
    await talk.close();
  }
}

const layouts: readonly { name: string; a: readonly [Scheme, Scheme]; b: Scheme }[] = [
  { name: 'over TCP and WebSocket', a: ['tcp', 'ws'], b: 'ws' },
  { name: 'over mem', a: ['mem', 'mem'], b: 'mem' },
];

for (const layout of layouts) {
  describe(layout.name, () => {
    let table: StaticResolver;
    let a: Switchboard;
    let b: Switchboard;
    let c: Switchboard;
    let aFirst: string;

    beforeEach(async () => {
      table = new StaticResolver();
      onCleanup(() => table.close());
      a = node({ resolver: table });
      b = node({ resolver: table });
      c = node({ resolver: table });
      aFirst = await listen(a, layout.a[0]);
      const aSecond = await listen(a, layout.a[1]);
      const bOnly = await listen(b, layout.b);
      table.add(serviceRecord(new ServiceAddress(alpha, 1), { endpoints: [aFirst] }));
      table.add(serviceRecord(new ServiceAddress(beta, 2), { endpoints: [aSecond] }));
      table.add(serviceRecord(new ServiceAddress(gamma, 3), { endpoints: [bOnly] }));

      a.registerService(
        alpha,
        serve(async (m) => {
          switch (m.procedureName) {
            case 'UPPER':
              return m.text().toUpperCase();
            case 'CHAIN':
              return `alpha>${await call(a, new ServiceAddress(gamma), 'CHAIN')}`;
          }
          throw SwitchboardError.of(StatusCode.unimplemented);
        }),
        { instance: 1 },
      );
      a.registerService(
        beta,
        serve(async (m) => `beta(${m.text()})`),
        { instance: 2 },
      );
      b.registerService(
        gamma,
        (incoming) => {
          void incoming.talk().serve((m) => {
            const fail = (e: unknown) => m.replyAbort(Status.of(StatusCode.internal, `${e}`));
            switch (m.procedureName) {
              case 'CHAIN':
                call(b, new ServiceAddress(beta), 'NAME', 'from gamma').then(
                  (r) => m.reply(`gamma>${r}`),
                  fail,
                );
                break;
              case 'ALPHA':
                call(b, new ServiceAddress(alpha), 'UPPER', m.text()).then((r) => m.reply(r), fail);
                break;
              case 'TICK': {
                // Items until cancelled or the channel goes away.
                let n = 0;
                const timer = setInterval(() => {
                  if (!m.canReply) {
                    clearInterval(timer);
                    return;
                  }
                  void m.replyItem(`${n++}`).catch(() => {});
                }, 5);
                void m.onCancel.then(() => clearInterval(timer));
                void incoming.channel.done.then(() => clearInterval(timer));
                break;
              }
              default:
                void m.replyAbort(Status.of(StatusCode.unimplemented));
            }
          });
        },
        { instance: 3 },
      );
    });

    test('cross calls over both listeners', async () => {
      expect(await call(c, new ServiceAddress(alpha), 'UPPER', 'hi')).toBe('HI');
      expect(await call(c, new ServiceAddress(beta, 2), 'X', 'c')).toBe('beta(c)');
      // A reaches its own beta through its second listener's endpoint:
      // in-process, no socket to itself.
      expect(await call(a, new ServiceAddress(beta), 'X', 'self')).toBe('beta(self)');
      // B reaches alpha on A, from inside a request from C.
      expect(await call(c, new ServiceAddress(gamma), 'ALPHA', 'up')).toBe('UP');
      // Each node pooled one connection per endpoint it used.
      const again = await c.openTalk(new ServiceAddress(alpha));
      expect(again.raw).toBeDefined();
      expect(again.raw?.connection).toBe(await c.connect(aFirst));
      await again.close();
    });

    test('a request chained across nodes', async () => {
      expect(await call(c, new ServiceAddress(alpha), 'CHAIN')).toBe(
        'alpha>gamma>beta(from gamma)',
      );
      // Concurrent chains share the pooled connections.
      const results = await Promise.all(
        Array.from({ length: 20 }, () => call(c, new ServiceAddress(alpha), 'CHAIN')),
      );
      expect(results.every((r) => r === 'alpha>gamma>beta(from gamma)')).toBe(true);
    });

    test('graceful shutdown of a node during a stream', async () => {
      const talk = await c.openTalk(new ServiceAddress(gamma));
      const stream = talk.streamRequest('TICK');
      const items: string[] = [];
      const ended = (async (): Promise<unknown> => {
        try {
          for await (const m of stream) {
            items.push(m.text());
          }
          return 'done without error';
        } catch (e) {
          return e;
        }
      })();
      await until(() => items.length >= 3, 'three items');
      const start = performance.now();
      await within(b.close(), 5000);
      const error = await within(ended, 5000);
      expect(performance.now() - start).toBeLessThan(2000);
      await rejectsWithOneOf(Promise.reject(error), [StatusCode.goingAway, StatusCode.unavailable]);
      expect(items.slice(0, 3)).toEqual(['0', '1', '2']);
      expect(await within(talk.done, 5000)).toBeInstanceOf(Status);
      // gamma is gone; the rest of the mesh still works.
      await rejectsWith(c.openTalk(new ServiceAddress(gamma)), StatusCode.unavailable);
      expect(await call(c, new ServiceAddress(alpha), 'UPPER', 'still')).toBe('STILL');
    });
  });
}
