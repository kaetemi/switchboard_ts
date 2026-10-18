// Review findings (connectQuic, docs/notes/review-quic.md), both in quic-dial.ts:
//
// - The dial's AbortSignal was watched only while the provider connected. Once the session was
//   there, `QuicBindingConnection.start` ran unwatched (it asks the session for its exporter,
//   bounded by the control stream timeout, 10 s by default, and on the initiator waits for the
//   control stream to open), and the signal was looked at again only after it: an abort meanwhile
//   rejected the dial only then. Design section 5 puts an AbortSignal on every operation that
//   waits, connect first. `start` is raced against the signal now, as the connect is.
// - The binding's own options were checked by `QuicBindingConnection.start` only, after the
//   provider connected: a `recordLimit` out of range rejected the dial with its RangeError and
//   left the session open, until the acceptor gave up waiting for a HELLO (the session holds a
//   native provider's event loop meanwhile). Dart validates in the same place. `listenQuic` took
//   the same options without checking them, and then refused every session it accepted
//   (INTERNAL, logged as a warning). Both check them first now (`checkQuicBindingOptions`), and
//   the dial closes the session should `start` throw all the same.
//
// In-memory provider, virtual time.
import { describe, expect, test } from 'vitest';
import type { MuxOptions } from '../../../src/core/mux/contract.ts';
import type { QuicConnector } from '../../../src/core/quic/contract.ts';
import { connectQuic } from '../../../src/core/quic/quic-dial.ts';
import { listenQuic } from '../../../src/core/quic/quic-listen.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  MemoryQuicNetwork,
  type MemoryQuicSession,
} from '../../../src/testing/memory-quic-provider.ts';

const endpoint = 'quic://server.test:4433';

/** A listener on [endpoint] and a connector that hands each session it dials to [prepare] first. */
function network(
  clock: ManualClock,
  prepare: (session: MemoryQuicSession) => void = () => {},
): { connector: QuicConnector; sessions: MemoryQuicSession[]; close: () => Promise<void> } {
  const memory = new MemoryQuicNetwork({ clock });
  const options: Partial<MuxOptions> = { clock, keepAliveInterval: undefined };
  const listener = listenQuic(memory.listen(endpoint), { options });
  const inner = memory.connector();
  const sessions: MemoryQuicSession[] = [];
  const connector: QuicConnector = {
    async connect(target, config) {
      const session = (await inner.connect(target, config)) as MemoryQuicSession;
      sessions.push(session);
      prepare(session);
      return session;
    },
  };
  return { connector, sessions, close: () => listener.close() };
}

/** Records how [promise] settled. */
function track(promise: Promise<unknown>): {
  readonly outcome: unknown;
  readonly settled: boolean;
} {
  const state = { outcome: undefined as unknown, settled: false };
  promise.then(
    (value) => {
      state.outcome = value;
      state.settled = true;
    },
    (error: unknown) => {
      state.outcome = error;
      state.settled = true;
    },
  );
  return state;
}

describe('review: connectQuic', () => {
  test('an abort while the binding starts rejects the dial at once and closes the session', async () => {
    const clock = new ManualClock();
    const exporter = new Deferred<void>();
    // The session's exporter takes its time (a browser's may, and the WebTransport one is async).
    const net = network(clock, (session) => {
      session.exporterGate = exporter.promise;
    });
    const controller = new AbortController();
    const reason = new Error('the caller gave up');
    const dial = track(
      connectQuic(endpoint, net.connector, {
        signal: controller.signal,
        options: { clock, keepAliveInterval: undefined },
      }),
    );
    await clock.elapseAsync(1);
    expect(net.sessions).toHaveLength(1);
    controller.abort(reason);
    await clock.elapseAsync(1);
    // Today the dial stays pending until the exporter wait ends (10 s of virtual time here).
    expect(dial.settled).toBe(true);
    expect(dial.outcome).toBe(reason);
    expect(net.sessions[0]?.ended).toBe(true);
    exporter.resolve();
    await clock.elapseAsync(60_000);
    await net.close();
  });

  test('binding options out of range are refused without leaving a session open', async () => {
    const clock = new ManualClock();
    const net = network(clock);
    const dial = track(
      connectQuic(endpoint, net.connector, {
        quicOptions: { recordLimit: 100 },
        options: { clock, keepAliveInterval: undefined },
      }),
    );
    await clock.elapseAsync(1);
    expect(dial.settled).toBe(true);
    expect(dial.outcome).toBeInstanceOf(RangeError);
    // Either no session was dialled, or the one dialled was closed.
    expect(net.sessions.every((session) => session.ended)).toBe(true);
    await clock.elapseAsync(60_000);
    await net.close();
  });

  test('listenQuic refuses binding options out of range at once, as connectQuic should', () => {
    const memory = new MemoryQuicNetwork({ clock: new ManualClock() });
    const listener = memory.listen(endpoint);
    let thrown: unknown;
    try {
      const accepting = listenQuic(listener, { quicOptions: { recordLimit: 100 } });
      void accepting.close();
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(RangeError);
  });
});
