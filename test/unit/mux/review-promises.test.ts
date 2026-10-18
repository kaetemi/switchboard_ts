/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding 1 (docs/notes/review-mux.md): `FrameMuxConnection.ping()`
// handed out a plain `Promise.reject(...)` when the connection was closed
// and when the signal it was given had aborted already, so a caller that
// dropped the promise (a heartbeat `void connection.ping()`) got an
// unhandled rejection, which ends a Node process run with the default
// `--unhandled-rejections=throw`. Design section 7 and the Channels and Talk
// wiki page promise that every promise the library hands out is pre-caught;
// the QUIC binding's `ping` is (it rejects through a Deferred), and now the
// frame mux's is too. The same holds for the other promises the frame mux
// hands out on a refusal: a second concurrent read of `incoming` (which
// was a plain `Promise.reject` too), `identify`, `send`, `whenWritable`,
// `goAway` and `close`.

import { describe, expect, test } from 'vitest';
import { hexBytes } from '../../../src/core/bytes.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { stubCredential } from '../../support/identity/stub-verifier.ts';
import { empty, Incoming, muxPair, quiet, rejection, settle } from './support.ts';

/**
 * The reasons of the unhandled rejections reported while [run] and a few
 * event loop turns after it ran. Vitest's own listeners are set aside
 * meanwhile, so that what this observes is not also reported as an error
 * of the run; a promise reported here is left unhandled for good, which
 * reports nothing further.
 */
async function unhandledDuring(run: () => void): Promise<unknown[]> {
  const reasons: unknown[] = [];
  const saved = process.listeners('unhandledRejection');
  process.removeAllListeners('unhandledRejection');
  const record = (reason: unknown): void => {
    reasons.push(reason);
  };
  process.on('unhandledRejection', record);
  try {
    run();
    await settle();
    await settle();
  } finally {
    process.off('unhandledRejection', record);
    for (const listener of saved) {
      process.on('unhandledRejection', listener);
    }
  }
  return reasons;
}

describe('review: the frame mux hands out pre-caught promises', () => {
  test('a ping dropped on a closed connection is not an unhandled rejection', async () => {
    const [a, b] = muxPair();
    await a.close();
    await b.done;
    const reasons = await unhandledDuring(() => {
      void a.ping();
    });
    expect(reasons).toEqual([]);
  });

  test('a ping dropped with an aborted signal is not an unhandled rejection', async () => {
    const [a, b] = muxPair();
    const controller = new AbortController();
    controller.abort(new Error('stop'));
    const reasons = await unhandledDuring(() => {
      void a.ping(undefined, { signal: controller.signal });
    });
    expect(reasons).toEqual([]);
    await a.close();
    await b.done;
  });

  test('a ping refused at once still rejects for a caller that awaits it', async () => {
    const [a, b] = muxPair();
    const controller = new AbortController();
    const reason = new Error('stop');
    controller.abort(reason);
    expect(await rejection(a.ping(undefined, { signal: controller.signal }))).toBe(reason);
    await a.close();
    await b.done;
    const closed = await rejection(a.ping());
    expect(closed).toBeInstanceOf(SwitchboardError);
    expect((closed as SwitchboardError).code).toBe(StatusCode.failedPrecondition);
  });

  test('a second concurrent read of incoming, dropped, is not an unhandled rejection', async () => {
    const [a, b] = muxPair();
    const iterator = b.incoming[Symbol.asyncIterator]();
    const first = iterator.next();
    const reasons = await unhandledDuring(() => {
      void iterator.next();
    });
    expect(reasons).toEqual([]);
    // One awaited still rejects.
    expect(await rejection(iterator.next())).toBeInstanceOf(Error);
    a.open(empty);
    expect((await first).done).toBe(false);
    await a.close();
    await b.done;
  });

  test('the other promises the frame mux refuses with are pre-caught', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, initialWindow: 64 } });
    const incoming = new Incoming(b);
    const channel = a.open(empty);
    await incoming.next();
    // Two subframes take the window (16 + 16 each); the third waits for credit.
    void channel.send(new Uint8Array(16));
    void channel.send(new Uint8Array(16));
    const waiting = channel.send(new Uint8Array(16));
    const writable = channel.whenWritable;
    const credential = stubCredential({ identity: 'n', bytes: hexBytes('01') });
    const reasons = await unhandledDuring(() => {
      void waiting;
      void writable;
      void a.close();
    });
    expect(reasons).toEqual([]);
    await b.done;
    const later = await unhandledDuring(() => {
      void channel.whenWritable;
      void channel.close();
      void a.identify(credential);
      void a.goAway();
      void a.close();
    });
    expect(later).toEqual([]);
    // Awaited, the refusals still reject.
    expect(await rejection(waiting)).toBeInstanceOf(SwitchboardError);
    expect(await rejection(writable)).toBeInstanceOf(SwitchboardError);
    expect(await rejection(channel.whenWritable)).toBeInstanceOf(SwitchboardError);
    expect(await rejection(a.identify(credential))).toBeInstanceOf(SwitchboardError);
  });
});
