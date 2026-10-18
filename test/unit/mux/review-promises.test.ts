/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// REVIEW FINDING (docs/notes/review-mux.md, finding 1). `FrameMuxConnection.ping()`
// hands out a plain `Promise.reject(...)` when the connection is closed and
// when the signal it is given is aborted already, so a caller that drops
// the promise (a heartbeat `void connection.ping()`) gets an unhandled
// rejection, which ends a Node process run with the default
// `--unhandled-rejections=throw`. Design section 7 and the Channels and Talk
// wiki page promise that every promise the library hands out is pre-caught;
// the QUIC binding's `ping` is (it rejects through a Deferred).
//
// The tests are `test.fails`: their bodies fail against the current sources.
// Once the defect is fixed they pass, `test.fails` reports that, and the
// `.fails` goes.

import { describe, expect, test } from 'vitest';
import { muxPair, settle } from './support.ts';

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

describe('review: ping hands out pre-caught promises', () => {
  test.fails('a ping dropped on a closed connection is not an unhandled rejection', async () => {
    const [a, b] = muxPair();
    await a.close();
    await b.done;
    const reasons = await unhandledDuring(() => {
      void a.ping();
    });
    expect(reasons).toEqual([]);
  });

  test.fails('a ping dropped with an aborted signal is not an unhandled rejection', async () => {
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
});
