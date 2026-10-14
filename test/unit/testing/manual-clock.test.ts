/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// ManualClock's promise-aware stepping (`elapseAsync`, `flushAsync`,
// `settle`): what a timer sets off through promises runs before the next
// timer fires, as in `FakeAsync.elapse`. The synchronous `advance` and
// `flush` are tested in test/unit/util.test.ts.

import { describe, expect, test } from 'vitest';
import { ManualClock } from '../../../src/testing/manual-clock.ts';

/** Resolves after [hops] promise jobs. */
async function hops(n: number): Promise<void> {
  for (let i = 0; i < n; i++) {
    await Promise.resolve();
  }
}

describe('ManualClock, promise-aware stepping', () => {
  test('a timer armed by a promise continuation of a timer fires within the same elapseAsync', async () => {
    const clock = new ManualClock();
    const fired: number[] = [];
    clock.setTimeout(() => {
      fired.push(clock.now());
      void hops(5).then(() => {
        clock.setTimeout(() => fired.push(clock.now()), 10);
      });
    }, 10);
    await clock.elapseAsync(25);
    expect(fired).toEqual([10, 20]);
    expect(clock.now()).toBe(25);
    expect(clock.pendingCount).toBe(0);
  });

  test('advance does not wait for the promise jobs a timer starts', async () => {
    const clock = new ManualClock();
    const fired: number[] = [];
    clock.setTimeout(() => {
      void hops(1).then(() => {
        clock.setTimeout(() => fired.push(clock.now()), 10);
      });
    }, 10);
    clock.advance(25);
    await hops(2);
    expect(fired).toEqual([]);
    // Armed after the advance, at 25: due at 35.
    expect(clock.nextAt).toBe(35);
  });

  test('promise jobs pending before the call run before the first timer', async () => {
    const clock = new ManualClock();
    const order: string[] = [];
    void hops(3).then(() => {
      order.push('job');
      clock.setTimeout(() => order.push('early timer'), 1);
    });
    clock.setTimeout(() => order.push('timer'), 5);
    await clock.elapseAsync(5);
    expect(order).toEqual(['job', 'early timer', 'timer']);
  });

  test('what the last timer starts has run when elapseAsync resolves', async () => {
    const clock = new ManualClock();
    let settled = false;
    clock.setTimeout(() => {
      void hops(10).then(() => {
        settled = true;
      });
    }, 3);
    await clock.elapseAsync(3);
    expect(settled).toBe(true);
  });

  test('timers due later than the target stay pending', async () => {
    const clock = new ManualClock(100);
    const fired: number[] = [];
    clock.setTimeout(() => fired.push(clock.now()), 5);
    clock.setTimeout(() => fired.push(clock.now()), 50);
    await clock.elapseAsync(10);
    expect(fired).toEqual([105]);
    expect(clock.pendingCount).toBe(1);
    expect(clock.nextAt).toBe(150);
  });

  test('flushAsync fires every timer, those armed through promises included', async () => {
    const clock = new ManualClock();
    const fired: number[] = [];
    const chain = (left: number): void => {
      clock.setTimeout(() => {
        fired.push(clock.now());
        if (left > 0) {
          void hops(2).then(() => chain(left - 1));
        }
      }, 7);
    };
    chain(3);
    await clock.flushAsync();
    expect(fired).toEqual([7, 14, 21, 28]);
    expect(clock.pendingCount).toBe(0);
    expect(clock.nextAt).toBeUndefined();
  });

  test('flushAsync throws on timers that re-arm without end', async () => {
    const clock = new ManualClock();
    const tick = (): void => {
      clock.setTimeout(tick, 1);
    };
    tick();
    await expect(clock.flushAsync(50)).rejects.toThrow(/more than 50 timers/);
  });

  test('settle lets a promise chain through a macrotask complete without moving the clock', async () => {
    const clock = new ManualClock(7);
    let done = false;
    void (async () => {
      await new Promise<void>((resolve) => setImmediate(resolve));
      await hops(3);
      done = true;
    })();
    await ManualClock.settle();
    expect(done).toBe(true);
    expect(clock.now()).toBe(7);
  });
});
