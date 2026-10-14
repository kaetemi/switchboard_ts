/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { TimerHandle } from '../../../src/core/util/clock.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';

/**
 * Lets every pending microtask run: the work of a promise chain, a memory
 * pair that hands frames over at once, a toy mux reading them.
 */
export async function settle(): Promise<void> {
  for (let i = 0; i < 2; i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}

/**
 * A {@link ManualClock} that moves like Dart's `fake_async` `elapse`: from
 * timer to timer, letting the microtasks each one starts run before the
 * next, so that a timer armed by a promise continuation fires at the right
 * time within the same {@link elapse}.
 */
export class SteppingClock extends ManualClock {
  readonly #due = new Map<TimerHandle, number>();

  override setTimeout(callback: () => void, ms: number): TimerHandle {
    const at = this.now() + Math.max(0, ms);
    const handle: TimerHandle = super.setTimeout(() => {
      this.#due.delete(handle);
      callback();
    }, ms);
    this.#due.set(handle, at);
    return handle;
  }

  override clearTimeout(handle: TimerHandle): void {
    this.#due.delete(handle);
    super.clearTimeout(handle);
  }

  /** Timers armed and not yet fired or cleared. */
  get pending(): number {
    return this.#due.size;
  }

  /** Moves time forward by [ms], firing timers in order and settling after each. */
  async elapse(ms: number): Promise<void> {
    const target = this.now() + ms;
    await settle();
    for (;;) {
      let next = Number.POSITIVE_INFINITY;
      for (const at of this.#due.values()) {
        next = Math.min(next, at);
      }
      if (next > target) {
        break;
      }
      this.advance(next - this.now());
      await settle();
    }
    this.advance(target - this.now());
    await settle();
  }
}
