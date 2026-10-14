/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { TimerHandle } from '../../../src/core/util/clock.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';

/**
 * A {@link ManualClock} that tells the delays its pending timers were
 * armed with, as `fake_async`'s `pendingTimers` does.
 */
export class TimerClock extends ManualClock {
  readonly #delays = new Map<TimerHandle, number>();

  override setTimeout(callback: () => void, ms: number): TimerHandle {
    const handle: TimerHandle = super.setTimeout(() => {
      this.#delays.delete(handle);
      callback();
    }, ms);
    this.#delays.set(handle, ms);
    return handle;
  }

  override clearTimeout(handle: TimerHandle): void {
    this.#delays.delete(handle);
    super.clearTimeout(handle);
  }

  /** The delays the pending timers were armed with. */
  get pendingDelays(): number[] {
    return [...this.#delays.values()];
  }
}
