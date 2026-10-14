/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { Clock, TimerHandle } from '../../../src/core/util/clock.ts';

/**
 * Lets everything that is not waiting for a timer run: promise jobs, Web
 * Streams deliveries, and the microtasks they queue. One macrotask turn
 * drains the microtask queue; a few turns let chains that hop through
 * `setImmediate` settle too.
 */
export async function flush(turns = 3): Promise<void> {
  for (let i = 0; i < turns; i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}

interface Pending {
  readonly at: number;
  readonly seq: number;
  readonly callback: () => void;
}

/**
 * A clock that moves only when told to, flushing what is in flight
 * between timers: the port of `fake_async`'s `elapse`, where microtasks
 * run before and after each timer. `ManualClock` fires its timers
 * synchronously, which Talk's promise-driven code does not keep up with.
 */
export class FakeClock implements Clock {
  #now: number;
  #seq = 0;
  readonly #pending = new Set<Pending>();

  constructor(start = 0) {
    this.#now = start;
  }

  now(): number {
    return this.#now;
  }

  setTimeout(callback: () => void, ms: number): TimerHandle {
    const pending: Pending = {
      at: this.#now + Math.max(0, ms),
      seq: this.#seq++,
      callback,
    };
    this.#pending.add(pending);
    return pending;
  }

  clearTimeout(handle: TimerHandle): void {
    this.#pending.delete(handle as Pending);
  }

  /** Timers armed and not yet fired. */
  get pendingCount(): number {
    return this.#pending.size;
  }

  /** The time of the next timer, if any. */
  get nextAt(): number | undefined {
    return this.#next()?.at;
  }

  /**
   * Moves time forward by [ms], firing every timer due on the way, in
   * order, with what they set in motion flushed before the next one.
   */
  async elapse(ms: number): Promise<void> {
    const target = this.#now + ms;
    await flush();
    for (;;) {
      const next = this.#next();
      if (next === undefined || next.at > target) {
        break;
      }
      this.#now = next.at;
      this.#pending.delete(next);
      next.callback();
      await flush();
    }
    this.#now = target;
    await flush();
  }

  /** Fires every timer, moving time to each, until none is left (at most [limit] of them). */
  async flushTimers(limit = 10_000): Promise<void> {
    await flush();
    for (let i = 0; i < limit; i++) {
      const next = this.#next();
      if (next === undefined) {
        return;
      }
      this.#now = next.at;
      this.#pending.delete(next);
      next.callback();
      await flush();
    }
    throw new Error(`more than ${limit} timers fired`);
  }

  #next(): Pending | undefined {
    let best: Pending | undefined;
    for (const p of this.#pending) {
      if (best === undefined || p.at < best.at || (p.at === best.at && p.seq < best.seq)) {
        best = p;
      }
    }
    return best;
  }
}
