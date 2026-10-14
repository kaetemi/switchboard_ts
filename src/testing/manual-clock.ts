/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { Clock, TimerHandle } from '../core/util/clock.ts';

interface Pending {
  readonly at: number;
  readonly seq: number;
  readonly callback: () => void;
}

/**
 * A clock that moves only when told to, the port of `fake_async`. Timers
 * fire in due order while `advance()` or `flush()` runs; a callback that
 * arms another timer inside the advanced span fires in the same call.
 */
export class ManualClock implements Clock {
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
    const pending: Pending = { at: this.#now + Math.max(0, ms), seq: this.#seq++, callback };
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

  /** Moves time forward by `ms`, firing every timer due on the way, in order. */
  advance(ms: number): void {
    const target = this.#now + ms;
    for (;;) {
      const next = this.#next();
      if (next === undefined || next.at > target) break;
      this.#now = next.at;
      this.#pending.delete(next);
      next.callback();
    }
    this.#now = target;
  }

  /** Fires every pending timer, moving time to each, until none is left. */
  flush(): void {
    for (;;) {
      const next = this.#next();
      if (next === undefined) break;
      this.#now = next.at;
      this.#pending.delete(next);
      next.callback();
    }
  }

  #next(): Pending | undefined {
    let best: Pending | undefined;
    for (const p of this.#pending) {
      if (best === undefined || p.at < best.at || (p.at === best.at && p.seq < best.seq)) best = p;
    }
    return best;
  }
}
