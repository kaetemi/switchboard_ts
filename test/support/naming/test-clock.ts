/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The naming tests' port of `fake_async`: a clock that moves only when told
// to and lets the microtasks run before and after each timer it fires, as
// `FakeAsync.elapse` does. The fake Talk pair delivers its frames in
// microtasks, so everything a timer sets off settles before the next one.

import type { Clock, TimerHandle } from '../../../src/core/util/clock.ts';

interface Pending {
  readonly at: number;
  readonly seq: number;
  readonly callback: () => void;
}

/**
 * Resolves once every microtask queued so far, and every one those queue,
 * has run: `FakeAsync.flushMicrotasks`, and the Dart tests' `pump()`.
 */
export function flushMicrotasks(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

/** A clock that moves only with {@link elapse} (`FakeAsync.elapse`). */
export class TestClock implements Clock {
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

  /**
   * Moves time forward by [ms], firing every timer due on the way in order
   * and letting the microtasks run before the first and after each one.
   */
  async elapse(ms: number): Promise<void> {
    const target = this.#now + ms;
    await flushMicrotasks();
    for (;;) {
      const next = this.#next();
      if (next === undefined || next.at > target) break;
      this.#now = next.at;
      this.#pending.delete(next);
      next.callback();
      await flushMicrotasks();
    }
    this.#now = target;
  }

  #next(): Pending | undefined {
    let best: Pending | undefined;
    for (const p of this.#pending) {
      if (best === undefined || p.at < best.at || (p.at === best.at && p.seq < best.seq)) best = p;
    }
    return best;
  }
}
