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

/** How many event loop turns {@link ManualClock.settle} lets run by default. */
const defaultTurns = 3;

/**
 * One turn of the event loop: resolves from a macrotask, after every
 * microtask queued before it (and every one those queue) has run.
 * `setImmediate` where the runtime has it (Node, Bun, Deno), else a
 * `MessageChannel` message (browsers), else a zero-delay timer.
 */
function macrotask(): Promise<void> {
  const immediate = (globalThis as { setImmediate?: (callback: () => void) => unknown })
    .setImmediate;
  if (typeof immediate === 'function') {
    return new Promise((resolve) => {
      immediate(resolve);
    });
  }
  if (typeof MessageChannel === 'function') {
    return new Promise((resolve) => {
      const { port1, port2 } = new MessageChannel();
      port1.onmessage = () => {
        port1.close();
        resolve();
      };
      port2.postMessage(undefined);
    });
  }
  return new Promise((resolve) => {
    globalThis.setTimeout(resolve, 0);
  });
}

/**
 * A clock that moves only when told to, the port of `fake_async`. Timers
 * fire in due order while `advance()` or `flush()` runs; a callback that
 * arms another timer inside the advanced span fires in the same call.
 *
 * `advance()` and `flush()` fire timers synchronously, one after the
 * other: what a timer sets off through promises runs only after the call.
 * Code driven by promises (a Talk channel, a mux reading its transport, a
 * Web Streams pipe) uses {@link elapseAsync} and {@link flushAsync}
 * instead, which let promise jobs run before the first timer, after each
 * and at the end, as `FakeAsync.elapse` runs microtasks around each timer:
 * a timer armed by a promise continuation then fires at its time within
 * the same call.
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

  /** When the next pending timer is due, or undefined when none is pending. */
  get nextAt(): number | undefined {
    return this.#next()?.at;
  }

  /**
   * Lets everything that does not wait for a timer run: promise jobs, Web
   * Streams deliveries and the microtasks they queue, over [turns] turns
   * of the event loop (default 3), so that chains which hop through a
   * macrotask settle too. Real time passes; no manual clock moves. The
   * Dart tests' `flushMicrotasks()` and `pumpEventQueue()`.
   */
  static async settle(turns = defaultTurns): Promise<void> {
    for (let i = 0; i < turns; i++) {
      await macrotask();
    }
  }

  /**
   * Moves time forward by [ms] as {@link advance} does, letting promise
   * jobs run ({@link settle} with [turns]) before the first timer, after
   * each timer it fires, and once time reached the target: what a timer
   * sets in motion runs before the next one fires, and a timer armed on
   * the way fires within the call if it is due by then.
   */
  async elapseAsync(ms: number, turns = defaultTurns): Promise<void> {
    const target = this.#now + ms;
    await ManualClock.settle(turns);
    for (;;) {
      const next = this.#next();
      if (next === undefined || next.at > target) break;
      this.#now = next.at;
      this.#pending.delete(next);
      next.callback();
      await ManualClock.settle(turns);
    }
    this.#now = target;
    await ManualClock.settle(turns);
  }

  /**
   * Fires every pending timer as {@link flush} does, moving time to each,
   * with promise jobs let run before the first and after each
   * ({@link settle} with [turns]), until none is left.
   *
   * @throws {Error} after [limit] timers (default 10 000), for timers that
   *   keep re-arming themselves.
   */
  async flushAsync(limit = 10_000, turns = defaultTurns): Promise<void> {
    await ManualClock.settle(turns);
    for (let i = 0; i < limit; i++) {
      const next = this.#next();
      if (next === undefined) return;
      this.#now = next.at;
      this.#pending.delete(next);
      next.callback();
      await ManualClock.settle(turns);
    }
    throw new Error(`more than ${limit} timers fired`);
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
