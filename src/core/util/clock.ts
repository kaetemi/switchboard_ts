/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Time, injectable. Every timer the library arms (keep-alive, the LIMITS
 * wait, close confirmation, the GOAWAY grace, identity, every Talk
 * timeout, bulk idle) comes from the {@link Clock} in its options, never
 * from the globals directly, so tests drive time by hand and so the Dart
 * `fake_async` tests port one to one.
 *
 * `now()` is monotonic milliseconds from an arbitrary origin, never wall
 * time. Timers longer than 2^31-1 ms are the clock's problem: the system
 * clock re-arms in chunks, as Dart Talk does.
 */

/** A handle to a pending timer. Opaque; pass it back to `clearTimeout`. */
export type TimerHandle = object;

/** The clock and timer source of a connection or channel. */
export interface Clock {
  /** Monotonic milliseconds. Fractions are fine. */
  now(): number;
  /** Runs `callback` once, `ms` milliseconds from now (0 = next macrotask). */
  setTimeout(callback: () => void, ms: number): TimerHandle;
  /** Cancels a timer; a handle that already fired or was cleared is ignored. */
  clearTimeout(handle: TimerHandle): void;
}

const maxTimeout = 2 ** 31 - 1;

interface SystemHandle {
  inner: ReturnType<typeof globalThis.setTimeout> | undefined;
  cancelled: boolean;
}

/**
 * The default clock: `performance.now()` and the global timers. Timers that
 * would keep a Node process alive are left referenced; a connection that
 * should not hold the process open is closed, not unref'd (Dart has no
 * such notion and the behaviour stays the same across runtimes).
 */
export const systemClock: Clock = Object.freeze({
  now(): number {
    return performance.now();
  },
  setTimeout(callback: () => void, ms: number): TimerHandle {
    const handle: SystemHandle = { inner: undefined, cancelled: false };
    const arm = (remaining: number): void => {
      const slice = Math.min(remaining, maxTimeout);
      handle.inner = globalThis.setTimeout(() => {
        if (handle.cancelled) return;
        if (remaining > slice) arm(remaining - slice);
        else callback();
      }, slice);
    };
    arm(Math.max(0, ms));
    return handle;
  },
  clearTimeout(handle: TimerHandle): void {
    const system = handle as SystemHandle;
    system.cancelled = true;
    if (system.inner !== undefined) globalThis.clearTimeout(system.inner);
  },
});
