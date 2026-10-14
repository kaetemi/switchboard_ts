/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import type { Status } from '../status.ts';
import type { FrameTransport } from '../transport/contract.ts';
import type { Clock } from '../util/clock.ts';

/**
 * Small promise helpers of the node and the clients: bounds on the
 * injectable clock, caller-side cancellation, and fire-and-forget shutdowns
 * that never surface as unhandled rejections. Internal.
 */

/**
 * [promise], rejected with what [onTimeout] gives if it has not settled
 * [ms] milliseconds from now on [clock]; undefined: no bound. A value that
 * arrives after the timeout goes to [onLate] (to close a connection nobody
 * will take, say).
 */
export function bounded<T>(
  promise: Promise<T>,
  ms: number | undefined,
  clock: Clock,
  onTimeout: () => unknown,
  onLate?: (value: T) => void,
): Promise<T> {
  if (ms === undefined) {
    return promise;
  }
  return new Promise<T>((resolve, reject) => {
    let timedOut = false;
    const timer = clock.setTimeout(
      () => {
        timedOut = true;
        reject(onTimeout());
      },
      Math.max(0, ms),
    );
    promise.then(
      (value) => {
        if (timedOut) {
          onLate?.(value);
        } else {
          clock.clearTimeout(timer);
          resolve(value);
        }
      },
      (error: unknown) => {
        if (!timedOut) {
          clock.clearTimeout(timer);
          reject(error);
        }
      },
    );
  });
}

/**
 * [promise] for one caller, rejected with `signal.reason` when [signal]
 * aborts first; the work behind [promise] goes on (other callers may share
 * it). A value that arrives after the abort goes to [onLate].
 */
export function abortable<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
  onLate?: (value: T) => void,
): Promise<T> {
  if (signal === undefined) {
    return promise;
  }
  if (signal.aborted) {
    promise.then(
      (value) => onLate?.(value),
      () => {},
    );
    return Promise.reject(signal.reason);
  }
  return new Promise<T>((resolve, reject) => {
    let aborted = false;
    const onAbort = (): void => {
      aborted = true;
      reject(signal.reason);
    };
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        if (aborted) {
          onLate?.(value);
        } else {
          resolve(value);
        }
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      },
    );
  });
}

/** Throws `signal.reason` when [signal] has aborted. */
export function checkAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) {
    throw signal.reason;
  }
}

/** Runs [operation] and swallows what it throws or rejects with. */
export function quietly(operation: () => Promise<unknown> | undefined): Promise<void> {
  try {
    const result = operation();
    return result === undefined
      ? Promise.resolve()
      : result.then(
          () => {},
          () => {},
        );
  } catch {
    return Promise.resolve();
  }
}

/** Sends GOAWAY on [connection] (with [status], if given); never throws or rejects. */
export function goAwayQuietly(connection: MuxConnection, status?: Status): Promise<void> {
  return quietly(() => connection.goAway(status));
}

/** Closes [channel] (a mux channel, or a channel carrying one) with [status]; never throws or rejects. */
export function closeQuietly(channel: Pick<MuxChannel, 'close'>, status?: Status): Promise<void> {
  return quietly(() => channel.close(status));
}

/** Destroys [transport], which nobody will run a connection over. */
export function discardTransport(transport: FrameTransport): void {
  if (transport.abort !== undefined) {
    try {
      transport.abort();
      return;
    } catch {
      // Fall back to the streams.
    }
  }
  void quietly(() => transport.writable.abort());
  void quietly(() => transport.readable.cancel());
}
