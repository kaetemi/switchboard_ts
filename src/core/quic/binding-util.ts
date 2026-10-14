/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Small helpers the binding's connection and channels share. Not exported
 * from the package.
 */

import type { Clock } from '../util/clock.ts';
import { QuicError } from './contract.ts';

/** The logger name of the binding, as in Dart. */
export const quicLoggerName = 'Switchboard.Quic';

/**
 * Whether [error] is the end of the session, which every pending operation
 * reports, rather than a failure of one stream.
 */
export function isSessionEnd(error: unknown): boolean {
  return error instanceof QuicError && error.kind === 'sessionEnded';
}

/** The promise of [action], which also turns a synchronous throw into a rejection. */
export function attempt<T>(action: () => T | PromiseLike<T>): Promise<T> {
  try {
    return Promise.resolve(action());
  } catch (error) {
    return Promise.reject(error);
  }
}

/**
 * [promise], or what [onTimeout] returns (or throws) once [ms] milliseconds
 * of [clock] passed first; [promise] itself when [ms] is not positive.
 */
export function withTimeout<T>(
  clock: Clock,
  promise: Promise<T>,
  ms: number,
  onTimeout: () => T | PromiseLike<T>,
): Promise<T> {
  if (!(ms > 0)) {
    return promise;
  }
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const timer = clock.setTimeout(() => {
      if (settled) {
        return;
      }
      settled = true;
      attempt(onTimeout).then(resolve, reject);
    }, ms);
    promise.then(
      (value) => {
        if (!settled) {
          settled = true;
          clock.clearTimeout(timer);
          resolve(value);
        }
      },
      (error: unknown) => {
        if (!settled) {
          settled = true;
          clock.clearTimeout(timer);
          reject(error);
        }
      },
    );
  });
}

/** Resolves after [ms] milliseconds of [clock] (0: the next macrotask). */
export function delay(clock: Clock, ms: number): Promise<void> {
  return new Promise((resolve) => {
    clock.setTimeout(resolve, ms);
  });
}
