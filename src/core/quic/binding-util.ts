/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Small helpers the binding's connection and channels share. Not exported
 * from the package.
 */

import type { MuxOptions } from '../mux/contract.ts';
import type { Clock } from '../util/clock.ts';
import { QuicError } from './contract.ts';
import type { QuicBindingOptions } from './quic-options.ts';
import { quicDefaultRecordLimit, quicMaxRecordLength } from './records.ts';

/** The logger name of the binding, as in Dart. */
export const quicLoggerName = 'Switchboard.Quic';

/**
 * How long a sign that a WebTransport session is gone waits for the
 * session's end, in milliseconds. The side that terminates a session resets
 * and stops its streams with `WT_SESSION_GONE`, which carries no
 * application code and can overtake the close itself: a reset or
 * `STOP_SENDING` without a code on the control stream is a protocol error
 * only after this grace, and on a channel's stream reads as `UNKNOWN` only
 * after it. The web provider gives the browser as long to report the
 * session's end once a stream reported it gone.
 */
export const sessionGoneGrace = 2000;

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

/**
 * Checks the options a connection of the binding starts with:
 * `QuicBindingConnection.start` does, and `connectQuic` and the listeners
 * before anything is dialled or accepted.
 *
 * @throws {RangeError} for a `recordLimit` outside 32752 to 2^30 - 1, or
 * binding limits and windows that are not positive.
 */
export function checkQuicBindingOptions(
  options: MuxOptions,
  quicOptions: QuicBindingOptions,
): void {
  const limit = quicOptions.recordLimit;
  if (!Number.isInteger(limit) || limit < quicDefaultRecordLimit || limit > quicMaxRecordLength) {
    throw new RangeError(
      `quicOptions.recordLimit must be ${quicDefaultRecordLimit} to ${quicMaxRecordLength}, not ${limit}`,
    );
  }
  if (
    !(quicOptions.maxPendingStreams >= 1) ||
    !(quicOptions.controlOutputMark >= 1) ||
    !(quicOptions.writeBatchBytes >= 1) ||
    !(quicOptions.maxHeldControlMessages >= 1) ||
    !(options.initialWindow >= 1) ||
    options.initialWindow > 0xffffffff ||
    !(options.bulkChunkSize >= 1)
  ) {
    throw new RangeError('binding limits and windows must be positive');
  }
}
