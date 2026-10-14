/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Helpers the Talk modules share: wire statuses, the item window of the
 * request API, and promises safe to drop.
 *
 * @internal
 */

import { Status, StatusCode, SwitchboardError } from '../status.ts';
import { TalkFrame } from './talk-frame.ts';

/**
 * A failure caused by the peer sending ABORT: an abort response to one of
 * our requests, or a channel abort. `status` is the peer's status (an
 * abort carrying OK or nothing reads as UNKNOWN).
 *
 * Every other failure a Talk channel reports is a plain
 * {@link SwitchboardError}: our own requester timeout, our own cancel,
 * loss or closing of the channel, a protocol error, a local limit. The
 * distinction matters to intermediaries, which pass on only what the peer
 * said (see `forwardMessage`).
 */
export class TalkAbortError extends SwitchboardError {
  /** True when the peer aborted the whole channel rather than answering one request with an abort. */
  readonly isChannelAbort: boolean;

  constructor(status: Status, options?: { isChannelAbort?: boolean }) {
    super(status);
    this.name = 'TalkAbortError';
    this.isChannelAbort = options?.isChannelAbort ?? false;
  }

  override toString(): string {
    return `TalkAbortError: ${this.isChannelAbort ? 'channel abort, ' : ''}${this.status}`;
  }
}

/** The logger name of the Talk layer. */
export const talkLoggerName = 'Switchboard.Talk';

/**
 * The longest delay a timer is armed with at once: JavaScript timers
 * overflow above a signed 32-bit millisecond delay, so longer expiries are
 * re-armed from their deadline in steps.
 */
export const maxTimerDelay = 0x7fffffff;

/** The status of a received abort; an abort cannot report success, so OK reads as UNKNOWN. */
export function abortStatus(frame: TalkFrame): Status {
  const status = frame.status;
  return status.isOk ? new Status(StatusCode.unknown, status.reason) : status;
}

/** @throws {RangeError} if [status] is OK: an abort cannot report success. */
export function checkAbortStatus(status: Status): void {
  if (status.isOk) {
    throw new RangeError('an abort cannot report success');
  }
}

/** CONNECTION_LOST is local only; on the wire it is UNAVAILABLE. */
export function wireAbortStatus(status: Status): Status {
  if (status.code !== StatusCode.connectionLost) {
    return status;
  }
  const reason = status.reason.length === 0 ? '' : `: ${status.reason}`;
  return Status.of(StatusCode.unavailable, `connection lost${reason}`);
}

/** Mux CLOSE carries neither application codes nor CONNECTION_LOST. */
export function wireStatus(status: Status): Status {
  if (!status.isApplicationCode) {
    return wireAbortStatus(status);
  }
  const reason = status.reason.length === 0 ? '' : `: ${status.reason}`;
  return Status.of(StatusCode.unknown, `application status ${status.code}${reason}`);
}

/**
 * The wire value of an item window given to the request API: undefined is
 * {@link TalkFrame.unlimitedWindow}.
 *
 * @throws {RangeError} for a value that is negative, not an integer, or not below it.
 */
export function windowOf(window: number | undefined): number {
  if (window === undefined) {
    return TalkFrame.unlimitedWindow;
  }
  if (!Number.isInteger(window) || window < 0 || window >= TalkFrame.unlimitedWindow) {
    throw new RangeError(`window ${window} is outside 0 to ${TalkFrame.unlimitedWindow - 1}`);
  }
  return window;
}

/** [error] as a {@link SwitchboardError}: itself, or INTERNAL with its text. */
export function asSwitchboardError(error: unknown): SwitchboardError {
  return error instanceof SwitchboardError
    ? error
    : SwitchboardError.of(StatusCode.internal, String(error), { cause: error });
}

/** [callback], called once however often the result is. */
export function once(callback: () => void): () => void {
  let pending = true;
  return () => {
    if (pending) {
      pending = false;
      callback();
    }
  };
}

/** Does nothing; given back where a callback is owed but nothing is to be done. */
export function noop(): void {}

/** [promise], marked handled: a rejection nobody waits for is not reported; whoever awaits it still sees it. */
export function quiet<T>(promise: Promise<T>): Promise<T> {
  promise.catch(noop);
  return promise;
}

/** A rejected promise that is never reported as unhandled. */
export function rejected<T = void>(error: unknown): Promise<T> {
  return quiet(Promise.reject(error));
}

/** Whether [value] is a promise or another thenable. */
export function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}
