/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The handle of a stream request: its items, its final response, its item
 * window.
 *
 * @internal
 */

import { Status, StatusCode } from '../status.ts';
import type { TalkMessage, TalkStream } from './contract.ts';
import type { Delivery } from './talk-delivery.ts';
import type { Outgoing } from './talk-request.ts';

/** The handle of a stream request, returned by `streamRequest` and the stream reply requests. */
export class TalkStreamHandle implements TalkStream {
  readonly #pending: Outgoing;

  constructor(pending: Outgoing) {
    this.#pending = pending;
  }

  get requestId(): number {
    return this.#pending.id;
  }

  /**
   * The STREAM_ITEMs, in order; one consumer. The credit of an item goes
   * back to the peer when it is taken. Ends when the final response
   * arrives; throws the failure of the request (a `TalkAbortError` when
   * the peer aborts it or the channel). Stopping the iteration early
   * cancels the request; item requests nobody took are answered
   * `ABORT CANCELLED`.
   */
  get items(): AsyncIterable<TalkMessage> {
    return this.#pending.items as Delivery;
  }

  get done(): Promise<TalkMessage> {
    return this.#pending.completer.promise;
  }

  get deadline(): number | undefined {
    return this.#pending.deadline;
  }

  get window(): number | undefined {
    return this.#pending.window;
  }

  grant(items: number): void {
    this.#pending.grant(items);
  }

  cancel(status?: Status): void {
    this.#pending.channel._cancelOutgoing(this.#pending, status ?? Status.of(StatusCode.cancelled));
  }

  [Symbol.asyncIterator](): AsyncIterator<TalkMessage> {
    return (this.#pending.items as Delivery)[Symbol.asyncIterator]();
  }
}
