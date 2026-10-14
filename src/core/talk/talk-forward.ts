/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { Name } from '../name.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import { Deferred } from '../util/deferred.ts';
import type { TalkMessage } from './contract.ts';
import type { TalkChannel } from './talk-channel.ts';
import { TalkFrame, TalkKind } from './talk-frame.ts';
import { IncomingMessage } from './talk-message.ts';
import type { Outgoing, ResponseSink } from './talk-request.ts';
import { TalkAbortError } from './talk-util.ts';

/**
 * Forwards [incoming], a message received on one channel, to [target]:
 * message chain proxying as described on the wiki page "Switchboard
 * Proxying". An intermediary typically calls it for every message of
 * `TalkChannel.messages` it lets through, in both directions.
 *
 * A message that expects no reply is sent on [target] as a plain message;
 * if [target] is closed it is dropped. A request is sent on [target] as a
 * new request with the same procedure, payload and `STREAM` flag, and
 * everything that comes back for it is relayed to [incoming], in order:
 *
 * - stream items and `EXTEND`s are passed on as they arrive, an `EXTEND`
 *   with its payload unchanged (the deadline and renewal the far responder
 *   declared, whose buffer covers the hops);
 * - a stream request keeps its item window: forwarded with the window
 *   [incoming] has left (the one it opened with, and the credits that
 *   arrived for it before it was forwarded), and every credit the
 *   requester grants afterwards is forwarded as a credit for the forwarded
 *   request, the same way for stream requests nested in replies. The
 *   intermediary grants nothing of its own and keeps no window: what the
 *   far responder sends is relayed at once, so the requester's window
 *   bounds the far responder end to end (wiki page "Polyverse Switchboard
 *   Talk", "Item windows"). Items the intermediary sent itself before
 *   forwarding took their share of the window; those still waiting for it
 *   take the first credits, which are not forwarded;
 * - the final response is passed on as the final reply, with its trailing
 *   payload;
 * - an item or final response that is itself a request is forwarded the
 *   other way, as an item request or chained response to [incoming], and
 *   the answer to it is relayed back the same way, so chains of any depth
 *   work in both directions;
 * - an abort response from [target]'s peer is passed on with the same
 *   status;
 * - every other failure of the forwarded request (the channel to the
 *   target lost or closed, a channel abort, a local limit) is answered
 *   with `ABORT UNAVAILABLE`: the intermediary's own failures, such as its
 *   `CONNECTION_LOST` or `DEADLINE_EXCEEDED`, are never presented as the
 *   far peer's answer;
 * - a cancel of [incoming] (see `TalkMessage.isCancelled`: the peer
 *   cancelled it, its channel was lost, or the handler that forwarded it
 *   then threw) cancels the forwarded request. A reply that cannot be
 *   delivered also cancels it.
 *
 * Procedure names and payloads pass through byte for byte, including
 * names that are not valid UTF-8; an absent procedure field on a response
 * stays absent (a request forwarded without one gets the empty name).
 *
 * The intermediary imposes no timeouts of its own: the responder timeout
 * and the credit bound of every forwarded request are disabled
 * (`TalkMessage.setReplyTimeout`, `TalkMessage.setCreditTimeout`) and the
 * requests it sends have no requester timeout, so the timeouts of the two
 * real peers apply end to end, with `EXTEND` forwarded.
 *
 * Flow control passes through hop by hop (wiki page "Switchboard
 * Proxying"): over mux channels, the credit of a frame that is forwarded
 * goes back to its sender only once the frame forwarded for it went out
 * on the other channel (its send window took it, or it was dropped), so
 * that a reader that stops reading on one side stalls the sender on the
 * other, rather than filling the intermediary. This holds for everything
 * relayed back (items, the final response, chained and item requests,
 * `EXTEND`), and for [incoming] itself when `forwardMessage` is called by
 * a `TalkChannel.serve` handler while it handles [incoming]. A message
 * taken with `for await` was handed over before the loop body runs: its
 * credit went back then.
 * The bytes of streamed bulk payloads are piped with their credit passed
 * through the same way; a payload the intermediary reassembled is its own,
 * sent from memory.
 *
 * The returned promise resolves once [incoming] and every request
 * forwarded on its behalf have ended, including their bulk transfers. It
 * never rejects. If [incoming] can no longer be answered (it was already
 * answered or cancelled), nothing is forwarded.
 *
 * ```ts
 * backend.serve((message) => void forwardMessage(message, client));
 * client.serve((message) => void forwardMessage(message, backend));
 * ```
 *
 * @throws {TypeError} if [incoming] was not received by a `TalkChannel`.
 * @throws {Error} if it is already being forwarded.
 */
export function forwardMessage(incoming: TalkMessage, target: TalkChannel): Promise<void> {
  if (!(incoming instanceof IncomingMessage)) {
    throw new TypeError('the message to forward was not received by a TalkChannel');
  }
  const procedure = incoming.frame.procedure ?? Name.empty;
  if (!incoming.expectsReply) {
    const done = new Deferred<void>();
    const failed = (error: SwitchboardError): void => {
      incoming.channel._log('debug', `forwarded ${incoming.frame} dropped: ${error.status}`);
      incoming._bulk?.abandon();
      done.resolve();
    };
    const accepted = incoming._takeCredit();
    try {
      const [payload, bulk] = incoming._forwardPayload();
      void target._sendFrame(new TalkFrame({ kind: TalkKind.message, procedure, payload }), {
        bulk,
        onSent: (out) => {
          done.resolve(out?.done.then(noop, noop));
        },
        onFailure: failed,
        onAccepted: accepted,
      });
    } catch (e) {
      accepted();
      if (!(e instanceof SwitchboardError)) {
        throw e;
      }
      failed(e);
    }
    return done.promise;
  }
  const stream = incoming.expectsStream;
  return Relay.start(incoming, (relay, accepted) => {
    const [payload, bulk] = incoming._forwardPayload();
    return target._startRequest({
      stream,
      window: incoming._window ?? TalkFrame.unlimitedWindow,
      timeout: 0,
      sink: relay,
      bulk,
      onAccepted: accepted,
      build: (id, window) =>
        new TalkFrame({
          kind: TalkKind.message,
          procedure,
          requestId: id,
          stream,
          window,
          payload,
        }),
    });
  });
}

function noop(): void {}

/** UNAVAILABLE, for a failure of the intermediary itself. */
function localFailure(cause: Status): Status {
  return Status.of(
    StatusCode.unavailable,
    `forwarding failed (${cause.name ?? `code ${cause.code}`})`,
  );
}

/**
 * Relays what one forwarded request receives back to the request it was
 * forwarded for.
 *
 * The two requests may be on any two channels, in either direction: for a
 * request from the client, [incoming] is on the client channel and the
 * forwarded request on the backend channel; for a request the backend
 * makes in a reply (a chained response or an item request) it is the
 * other way round.
 */
class Relay implements ResponseSink {
  /** The request answered with whatever the forwarded request receives. */
  readonly incoming: IncomingMessage;

  /** The forwarded request, once sent. */
  #out: Outgoing | undefined;

  /** Cancellation requested while the forwarded request was being sent. */
  #cancelWhenSent: Status | undefined;

  /** Credits granted for {@link incoming} while the forwarded request was being sent, forwarded once it was. */
  #creditWhenSent = 0;

  /** The forwarded request ended. */
  #ended = false;

  /** The outgoing handle is not installed until the initial send returns. */
  #starting = true;
  #finishing = false;

  /** Requests forwarded on behalf of replies to {@link incoming}, still going. */
  #nested = 0;

  /** Resolves when the forwarded request and every nested one ended. */
  readonly #done = new Deferred<void>();

  private constructor(incoming: IncomingMessage) {
    this.incoming = incoming;
  }

  /**
   * Forwards [incoming] with [send], which sends the forwarded request
   * with the relay as its response sink, and calls the function it gets
   * once that request went out (see `TalkChannel._sendFrame`), which
   * returns the credit of [incoming]'s frame if it is being handed over.
   */
  static start(
    incoming: IncomingMessage,
    send: (relay: Relay, accepted: () => void) => Outgoing,
  ): Promise<void> {
    if (incoming._forwarded) {
      throw new Error(`request ${incoming.requestId} is already forwarded`);
    }
    const relay = new Relay(incoming);
    if (!incoming.canReply) {
      relay.#starting = false;
      relay.#end();
      return relay.#done.promise;
    }
    incoming._forwarded = true;
    // The far requester's own timeout applies; EXTEND is forwarded. So
    // does the far responder's credit bound; credits are forwarded.
    incoming.setReplyTimeout(0);
    incoming.setCreditTimeout(0);
    incoming._cancelHook = () => relay.#onIncomingCancelled();
    incoming._creditHook = (items) => relay.#onIncomingCredit(items);
    // Replies of the application's own that wait for the window (it is
    // then 0, and so is the forwarded request's) take the first credits.
    incoming._creditOwed = incoming._queuedItems;
    const accepted = incoming._takeCredit();
    try {
      relay.#out = send(relay, accepted);
    } catch (e) {
      accepted();
      let status: Status;
      if (e instanceof SwitchboardError) {
        status = e.status;
        incoming.channel._log('debug', `request ${incoming.requestId} not forwarded: ${e.status}`);
      } else {
        status = Status.of(StatusCode.internal);
        incoming.channel._log('error', `forwarding request ${incoming.requestId} failed`, e);
      }
      relay.#abortIncoming(localFailure(status));
      relay.#end();
    } finally {
      relay.#starting = false;
      relay.#checkDone();
    }
    const credit = relay.#creditWhenSent;
    const out = relay.#out;
    if (credit > 0 && out !== undefined) {
      relay.#creditWhenSent = 0;
      out.forwardCredit(credit);
    }
    const cancel = relay.#cancelWhenSent;
    if (cancel !== undefined) {
      relay.#cancelOutgoing(cancel);
    }
    return relay.#done.promise;
  }

  #end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.incoming._cancelHook = undefined;
    this.incoming._creditHook = undefined;
    this.#checkDone();
  }

  #addNested(nested: Promise<void>): void {
    this.#nested++;
    void nested.then(() => {
      this.#nested--;
      this.#checkDone();
    });
  }

  #checkDone(): void {
    if (this.#ended && this.#nested === 0 && !this.#starting && !this.#finishing) {
      this.#finishing = true;
      // The final header ends the request before its bytes necessarily
      // arrive. Keep the forwarding owner (notably a slot gate's idle
      // close) alive until both the upload and all replies have drained.
      const transfers = [...(this.#out?.bulkOuts ?? []), ...this.incoming._bulkOuts];
      this.#done.resolve(Promise.all(transfers.map((out) => out.done.then(noop, noop))).then(noop));
    }
  }

  #onIncomingCancelled(): void {
    this.#cancelOutgoing(this.incoming._cancelStatus ?? Status.of(StatusCode.cancelled));
  }

  /** The requester granted [items] more items of {@link incoming}: a credit for the forwarded request, granting nothing of the intermediary's own. */
  #onIncomingCredit(items: number): void {
    const out = this.#out;
    if (out === undefined) {
      this.#creditWhenSent += items;
      return;
    }
    out.forwardCredit(items);
  }

  #cancelOutgoing(status: Status): void {
    const out = this.#out;
    if (out === undefined) {
      this.#cancelWhenSent ??= status;
      return;
    }
    out.channel._cancelOutgoing(out, status);
  }

  #abortIncoming(status: Status): void {
    if (!this.incoming.canReply) {
      return;
    }
    try {
      void this.incoming._replyAbort(status);
    } catch (e) {
      if (!(e instanceof SwitchboardError)) {
        throw e;
      }
      this.incoming.channel._log(
        'debug',
        `abort of request ${this.incoming.requestId} not sent: ${e.status}`,
      );
    }
  }

  /**
   * Sends one response to {@link incoming}, relaying [source]: [send]
   * calls the function it gets once the response went out, which returns
   * the credit of [source]'s frame. If that fails, {@link incoming} is
   * answered with UNAVAILABLE instead and the forwarded request cancelled.
   */
  #deliver(source: IncomingMessage, send: (accepted: () => void) => void): void {
    if (!this.incoming.canReply) {
      return;
    }
    const accepted = source._takeCredit();
    try {
      send(accepted);
    } catch (e) {
      accepted();
      if (!(e instanceof SwitchboardError)) {
        throw e;
      }
      this.incoming.channel._log(
        'debug',
        `response to request ${this.incoming.requestId} not sent: ${e.status}`,
      );
      this.#abortIncoming(localFailure(e.status));
      this.#cancelOutgoing(Status.of(StatusCode.cancelled, 'requester unreachable'));
    }
  }

  item(item: IncomingMessage): void {
    if (!item.expectsReply) {
      this.#deliver(item, (accepted) => {
        const [payload, bulk] = item._forwardPayload();
        void this.incoming._replyItem(
          payload,
          item.frame.procedure,
          { bulk, onAccepted: accepted },
          false,
        );
      });
      return;
    }
    if (!item.canReply) {
      // Refused by the channel (incoming request limit); the sender
      // already has its answer.
      this.incoming.channel._log('debug', `item request ${item.requestId} refused, not forwarded`);
      return;
    }
    this.#addNested(
      Relay.start(item, (relay, accepted) => {
        const [payload, bulk] = item._forwardPayload();
        return this.incoming._startItemRequest(
          payload,
          item.frame.procedure,
          {
            stream: item.expectsStream,
            window: item._window ?? TalkFrame.unlimitedWindow,
            timeout: 0,
            sink: relay,
            bulk,
            onAccepted: accepted,
          },
          false,
        );
      }),
    );
  }

  complete(message: IncomingMessage): void {
    if (!message.expectsReply) {
      this.#deliver(message, (accepted) => {
        const [payload, bulk] = message._forwardPayload();
        void this.incoming._reply(payload, message.frame.procedure, {
          bulk,
          onAccepted: accepted,
        });
      });
    } else if (!message.canReply) {
      // The chained request was refused by the channel (incoming request
      // limit), so the chain cannot continue.
      this.#abortIncoming(localFailure(Status.of(StatusCode.resourceExhausted)));
    } else {
      this.#addNested(
        Relay.start(message, (relay, accepted) => {
          const [payload, bulk] = message._forwardPayload();
          return this.incoming._startReplyRequest(payload, message.frame.procedure, {
            stream: message.expectsStream,
            window: message._window ?? TalkFrame.unlimitedWindow,
            timeout: 0,
            sink: relay,
            bulk,
            onAccepted: accepted,
          });
        }),
      );
      // Sent, the chained request is the final of {@link incoming}, which
      // is then finished and this does nothing. Not sent (over the
      // outgoing request limit of its channel, say), `start` answered
      // [message] only, and {@link incoming} still needs its final.
      this.#abortIncoming(localFailure(Status.of(StatusCode.unavailable)));
    }
    this.#end();
  }

  fail(error: SwitchboardError): void {
    const remote = error instanceof TalkAbortError && !error.isChannelAbort;
    this.#abortIncoming(remote ? error.status : localFailure(error.status));
    this.#end();
  }

  extended(message: IncomingMessage): void {
    // Byte for byte: the far responder's buffer covers the hops.
    this.#deliver(message, (accepted) => this.incoming._extendRaw(message.frame.payload, accepted));
  }
}
