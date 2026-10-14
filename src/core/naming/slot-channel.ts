/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { defaultMuxOptions, type MuxChannel, type MuxChannelCarrier } from '../mux/contract.ts';
import type { Name } from '../name.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import { Deferred } from '../util/deferred.ts';
import { type Logger, noopLogger } from '../util/logger.ts';

const logName = 'Switchboard.Router';

/**
 * Opens the replacement channel after a `MOVED` rejection, or resolves with
 * undefined when there is nowhere else to go. Rejects with a
 * `SwitchboardError` `NOT_FOUND` when the caller's filter refuses the new
 * owner: the {@link SlotChannel} then ends with that status.
 */
export type SlotReopen = (moved: Status) => Promise<MuxChannel | undefined>;

/** What a {@link SlotChannel} takes beside its channel and its reopen. */
export interface SlotChannelOptions {
  /**
   * Largest number of bytes held while the replacement is being opened, each
   * subframe counted at its flow-control cost (`MuxChannel.sendCost` of the
   * current channel); beyond, the channel is closed with
   * `RESOURCE_EXHAUSTED`. 0 (or less) means no limit. Default: the mux's
   * default initial window (`MuxOptions.initialWindow`, 64 KiB).
   */
  readonly maxHeldBytes?: number;
  /**
   * Called when a replacement takes over from [previous], so that the bulk
   * channels the new owner opens go where the first one's would (Talk's
   * bulk routes follow the replacement).
   */
  readonly onReplaced?: (previous: MuxChannel, next: MuxChannel) => void;
  /** Diagnostics, logged as `Switchboard.Router`. */
  readonly logger?: Logger;
}

/** A subframe held while the replacement channel is being opened. */
interface Held {
  readonly subframe: Uint8Array;
  readonly sent: Deferred<void>;
}

/**
 * A channel to the owner of a shard slot, opened with
 * `Switchboard.openChannelToSlot`.
 *
 * It forwards to an underlying `MuxChannel` ({@link channel}) and behaves
 * like one. In addition it retries **once** when the owner rejects the
 * channel with CLOSE `MOVED` (the slot moved, and the router's table was
 * stale) **before anything was sent on it or received from it**: the
 * replacement channel is opened to the owner the rejection names
 * (`MovedStatus`; or, if it names none, the one the router finds by
 * refreshing the slot) with the same open payload, and takes over
 * transparently. Subframes the caller sends while the replacement is being
 * opened are sent on it, in order. At most {@link maxHeldBytes} are held
 * meanwhile (each subframe counted at its flow-control cost on the channel,
 * `MuxChannel.sendCost`: its length plus 16 bytes on the frame mux): a
 * subframe beyond that is dropped, the channel is closed with
 * `RESOURCE_EXHAUSTED`, which {@link done} reports at once, and a
 * replacement opened after all is closed with it too. Through the proxy the
 * caller is a remote client, whose subframes would otherwise grow the
 * proxy's memory for as long as the retry takes.
 *
 * Once the caller has sent a subframe, or a subframe has arrived, a `MOVED`
 * close is surfaced like any other close ({@link done}), because the
 * subframes already sent are gone with the old channel. `MOVED` means the
 * owner processed nothing on the channel, so the caller may open the slot
 * again and send the same data. Protocols in which the server speaks first
 * (a greeting or a snapshot before the client sends) get the retry on every
 * open; Talk clients that send their first request at once see a `MOVED`
 * only when the router's table was stale, and retry at their own level.
 *
 * A channel the owner was serving when the slot moved on is closed with
 * `RELOCATED` carrying the same owner and epoch fields
 * (`MovedStatus.fromStatus` reads them). It is never retried: the work may
 * have taken effect, and only the application knows whether it can be
 * repeated.
 */
export class SlotChannel implements MuxChannelCarrier, AsyncDisposable {
  /** The sharded type. */
  readonly type: Name;
  /** The slot this channel is addressed to. */
  readonly slot: number;
  /**
   * Largest number of bytes held while the replacement is being opened, at
   * flow-control cost; beyond, the channel is closed with
   * `RESOURCE_EXHAUSTED`. 0 (or less) means no limit.
   */
  readonly maxHeldBytes: number;

  readonly #reopen: SlotReopen;
  readonly #onReplaced: ((previous: MuxChannel, next: MuxChannel) => void) | undefined;
  readonly #logger: Logger;
  #current: MuxChannel;
  /** The reader of {@link readable}'s current source, and the channel it reads. */
  #reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  #readerOf: MuxChannel | undefined;
  /** The first channel once rejected: nothing more is read from it. */
  #abandoned: MuxChannel | undefined;
  #readable: ReadableStream<Uint8Array> | undefined;
  #writable: WritableStream<Uint8Array> | undefined;
  readonly #done = new Deferred<Status>();
  readonly #pending: Held[] = [];
  #pendingBytes = 0;
  #manualCredit = false;

  #sent = false;
  #received = false;
  #retried = false;
  #reopening = false;
  #final = false;
  #closeRequested: Status | undefined;
  /** Woken when a replacement took over or the channel ended. */
  #settled = new Deferred<void>();

  /** The status the channel was closed with when the held subframes went beyond {@link maxHeldBytes}. */
  #overflow: Status | undefined;

  /**
   * @internal Wraps [channel], the first channel opened for [slot] of
   * [type]; [reopen] opens the replacement. Created by
   * `Switchboard.openChannelToSlot` and the proxy, with the node's initial
   * window as `maxHeldBytes`: one window, what a client of the proxy can
   * send before it needs credit.
   */
  constructor(
    type: Name,
    slot: number,
    channel: MuxChannel,
    reopen: SlotReopen,
    options: SlotChannelOptions = {},
  ) {
    this.type = type;
    this.slot = slot;
    this.#current = channel;
    this.#reopen = reopen;
    this.maxHeldBytes = options.maxHeldBytes ?? defaultMuxOptions.initialWindow;
    this.#onReplaced = options.onReplaced;
    this.#logger = options.logger ?? noopLogger;
    this.#watch(channel);
  }

  #log(level: 'debug' | 'info', message: string): void {
    try {
      this.#logger.log(level, logName, message);
    } catch {
      // A logger that throws must not break the channel.
    }
  }

  /** The current underlying channel: the first one, or the replacement after a retry. */
  get channel(): MuxChannel {
    return this.#current;
  }

  /** Whether the channel was rejected with `MOVED` and replaced. */
  get retried(): boolean {
    return this.#retried;
  }

  /** Whether DATA may be sent: the current channel is open, or its replacement is being opened. */
  get canSend(): boolean {
    return this.#closeRequested === undefined && (this.#holding || this.#current.canSend);
  }

  /**
   * Incoming DATA subframes of the current channel; one reader, buffered
   * (by the mux channel) until read. Ends when the final channel ends; read
   * {@link done} for the reason.
   */
  get readable(): ReadableStream<Uint8Array> {
    this.#readable ??= new ReadableStream<Uint8Array>(
      {
        pull: (controller) => this.#pull(controller),
        cancel: (reason) => {
          const reader = this.#reader;
          this.#reader = undefined;
          this.#readerOf = undefined;
          return reader?.cancel(reason);
        },
      },
      { highWaterMark: 0 },
    );
    return this.#readable;
  }

  /**
   * Outgoing subframes, as `MuxChannel.writable`: a `write` is a
   * {@link send} once {@link canSend} (dropped otherwise; a subframe the
   * channel refuses for its size fails the write); `close()` is
   * `close(OK)`; `abort(reason)` closes with the status of a
   * `SwitchboardError` (an application code becomes `UNKNOWN`), else
   * `INTERNAL`.
   */
  get writable(): WritableStream<Uint8Array> {
    this.#writable ??= new WritableStream<Uint8Array>({
      write: async (subframe) => {
        if (!this.canSend) {
          return;
        }
        try {
          await this.send(subframe);
        } catch (error) {
          if (!(error instanceof SwitchboardError) || !error.is(StatusCode.failedPrecondition)) {
            throw error;
          }
          // Closed while waiting: dropped, as a write after the end is.
        }
      },
      close: () => this.close(),
      abort: (reason: unknown) => {
        let status =
          reason instanceof SwitchboardError
            ? reason.status
            : Status.of(StatusCode.internal, String(reason));
        if (status.isApplicationCode) {
          status = Status.of(StatusCode.unknown, status.toString());
        }
        return this.close(status);
      },
    });
    return this.#writable;
  }

  /**
   * The end status of the final channel. A `MOVED` that was retried is not
   * reported; a `MOVED` that was not is, as is a `RELOCATED`. Never rejects.
   */
  get done(): Promise<Status> {
    return this.#done.promise;
  }

  /**
   * Sends one DATA subframe, as `MuxChannel.send`, whose promise the
   * returned one is. While the replacement channel is being opened the
   * subframe is held and sent on it, and the promise settles as that send's
   * does; it rejects if the channel ends first. One that would take the held
   * subframes beyond {@link maxHeldBytes} is dropped and closes the channel
   * with `RESOURCE_EXHAUSTED`, and its promise rejects with that status. A
   * rejection is never reported as unhandled.
   *
   * @throws {SwitchboardError} `FAILED_PRECONDITION` once the channel is closing.
   */
  send(subframe: Uint8Array): Promise<void> {
    if (this.#closeRequested !== undefined) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'slot channel is closing');
    }
    if (this.#holding) {
      const size = this.#current.sendCost(subframe.length);
      if (this.maxHeldBytes > 0 && this.#pendingBytes + size > this.maxHeldBytes) {
        const failed = Promise.reject(new SwitchboardError(this.#overflowed()));
        failed.catch(() => {});
        return failed;
      }
      const held: Held = { subframe: subframe.slice(), sent: new Deferred<void>() };
      this.#pending.push(held);
      this.#pendingBytes += size;
      return held.sent.promise;
    }
    this.#sent = true;
    return this.#current.send(subframe);
  }

  /**
   * As `MuxChannel.isWritable`, for the current channel: false while a
   * replacement is being opened (subframes sent meanwhile are held, see
   * {@link send}) and once the channel is closing.
   */
  get isWritable(): boolean {
    return this.#closeRequested === undefined && !this.#holding && this.#current.isWritable;
  }

  /**
   * As `MuxChannel.whenWritable`, for the current channel: while a
   * replacement is being opened, or the first channel was rejected with
   * nothing sent, it waits for the replacement and resolves when that one is
   * writable. Rejects with `FAILED_PRECONDITION` once the slot channel can no
   * longer send (closed, or ended without a replacement). Never reports an
   * unhandled rejection.
   */
  get whenWritable(): Promise<void> {
    const writable = this.#whenWritable();
    writable.catch(() => {});
    return writable;
  }

  async #whenWritable(): Promise<void> {
    for (;;) {
      if (this.#closeRequested !== undefined || this.#final) {
        throw SwitchboardError.of(
          StatusCode.failedPrecondition,
          `slot channel is ${this.#final ? 'closed' : 'closing'}`,
        );
      }
      if (this.#holding) {
        await this.#settled.promise;
        continue;
      }
      const channel = this.#current;
      try {
        await channel.whenWritable;
        return;
      } catch (error) {
        // The channel stopped sending: a rejection that is retried waits for
        // the replacement, any other end fails.
        if (error instanceof SwitchboardError && (channel !== this.#current || this.#holding)) {
          continue;
        }
        throw error;
      }
    }
  }

  #settle(): void {
    const settled = this.#settled;
    this.#settled = new Deferred<void>();
    settled.resolve();
  }

  /** As `MuxChannel.manualCredit`, for the current channel and a replacement. */
  get manualCredit(): boolean {
    return this.#manualCredit;
  }

  set manualCredit(value: boolean) {
    this.#manualCredit = value;
    this.#current.manualCredit = value;
  }

  /** As `MuxChannel.consumed`, on the current channel: subframes come from the replacement only if none came from the first channel. */
  consumed(bytes: number, subframes = 1): void {
    this.#current.consumed(bytes, subframes);
  }

  /**
   * @internal Gives up the retry: the channel stays on its current mux
   * channel whatever ends it, as once something was sent. A forwarded bulk
   * channel, opened on the current mux channel in either direction,
   * commits it.
   */
  commit(): void {
    this.#sent = true;
  }

  /**
   * Whether a subframe must be held for a possible replacement: one is being
   * opened, or the first channel is already closing (a rejection on its way)
   * and nothing has gone out on it.
   */
  get #holding(): boolean {
    return (
      this.#reopening || (!this.#retried && !this.#sent && !this.#current.canSend && !this.#final)
    );
  }

  /**
   * Closes the current channel with [status], as `MuxChannel.close`; a
   * replacement being opened is closed as soon as it is open. Resolves when
   * {@link done} does.
   *
   * @throws {RangeError} for an application status code, which a mux CLOSE
   * cannot carry.
   */
  close(status: Status = Status.ok): Promise<void> {
    if (status.isApplicationCode) {
      throw new RangeError(`application status codes are not allowed in mux CLOSE: ${status}`);
    }
    if (this.#closeRequested === undefined && !this.#done.isSettled) {
      this.#closeRequested = status;
      this.#dropPending();
      if (!this.#reopening) {
        void this.#current.close(status).catch(() => {});
      }
    }
    return this.#done.promise.then(() => {});
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  #dropPending(): void {
    if (this.#pending.length === 0) {
      return;
    }
    const error = SwitchboardError.of(
      StatusCode.failedPrecondition,
      'slot channel ended before the subframe was sent',
    );
    const held = this.#pending.splice(0);
    this.#pendingBytes = 0;
    for (const h of held) {
      h.sent.reject(error);
    }
  }

  /**
   * Stops holding: the held subframes went beyond {@link maxHeldBytes}. The
   * channel ends at once with `RESOURCE_EXHAUSTED`; a replacement being
   * opened is closed with it as soon as it is open.
   */
  #overflowed(): Status {
    const status = Status.of(
      StatusCode.resourceExhausted,
      `send buffer of ${this.maxHeldBytes} bytes exceeded`,
    );
    this.#log(
      'info',
      `${this.type}/${this.slot}: more than ${this.maxHeldBytes} bytes sent while the ` +
        'replacement channel was being opened; closed',
    );
    this.#overflow = status;
    this.#closeRequested = status;
    if (!this.#reopening) {
      void this.#current.close(status).catch(() => {});
    }
    this.#finish(status);
    return status;
  }

  #watch(channel: MuxChannel): void {
    void channel.done.then((status) => this.#ended(channel, status));
  }

  /** Reads the next subframe of the current channel into [controller], across a replacement. */
  async #pull(controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> {
    for (;;) {
      const channel = this.#current;
      if (this.#reopening || this.#abandoned === channel) {
        // Nothing to read until the replacement takes over, or ever.
        if (this.#final) {
          controller.close();
          return;
        }
        await this.#settled.promise;
        continue;
      }
      if (this.#readerOf !== channel) {
        this.#reader = channel.readable.getReader();
        this.#readerOf = channel;
      }
      const reader = this.#reader as ReadableStreamDefaultReader<Uint8Array>;
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await reader.read();
      } catch (error) {
        if (channel !== this.#current) continue;
        controller.error(error);
        return;
      }
      if (channel !== this.#current) {
        // Replaced meanwhile; the first channel had nothing to deliver.
        continue;
      }
      if (!result.done) {
        this.#received = true;
        controller.enqueue(result.value);
        return;
      }
      // The current channel's subframes ended: the end of the slot channel,
      // unless a replacement follows.
      while (!this.#final && channel === this.#current) {
        await this.#settled.promise;
      }
      if (channel === this.#current) {
        controller.close();
        return;
      }
    }
  }

  #ended(channel: MuxChannel, status: Status): void {
    if (channel !== this.#current) {
      return;
    }
    const retry =
      status.is(StatusCode.moved) &&
      !this.#retried &&
      !this.#sent &&
      !this.#received &&
      channel.bufferedBytes === 0 &&
      this.#closeRequested === undefined;
    if (retry) {
      void this.#retry(status);
    } else {
      this.#finish(status);
    }
  }

  #finish(status: Status): void {
    this.#final = true;
    this.#settle();
    this.#dropPending();
    this.#done.resolve(status);
  }

  async #retry(moved: Status): Promise<void> {
    this.#retried = true;
    this.#reopening = true;
    this.#abandoned = this.#current;
    const old = this.#reader;
    this.#reader = undefined;
    this.#readerOf = undefined;
    await old?.cancel().catch(() => {});
    let next: MuxChannel | undefined;
    let refused: Status | undefined;
    if (this.#overflow === undefined) {
      try {
        next = await this.#reopen(moved);
      } catch (error) {
        this.#log('debug', `${this.type}/${this.slot}: retry after ${moved} failed: ${error}`);
        if (error instanceof SwitchboardError && error.is(StatusCode.notFound)) {
          refused = error.status;
        }
      }
    }
    this.#reopening = false;
    const overflow = this.#overflow;
    if (overflow !== undefined) {
      // Ended already; the replacement is not used.
      if (next !== undefined) {
        void next.close(overflow).catch(() => {});
      }
      return;
    }
    if (next === undefined) {
      // Reported as the rejection itself, so that the caller re-resolves; or,
      // when its filter refused the new owner, as that.
      this.#finish(refused ?? moved);
      return;
    }
    this.#log('debug', `${this.type}/${this.slot} moved (${moved}), retried on channel ${next.id}`);
    next.manualCredit = this.#manualCredit;
    // Bulk channels the new owner opens go where the first one's would.
    this.#onReplaced?.(this.#current, next);
    this.#current = next;
    this.#settle();
    this.#watch(next);
    const requested = this.#closeRequested;
    if (requested !== undefined) {
      void next.close(requested).catch(() => {});
      return;
    }
    const pending = this.#pending.splice(0);
    this.#pendingBytes = 0;
    try {
      for (const held of pending) {
        this.#sent = true;
        const sent = next.send(held.subframe);
        sent.then(
          () => held.sent.resolve(),
          (error: unknown) => held.sent.reject(error),
        );
      }
    } catch (error) {
      const status = error instanceof SwitchboardError ? error.status : Status.ok;
      this.#log('debug', `${this.type}/${this.slot}: held subframes not sent: ${status}`);
      for (const held of pending) {
        held.sent.reject(error);
      }
      void next.close(status.isOk ? Status.ok : status).catch(() => {});
    }
  }

  toString(): string {
    return (
      `SlotChannel(${this.type}/${this.slot} on channel ${this.#current.id}` +
      `${this.#retried ? ', retried' : ''})`
    );
  }
}
