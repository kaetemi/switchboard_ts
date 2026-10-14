/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * An in-memory pair of mux connections implementing `mux/contract.ts`,
 * for the Talk tests until the frame mux lands, and as a unit fixture
 * after: channels with send windows charged at payload length + 16,
 * credit returned as the consumer takes delivery (or reports consumption
 * in manual credit) and batched at half the window, `grant`, CLOSE with a
 * status confirmed by the peer, `openAfter` keeping the child's OPEN in
 * its parent's frame order, bulk-priority sends cut into chunks, the
 * incoming filters and the GOAWAY admission rule. Frames cross between
 * the two connections in order, one microtask each.
 *
 * Not modelled: the output scheduler's tiers, LIMITS (except a manual
 * `awaitingLimits` switch), keep-alive, identity, the receive high-water
 * mark. With `ordered: false` the connections report independent
 * channels, and a test can hold back the frames of chosen channels to
 * reorder them.
 */

import type { PeerIdentity } from '../../../src/core/identity/contract.ts';
import {
  type MuxChannel,
  type MuxChannelState,
  type MuxConnection,
  type MuxOptions,
  type MuxPriority,
  muxOptions,
} from '../../../src/core/mux/contract.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { TimerHandle } from '../../../src/core/util/clock.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';

/** Flow-control cost of a subframe beyond its payload. */
export const frameOverhead = 16;

/** The flow-control cost of a subframe of [length] bytes. */
export function costOf(length: number): number {
  return length + frameOverhead;
}

type Wire =
  | { readonly type: 'open'; readonly id: number; readonly payload: Uint8Array }
  | { readonly type: 'data'; readonly id: number; readonly bytes: Uint8Array }
  | { readonly type: 'close'; readonly id: number; readonly status: Status }
  | { readonly type: 'credit'; readonly id: number; readonly bytes: number }
  | { readonly type: 'goAway'; readonly status: Status }
  | { readonly type: 'end' };

/** What a channel has queued for the wire, in order. */
type Queued =
  | {
      readonly type: 'data';
      readonly bytes: Uint8Array;
      readonly done: Deferred<void> | undefined;
    }
  | { readonly type: 'open'; readonly child: FakeMuxChannel }
  | { readonly type: 'close'; readonly status: Status };

const failedPrecondition = (reason: string): SwitchboardError =>
  SwitchboardError.of(StatusCode.failedPrecondition, reason);

/** Options of {@link fakeMuxPair}. */
export interface FakeMuxOptions {
  /** Mux options of both connections (the clock, `initialWindow`, `closeConfirmTimeout`). */
  readonly mux?: Partial<MuxOptions>;
  /** Whether channels are ordered with each other (the frame mux); false: independent streams. Default true. */
  readonly ordered?: boolean;
  /** Channels report `awaitingLimits` until {@link FakeMuxConnection.announceLimits} on the peer. */
  readonly awaitLimits?: boolean;
}

/** A client (initiator) and a server connection joined in memory. */
export function fakeMuxPair(options: FakeMuxOptions = {}): [FakeMuxConnection, FakeMuxConnection] {
  const ordered = options.ordered ?? true;
  const opts = muxOptions({ keepAliveInterval: undefined, ...options.mux });
  const a = new FakeMuxConnection(true, opts, ordered, options.awaitLimits === true);
  const b = new FakeMuxConnection(false, opts, ordered, options.awaitLimits === true);
  a._peer = b;
  b._peer = a;
  return [a, b];
}

/** One side of a {@link fakeMuxPair}. */
export class FakeMuxConnection implements MuxConnection {
  readonly isInitiator: boolean;
  readonly options: MuxOptions;
  readonly hasOrderedChannels: boolean;
  readonly remote: string;
  _peer!: FakeMuxConnection;

  readonly #channels = new Map<number, FakeMuxChannel>();
  #nextId: number;
  readonly #filters: ((channel: MuxChannel) => boolean)[] = [];
  readonly #admitters: ((openPayload: Uint8Array) => boolean)[] = [];
  readonly #incoming: FakeMuxChannel[] = [];
  #incomingWaiter: Deferred<IteratorResult<MuxChannel>> | undefined;
  #incomingCancelled = false;
  #open = true;
  #goAwaySent = false;
  #peerGoingAway = false;
  readonly #done = new Deferred<Status>();
  readonly #peerGoAway = new Deferred<Status>();
  #awaitingLimits: boolean;
  readonly #limitsKnown = new Deferred<void>();

  /** Frames arriving for these channel ids are held until {@link release}. */
  readonly holding = new Set<number>();
  readonly #held: Wire[] = [];

  /** Every frame this side received, for inspection. */
  readonly received: Wire[] = [];

  localIdentity: string | undefined;

  constructor(isInitiator: boolean, options: MuxOptions, ordered: boolean, awaitLimits: boolean) {
    this.isInitiator = isInitiator;
    this.options = options;
    this.hasOrderedChannels = ordered;
    this.remote = isInitiator ? 'fake server' : 'fake client';
    this.#nextId = isInitiator ? 2 : 1;
    this.#awaitingLimits = awaitLimits;
    if (!awaitLimits) {
      this.#limitsKnown.resolve();
    }
  }

  // -- the contract ------------------------------------------------------

  get incoming(): AsyncIterable<MuxChannel> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: () => {
          const channel = this.#incoming.shift();
          if (channel !== undefined) {
            return Promise.resolve({ value: channel, done: false });
          }
          if (!this.#open || this.#incomingCancelled) {
            return Promise.resolve({ value: undefined, done: true });
          }
          this.#incomingWaiter = new Deferred();
          return this.#incomingWaiter.promise;
        },
        return: () => {
          this.#incomingCancelled = true;
          for (const channel of this.#incoming.splice(0)) {
            void channel.close(Status.of(StatusCode.unavailable, 'not accepting channels'));
          }
          return Promise.resolve({ value: undefined, done: true });
        },
      }),
    };
  }

  /** The next channel the peer opened that no filter adopted. */
  async accept(): Promise<FakeMuxChannel> {
    const result = await this.incoming[Symbol.asyncIterator]().next();
    if (result.done === true) {
      throw new Error('no more incoming channels');
    }
    return result.value as FakeMuxChannel;
  }

  adoptIncoming(filter: (channel: MuxChannel) => boolean): () => void {
    this.#filters.push(filter);
    return () => {
      const index = this.#filters.indexOf(filter);
      if (index >= 0) {
        this.#filters.splice(index, 1);
      }
    };
  }

  admitAfterGoAway(predicate: (openPayload: Uint8Array) => boolean): () => void {
    this.#admitters.push(predicate);
    return () => {
      const index = this.#admitters.indexOf(predicate);
      if (index >= 0) {
        this.#admitters.splice(index, 1);
      }
    };
  }

  open(openPayload: Uint8Array): FakeMuxChannel {
    if (!this.#open || this.#peerGoingAway) {
      throw failedPrecondition('connection closed or going away');
    }
    const channel = this._create(openPayload);
    this._send({ type: 'open', id: channel.id, payload: channel.openPayload });
    channel._openWritten = true;
    return channel;
  }

  /** A locally opened channel, its OPEN not sent yet. */
  _create(openPayload: Uint8Array): FakeMuxChannel {
    const id = this.#nextId;
    this.#nextId += 2;
    const channel = new FakeMuxChannel(this, id, true, openPayload.slice(), this.#awaitingLimits);
    this.#channels.set(id, channel);
    return channel;
  }

  channelWithId(id: number): MuxChannel | undefined {
    return this.#channels.get(id);
  }

  get channels(): Iterable<MuxChannel> {
    return [...this.#channels.values()];
  }

  get openChannelCount(): number {
    let count = 0;
    for (const channel of this.#channels.values()) {
      if (channel.state !== 'closed') {
        count++;
      }
    }
    return count;
  }

  readonly peerMaxChannels = 0;

  async ping(): Promise<number> {
    return 0;
  }

  async goAway(status: Status = Status.ok): Promise<void> {
    if (!this.#open) {
      return;
    }
    this.#goAwaySent = true;
    this._send({ type: 'goAway', status });
    await this.close();
  }

  async close(): Promise<void> {
    if (!this.#open) {
      return;
    }
    this._send({ type: 'end' });
    this._lose(Status.of(StatusCode.connectionLost, 'connection closed'));
  }

  get done(): Promise<Status> {
    return this.#done.promise;
  }

  get isOpen(): boolean {
    return this.#open;
  }

  get peerGoingAway(): boolean {
    return this.#peerGoingAway;
  }

  get peerGoAwayStatus(): Promise<Status> {
    return this.#peerGoAway.promise;
  }

  identify(): Promise<void> {
    return Promise.reject(SwitchboardError.of(StatusCode.unimplemented, 'fake mux'));
  }

  readonly peerIdentity: PeerIdentity | undefined = undefined;
  readonly peerIdentified: Promise<PeerIdentity> = new Promise(() => {});
  readonly identityRequested: Promise<void> = new Promise(() => {});
  readonly identityChanged: Promise<void> = new Promise(() => {});

  get bufferedBytes(): number {
    let total = 0;
    for (const channel of this.#channels.values()) {
      total += channel.bufferedBytes;
    }
    return total;
  }

  readonly sentBytes = 0;
  readonly receivedBytes = 0;
  readonly openPayloadBytes = 0;
  readonly isReceivePaused = false;
  readonly unconfirmedCloseCount = 0;
  readonly heldOutputBytes = 0;

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  // -- test controls -------------------------------------------------------

  /** The peer's LIMITS arrive: channels stop awaiting them. Call on the side whose channels wait. */
  announceLimits(): void {
    this.#awaitingLimits = false;
    for (const channel of this.#channels.values()) {
      channel._limitsArrived();
    }
    this.#limitsKnown.resolve();
  }

  get _limitsKnown(): Promise<void> {
    return this.#limitsKnown.promise;
  }

  /** Delivers the frames held for the channel ids in {@link holding} (all of them, or those of [id]), and stops holding them. */
  release(id?: number): void {
    const frames = this.#held.splice(0);
    for (const frame of frames) {
      const frameId = 'id' in frame ? frame.id : 0;
      if (id === undefined || frameId === id) {
        this.holding.delete(frameId);
        this.#receive(frame);
      } else {
        this.#held.push(frame);
      }
    }
    if (id === undefined) {
      this.holding.clear();
    }
  }

  // -- the wire --------------------------------------------------------------

  /** Sends [frame] to the peer; it arrives in a microtask, in order. */
  _send(frame: Wire): void {
    if (!this.#open) {
      return;
    }
    const peer = this._peer;
    queueMicrotask(() => peer._arrive(frame));
  }

  _arrive(frame: Wire): void {
    if (!this.#open) {
      return;
    }
    if ('id' in frame && this.holding.has(frame.id)) {
      this.#held.push(frame);
      return;
    }
    this.#receive(frame);
  }

  #receive(frame: Wire): void {
    if (!this.#open) {
      return;
    }
    this.received.push(frame);
    switch (frame.type) {
      case 'open':
        this.#onOpen(frame.id, frame.payload);
        break;
      case 'data':
        this.#channels.get(frame.id)?._onData(frame.bytes);
        break;
      case 'close':
        this.#channels.get(frame.id)?._onClose(frame.status);
        break;
      case 'credit':
        this.#channels.get(frame.id)?._onCredit(frame.bytes);
        break;
      case 'goAway':
        this.#peerGoingAway = true;
        this.#peerGoAway.resolve(frame.status);
        break;
      case 'end':
        this._lose(Status.of(StatusCode.connectionLost, 'transport closed'));
        break;
    }
  }

  #onOpen(id: number, payload: Uint8Array): void {
    const channel = new FakeMuxChannel(this, id, false, payload, false);
    channel._openWritten = true;
    this.#channels.set(id, channel);
    if (this.#goAwaySent && !this.#admitters.some((admit) => admit(payload))) {
      void channel.close(Status.of(StatusCode.goingAway, 'going away'));
      return;
    }
    for (const filter of [...this.#filters]) {
      if (filter(channel)) {
        return;
      }
    }
    if (this.#incomingCancelled) {
      void channel.close(Status.of(StatusCode.unavailable, 'not accepting channels'));
      return;
    }
    const waiter = this.#incomingWaiter;
    if (waiter !== undefined) {
      this.#incomingWaiter = undefined;
      waiter.resolve({ value: channel, done: false });
      return;
    }
    this.#incoming.push(channel);
  }

  /** A channel is mutually closed: its id is free. */
  _released(channel: FakeMuxChannel): void {
    if (this.#channels.get(channel.id) === channel) {
      this.#channels.delete(channel.id);
    }
  }

  /** The connection ends: every channel ends with [status]. */
  _lose(status: Status): void {
    if (!this.#open) {
      return;
    }
    this.#open = false;
    for (const channel of [...this.#channels.values()]) {
      channel._lost(status);
    }
    this.#channels.clear();
    for (const channel of this.#incoming.splice(0)) {
      channel._lost(status);
    }
    const waiter = this.#incomingWaiter;
    this.#incomingWaiter = undefined;
    waiter?.resolve({ value: undefined, done: true });
    this.#done.resolve(this.#peerGoingAway ? Status.of(StatusCode.goingAway) : status);
  }
}

/** One channel of a {@link FakeMuxConnection}. */
export class FakeMuxChannel implements MuxChannel {
  readonly connection: FakeMuxConnection;
  readonly id: number;
  readonly isLocallyOpened: boolean;
  readonly openPayload: Uint8Array;
  priority: MuxPriority = 'normal';
  manualCredit = false;

  /** The OPEN went to the peer: DATA and CLOSE may follow. */
  _openWritten = false;

  #state: MuxChannelState = 'open';
  #sendWindow: number;
  #receiveWindow: number;
  #creditWindow: number;
  #unreturned = 0;
  #held = 0;
  #buffered: Uint8Array[] = [];
  #bufferedBytes = 0;
  #queue: Queued[] = [];
  #closeQueued = false;
  #peerClosed = false;
  #discarding = false;
  #readerWaiting: Deferred<void> | undefined;
  #readableEnded = false;
  #sentStatus: Status | undefined;
  #receivedStatus: Status | undefined;
  readonly #done = new Deferred<Status>();
  #closePromise: Promise<void> | undefined;
  #confirmTimer: TimerHandle | undefined;
  #writableWaiters: Deferred<void>[] = [];
  #awaitingLimits: boolean;
  #locked = false;
  readonly #readable: ReadableStream<Uint8Array>;
  readonly #writable: WritableStream<Uint8Array>;

  /** Every status this side closed the channel with. */
  readonly closedWith: Status[] = [];

  constructor(
    connection: FakeMuxConnection,
    id: number,
    locallyOpened: boolean,
    openPayload: Uint8Array,
    awaitingLimits: boolean,
  ) {
    this.connection = connection;
    this.id = id;
    this.isLocallyOpened = locallyOpened;
    this.openPayload = openPayload;
    const window = connection.options.initialWindow;
    this.#sendWindow = window;
    this.#receiveWindow = window;
    this.#creditWindow = window;
    this.#awaitingLimits = awaitingLimits;
    this.#readable = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => this.#pull(controller),
        cancel: () => {
          this.#discarding = true;
          this.#discard();
        },
      },
      { highWaterMark: 0 },
    );
    this.#writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.send(chunk),
      close: () => this.close(),
      abort: (reason: unknown) =>
        this.close(
          reason instanceof SwitchboardError ? reason.status : Status.of(StatusCode.cancelled),
        ),
    });
  }

  get state(): MuxChannelState {
    return this.#state;
  }

  get canSend(): boolean {
    return this.#state === 'open';
  }

  get readable(): ReadableStream<Uint8Array> {
    this.#locked = true;
    return this.#readable;
  }

  get writable(): WritableStream<Uint8Array> {
    return this.#writable;
  }

  get hasListener(): boolean {
    return this.#locked;
  }

  get awaitingLimits(): boolean {
    return this.#awaitingLimits;
  }

  get limitsKnown(): Promise<void> {
    return this.connection._limitsKnown;
  }

  _limitsArrived(): void {
    this.#awaitingLimits = false;
    this.#pump();
  }

  // -- sending -----------------------------------------------------------------

  sendCost(length: number): number {
    return costOf(length);
  }

  get maxSubframeLength(): number {
    return Math.floor(this.connection.options.initialWindow / 2) - frameOverhead;
  }

  get sendWindow(): number {
    return this.#sendWindow;
  }

  get isWritable(): boolean {
    return (
      this.canSend &&
      !this.#awaitingLimits &&
      this.#queue.length === 0 &&
      this.#sendWindow >= costOf(this.maxSubframeLength)
    );
  }

  get whenWritable(): Promise<void> {
    if (this.isWritable) {
      return Promise.resolve();
    }
    if (!this.canSend) {
      const rejected = new Deferred<void>();
      rejected.reject(failedPrecondition(`channel ${this.id} cannot send`));
      return rejected.promise;
    }
    const waiter = new Deferred<void>();
    this.#writableWaiters.push(waiter);
    return waiter.promise;
  }

  send(subframe: Uint8Array): Promise<void> {
    if (!this.canSend) {
      throw failedPrecondition(`channel ${this.id} is ${this.#state}`);
    }
    const max = this.maxSubframeLength;
    const pieces: Uint8Array[] = [];
    if (this.priority === 'bulk' && subframe.length > max) {
      for (let offset = 0; offset < subframe.length; offset += max) {
        pieces.push(subframe.slice(offset, Math.min(offset + max, subframe.length)));
      }
    } else {
      if (subframe.length > max) {
        throw SwitchboardError.of(
          StatusCode.frameTooLarge,
          `subframe of ${subframe.length} bytes, more than ${max}`,
        );
      }
      pieces.push(subframe.slice());
    }
    const done = new Deferred<void>();
    pieces.forEach((bytes, index) => {
      this.#queue.push({
        type: 'data',
        bytes,
        done: index === pieces.length - 1 ? done : undefined,
      });
    });
    this.#pump();
    return done.promise;
  }

  /** Hands what the window takes to the wire, in order. */
  #pump(): void {
    if (!this._openWritten) {
      return;
    }
    while (this.#queue.length > 0) {
      const head = this.#queue[0] as Queued;
      if (head.type === 'data') {
        if (this.#awaitingLimits) {
          break;
        }
        const cost = costOf(head.bytes.length);
        if (cost > this.#sendWindow) {
          break;
        }
        this.#queue.shift();
        this.#sendWindow -= cost;
        this.connection._send({ type: 'data', id: this.id, bytes: head.bytes });
        head.done?.resolve();
      } else if (head.type === 'open') {
        this.#queue.shift();
        const child = head.child;
        this.connection._send({ type: 'open', id: child.id, payload: child.openPayload });
        child._openWritten = true;
        child.#pump();
      } else {
        this.#queue.shift();
        this.connection._send({ type: 'close', id: this.id, status: head.status });
      }
    }
    this.#checkWritable();
  }

  #checkWritable(): void {
    if (this.#writableWaiters.length === 0) {
      return;
    }
    if (this.isWritable) {
      for (const waiter of this.#writableWaiters.splice(0)) {
        waiter.resolve();
      }
    } else if (!this.canSend) {
      for (const waiter of this.#writableWaiters.splice(0)) {
        waiter.reject(failedPrecondition(`channel ${this.id} cannot send`));
      }
    }
  }

  _onCredit(bytes: number): void {
    if (this.#state !== 'open') {
      return;
    }
    this.#sendWindow += bytes;
    this.#pump();
  }

  openAfter(openPayload: Uint8Array): MuxChannel {
    if (!this.canSend) {
      throw failedPrecondition(`channel ${this.id} is ${this.#state}`);
    }
    const child = this.connection._create(openPayload);
    this.#queue.push({ type: 'open', child });
    this.#pump();
    return child;
  }

  /** Drops what waits for the window: the channel stops sending. */
  #dropQueue(): void {
    const queue = this.#queue;
    this.#queue = [];
    for (const queued of queue) {
      if (queued.type === 'data') {
        queued.done?.reject(failedPrecondition(`channel ${this.id} stopped sending`));
      } else if (queued.type === 'open') {
        queued.child._neverOpened();
      }
    }
    this.#checkWritable();
  }

  /** The OPEN of this child channel will never be written. */
  _neverOpened(): void {
    this.#end(Status.of(StatusCode.cancelled, 'parent stopped before the open'));
  }

  // -- receiving ---------------------------------------------------------------

  get receiveWindow(): number {
    return this.#receiveWindow;
  }

  get bufferedBytes(): number {
    return this.#bufferedBytes;
  }

  get heldBytes(): number {
    return this.#held;
  }

  _onData(bytes: Uint8Array): void {
    if (this.#state === 'closed' || this.#peerClosed) {
      return;
    }
    const cost = costOf(bytes.length);
    this.#receiveWindow -= cost;
    if (this.#receiveWindow < 0) {
      throw new Error(`fake mux: DATA beyond the window of channel ${this.id}`);
    }
    if (this.#discarding) {
      this.#consume(cost);
      return;
    }
    this.#buffered.push(bytes);
    this.#bufferedBytes += cost;
    this.#wakeReader();
  }

  #wakeReader(): void {
    const waiter = this.#readerWaiting;
    this.#readerWaiting = undefined;
    waiter?.resolve();
  }

  async #pull(controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> {
    for (;;) {
      const chunk = this.#buffered.shift();
      if (chunk !== undefined) {
        const cost = costOf(chunk.length);
        this.#bufferedBytes -= cost;
        if (this.manualCredit) {
          this.#held += cost;
        } else {
          this.#consume(cost);
        }
        controller.enqueue(chunk);
        return;
      }
      if (this.#readableEnded) {
        controller.close();
        return;
      }
      const waiter = new Deferred<void>();
      this.#readerWaiting = waiter;
      await waiter.promise;
    }
  }

  /** The reader stopped: what is buffered and what arrives counts as consumed. */
  #discard(): void {
    let cost = 0;
    for (const chunk of this.#buffered.splice(0)) {
      cost += costOf(chunk.length);
    }
    this.#bufferedBytes = 0;
    if (cost > 0) {
      this.#consume(cost);
    }
  }

  consumed(bytes: number, subframes = 1): void {
    if (bytes < 0 || subframes < 0) {
      throw new RangeError('negative consumption');
    }
    if (this.#state === 'closed' && this.#held === 0) {
      return;
    }
    const cost = bytes + frameOverhead * subframes;
    if (cost > this.#held) {
      throw new Error(`consumed ${cost} beyond the ${this.#held} held on channel ${this.id}`);
    }
    this.#held -= cost;
    this.#consume(cost);
  }

  /** Counts [cost] consumed, returning credit at half the window. */
  #consume(cost: number): void {
    this.#unreturned += cost;
    if (!this.#canCredit) {
      return;
    }
    if (this.#unreturned >= Math.max(1, Math.floor(this.#creditWindow / 2))) {
      this.#sendCredit(this.#unreturned);
    }
  }

  get #canCredit(): boolean {
    return this.#state === 'open' && !this.#peerClosed;
  }

  #sendCredit(bytes: number): void {
    this.#unreturned = 0;
    this.#receiveWindow += bytes;
    this.connection._send({ type: 'credit', id: this.id, bytes });
  }

  grant(bytes: number): void {
    if (!(bytes > 0)) {
      throw new RangeError(`grant of ${bytes} bytes`);
    }
    if (!this.#canCredit) {
      return;
    }
    if (this.#receiveWindow + bytes > 0xffffffff) {
      throw new RangeError('window beyond 2^32 - 1');
    }
    this.#creditWindow += bytes;
    this.#sendCredit(bytes + this.#unreturned);
  }

  // -- closing -----------------------------------------------------------------

  get done(): Promise<Status> {
    return this.#done.promise;
  }

  close(status: Status = Status.ok): Promise<void> {
    if (this.#closePromise !== undefined) {
      return this.#closePromise;
    }
    this.closedWith.push(status);
    if (this.#state !== 'open') {
      this.#closePromise = this.#done.promise.then(() => {});
      return this.#closePromise;
    }
    this.#state = 'halfClosedLocal';
    this.#closeQueued = true;
    this.#sentStatus = status;
    this.#queue.push({ type: 'close', status });
    this.#pump();
    this.#checkWritable();
    const timeout = this.connection.options.closeConfirmTimeout;
    if (timeout > 0) {
      this.#confirmTimer = this.connection.options.clock.setTimeout(() => {
        this.#confirmTimer = undefined;
        if (this.#state === 'closed') {
          return;
        }
        const waiting = this.#queue.some((queued) => queued.type === 'close');
        this.#dropQueue();
        if (waiting) {
          const end = status.isOk ? Status.of(StatusCode.deadlineExceeded) : status;
          this.#sentStatus = end;
          this.connection._send({ type: 'close', id: this.id, status: end });
        }
        this.#end(this.#endStatus());
      }, timeout);
    }
    this.#closePromise = this.#done.promise.then(() => {});
    return this.#closePromise;
  }

  _onClose(status: Status): void {
    if (this.#state === 'closed' || this.#peerClosed) {
      return;
    }
    this.#peerClosed = true;
    this.#receivedStatus = status;
    if (!this.#closeQueued) {
      // Confirm, after dropping what still waited to go.
      this.#dropQueue();
      this.#closeQueued = true;
      this.connection._send({ type: 'close', id: this.id, status: Status.ok });
    }
    this.#end(this.#endStatus());
  }

  #endStatus(): Status {
    const sent = this.#sentStatus;
    if (sent !== undefined && !sent.isOk) {
      return sent;
    }
    const received = this.#receivedStatus;
    if (received !== undefined && !received.isOk) {
      return received;
    }
    return Status.ok;
  }

  /** The connection ended. */
  _lost(status: Status): void {
    this.#end(this.#done.isSettled ? Status.ok : status);
  }

  #end(status: Status): void {
    if (this.#state === 'closed') {
      return;
    }
    this.#state = 'closed';
    if (this.#confirmTimer !== undefined) {
      this.connection.options.clock.clearTimeout(this.#confirmTimer);
      this.#confirmTimer = undefined;
    }
    this.#dropQueue();
    this.#readableEnded = true;
    this.#wakeReader();
    this.connection._released(this);
    this.#done.resolve(status);
    this.#checkWritable();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  toString(): string {
    return `FakeMuxChannel(${this.id})`;
  }
}

/** Opens a channel from [client] and takes it at [server]: both ends. */
export async function channelPair(
  client: FakeMuxConnection,
  server: FakeMuxConnection,
  openPayload: Uint8Array = new Uint8Array(0),
): Promise<[FakeMuxChannel, FakeMuxChannel]> {
  const local = client.open(openPayload);
  const remote = await server.accept();
  return [local, remote];
}
