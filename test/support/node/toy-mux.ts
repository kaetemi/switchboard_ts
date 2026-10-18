/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ByteReader, ByteWriter, utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import type { Credential, PeerIdentity } from '../../../src/core/identity/contract.ts';
import {
  type IdentifyOptions,
  type MuxChannel,
  type MuxChannelState,
  type MuxConnection,
  type MuxOptions,
  type MuxPriority,
  muxOptions,
} from '../../../src/core/mux/contract.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import { AsyncQueue } from '../../../src/core/util/async-queue.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { peerIdentityOf } from './seeded-identity.ts';

/**
 * A toy implementation of the connection contract (`mux/contract.ts`) over
 * a {@link FrameTransport}, with a private wire format, for the node
 * thread's tests until the protocol core's frame mux lands. It keeps the
 * behaviours the node depends on: opens refused after either GOAWAY (bar
 * continuations), the peer's channel limit, mutual CLOSE with the first
 * non-OK status as `done`, GOAWAY grace and idle close, the end statuses,
 * `incoming` with filters and cancellation, PING, and identification
 * through the options' verifier (receiver binding and `requireNamedIdent`
 * included; no nonces and no proofs). No flow control: windows are
 * unbounded.
 */

const Kind = {
  open: 1,
  data: 2,
  close: 3,
  goAway: 4,
  ping: 5,
  pong: 6,
  ident: 7,
  identAck: 8,
  limits: 9,
} as const;
type Kind = (typeof Kind)[keyof typeof Kind];

/** A toy connection over [transport]. */
export function toyMux(
  transport: FrameTransport,
  options: Partial<MuxOptions>,
  isInitiator: boolean,
): MuxConnection {
  return new ToyMuxConnection(transport, muxOptions(options), isInitiator);
}

export class ToyMuxConnection implements MuxConnection {
  readonly isInitiator: boolean;
  readonly options: MuxOptions;
  readonly remote: string;
  readonly hasOrderedChannels = true;
  localIdentity: string | undefined;

  readonly #transport: FrameTransport;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;
  readonly #incoming = new AsyncQueue<MuxChannel>();
  #incomingCancelled = false;
  readonly #filters: ((channel: MuxChannel) => boolean)[] = [];
  readonly #admit: ((payload: Uint8Array) => boolean)[] = [];
  readonly #channels = new Map<number, ToyChannel>();
  #nextId: number;
  #closing = false;
  #goAwaySent = false;
  #ownGoAway: Status | undefined;
  #peerGoAway: Status | undefined;
  readonly #done = new Deferred<Status>();
  readonly #peerGoAwayStatus = new Deferred<Status>();
  #peerMaxChannels = 0;
  readonly #pings = new Map<number, { deferred: Deferred<number>; start: number }>();
  #pingSeq = 0;
  readonly #identWaits: Deferred<void>[] = [];
  #peerIdentity: PeerIdentity | undefined;
  readonly #peerIdentified = new Deferred<PeerIdentity>();
  #identityChanged = new Deferred<void>();
  readonly #identityRequested = new Deferred<void>();
  #idle: Deferred<void> | undefined;
  #closeFuture: Promise<void> | undefined;
  sentBytes = 0;
  receivedBytes = 0;

  constructor(transport: FrameTransport, options: MuxOptions, isInitiator: boolean) {
    this.#transport = transport;
    this.options = options;
    this.isInitiator = isInitiator;
    this.remote = transport.remote ?? 'toy';
    this.#nextId = isInitiator ? 1 : 2;
    this.#writer = transport.writable.getWriter();
    this.#writer.closed.catch(() => {});
    this.#incoming.onCancel = () => {
      this.#incomingCancelled = true;
      for (const channel of this.#incoming.drain()) {
        void channel.close(Status.of(StatusCode.unavailable, 'not accepting channels'));
      }
    };
    if (options.announceLimits && options.maxChannels > 0) {
      const w = new ByteWriter(4);
      w.u32(options.maxChannels);
      this.#send(Kind.limits, 0, w.toBytes());
    }
    void this.#read();
  }

  // Contract ------------------------------------------------------------

  get incoming(): AsyncIterable<MuxChannel> {
    return this.#incoming;
  }

  adoptIncoming(filter: (channel: MuxChannel) => boolean): () => void {
    this.#filters.push(filter);
    return () => {
      const i = this.#filters.indexOf(filter);
      if (i >= 0) this.#filters.splice(i, 1);
    };
  }

  admitAfterGoAway(predicate: (openPayload: Uint8Array) => boolean): () => void {
    this.#admit.push(predicate);
    return () => {
      const i = this.#admit.indexOf(predicate);
      if (i >= 0) this.#admit.splice(i, 1);
    };
  }

  open(openPayload: Uint8Array): MuxChannel {
    return this.#open(openPayload, false);
  }

  #open(openPayload: Uint8Array, continuation: boolean): MuxChannel {
    if (this.#closing || (this.#goAwaySent && !continuation)) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closing');
    }
    if (this.#peerGoAway !== undefined && !continuation) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'peer is going away');
    }
    if (this.#peerMaxChannels > 0 && this.#channels.size >= this.#peerMaxChannels) {
      throw SwitchboardError.of(
        StatusCode.resourceExhausted,
        `peer accepts at most ${this.#peerMaxChannels} channels`,
      );
    }
    const id = this.#nextId;
    this.#nextId += 2;
    const channel = new ToyChannel(this, id, true, openPayload.slice());
    this.#channels.set(id, channel);
    this.#send(Kind.open, id, openPayload);
    return channel;
  }

  openContinuation(openPayload: Uint8Array): MuxChannel {
    return this.#open(openPayload, true);
  }

  channelWithId(id: number): MuxChannel | undefined {
    return this.#channels.get(id);
  }

  get channels(): Iterable<MuxChannel> {
    return [...this.#channels.values()];
  }

  get openChannelCount(): number {
    return this.#channels.size;
  }

  get peerMaxChannels(): number {
    return this.#peerMaxChannels;
  }

  ping(payload?: Uint8Array, options: { signal?: AbortSignal } = {}): Promise<number> {
    if (this.#closing) {
      return Promise.reject(SwitchboardError.of(StatusCode.failedPrecondition, 'closed'));
    }
    void payload;
    const seq = ++this.#pingSeq;
    const deferred = new Deferred<number>();
    this.#pings.set(seq, { deferred, start: this.options.clock.now() });
    const w = new ByteWriter(4);
    w.u32(seq);
    this.#send(Kind.ping, 0, w.toBytes());
    const signal = options.signal;
    if (signal !== undefined) {
      const onAbort = (): void => {
        this.#pings.delete(seq);
        deferred.reject(signal.reason);
      };
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    }
    return deferred.promise;
  }

  goAway(status: Status = Status.of(StatusCode.goingAway)): Promise<void> {
    if (this.#closing) {
      return this.close();
    }
    if (!this.#goAwaySent) {
      this.#goAwaySent = true;
      this.#ownGoAway = status;
      this.#send(Kind.goAway, 0, status.encode());
    }
    return this.#goAwayWait();
  }

  async #goAwayWait(): Promise<void> {
    if (this.#channels.size > 0 && !this.#closing) {
      this.#idle ??= new Deferred<void>();
      const idle = this.#idle;
      const clock = this.options.clock;
      const grace = clock.setTimeout(() => idle.resolve(), this.options.goAwayGrace);
      await idle.promise;
      clock.clearTimeout(grace);
    }
    await this.close();
  }

  close(): Promise<void> {
    return this.#shutdown(this.#lostStatus(), undefined);
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  get done(): Promise<Status> {
    return this.#done.promise;
  }

  get isOpen(): boolean {
    return !this.#closing;
  }

  get peerGoingAway(): boolean {
    return this.#peerGoAway !== undefined;
  }

  get peerGoAwayStatus(): Promise<Status> {
    return this.#peerGoAwayStatus.promise;
  }

  identify(credential: Credential, options: IdentifyOptions = {}): Promise<void> {
    if (this.#closing) {
      return Promise.reject(SwitchboardError.of(StatusCode.failedPrecondition, 'closed'));
    }
    const intent = options.intent ?? new Uint8Array(0);
    const receiver = utf8Encode(options.receiver ?? '');
    const bytes = credential.encode();
    const w = new ByteWriter(4 + bytes.length + intent.length + receiver.length);
    w.u16(bytes.length);
    w.bytes(bytes);
    w.u8(intent.length);
    w.bytes(intent);
    w.u8(receiver.length);
    w.bytes(receiver);
    const wait = new Deferred<void>();
    this.#identWaits.push(wait);
    this.#send(Kind.ident, 0, w.toBytes());
    const clock = this.options.clock;
    const timeout = options.timeout ?? this.options.identityTimeout;
    const timer =
      timeout > 0
        ? clock.setTimeout(
            () => wait.reject(SwitchboardError.of(StatusCode.deadlineExceeded, 'IDENT timed out')),
            timeout,
          )
        : undefined;
    const signal = options.signal;
    const onAbort = (): void => {
      wait.reject(signal?.reason);
    };
    signal?.addEventListener('abort', onAbort, { once: true });
    return wait.promise.finally(() => {
      if (timer !== undefined) clock.clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    });
  }

  get peerIdentity(): PeerIdentity | undefined {
    const identity = this.#peerIdentity;
    return identity === undefined || identity.isExpired(this.options.wallTime?.())
      ? undefined
      : identity;
  }

  get peerIdentified(): Promise<PeerIdentity> {
    return this.#peerIdentified.promise;
  }

  get identityRequested(): Promise<void> {
    return this.#identityRequested.promise;
  }

  get identityChanged(): Promise<void> {
    return this.#identityChanged.promise;
  }

  get bufferedBytes(): number {
    return 0;
  }
  readonly openPayloadBytes = 0;
  readonly isReceivePaused = false;
  readonly unconfirmedCloseCount = 0;
  readonly heldOutputBytes = 0;

  // Test helpers ----------------------------------------------------------

  /** Whether this side sent GOAWAY. */
  get goAwaySent(): boolean {
    return this.#goAwaySent;
  }

  /** Breaks the transport without any frame (the peer sees CONNECTION_LOST). */
  breakTransport(): void {
    this.#transport.abort?.();
    void this.#shutdown(Status.of(StatusCode.connectionLost, 'broken'), undefined);
  }

  // Internals -------------------------------------------------------------

  #send(kind: Kind, id: number, payload: Uint8Array): void {
    if (this.#closing && kind !== Kind.goAway) return;
    const w = new ByteWriter(7 + payload.length);
    w.u8(kind);
    w.u48(id);
    w.bytes(payload);
    const frame = w.toBytes();
    this.sentBytes += frame.length;
    this.#writer.write(frame).catch(() => {});
  }

  sendData(id: number, payload: Uint8Array): void {
    this.#send(Kind.data, id, payload);
  }

  sendClose(id: number, status: Status): void {
    this.#send(Kind.close, id, status.encode());
  }

  /** [channel] is mutually closed. */
  released(channel: ToyChannel): void {
    if (this.#channels.get(channel.id) !== channel) return;
    this.#channels.delete(channel.id);
    if (this.#channels.size === 0) {
      this.#idle?.resolve();
      if (this.#peerGoAway !== undefined) void this.close();
    }
  }

  async #read(): Promise<void> {
    const reader = this.#transport.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        this.receivedBytes += value.length;
        await this.#handle(value);
        if (this.#closing) return;
      }
    } catch {
      // Transport failure.
    } finally {
      reader.releaseLock();
    }
    void this.#shutdown(this.#lostStatus(), undefined);
  }

  async #handle(frame: Uint8Array): Promise<void> {
    const r = new ByteReader(frame);
    const kind = r.u8('kind') as Kind;
    const id = r.u48('id');
    const payload = r.rest();
    switch (kind) {
      case Kind.open:
        this.#onOpen(id, payload.slice());
        break;
      case Kind.data:
        this.#channels.get(id)?.receive(payload.slice());
        break;
      case Kind.close:
        this.#channels.get(id)?.peerClosed(Status.decode(payload));
        break;
      case Kind.goAway: {
        const status = Status.decode(payload);
        this.#peerGoAway = status;
        this.#peerGoAwayStatus.resolve(status);
        if (this.#channels.size === 0) void this.close();
        break;
      }
      case Kind.ping:
        this.#send(Kind.pong, 0, payload.slice());
        break;
      case Kind.pong: {
        const seq = new ByteReader(payload).u32('seq');
        const ping = this.#pings.get(seq);
        if (ping !== undefined) {
          this.#pings.delete(seq);
          ping.deferred.resolve(this.options.clock.now() - ping.start);
        }
        break;
      }
      case Kind.ident:
        await this.#onIdent(payload);
        break;
      case Kind.identAck:
        this.#identWaits.shift()?.resolve();
        break;
      case Kind.limits:
        this.#peerMaxChannels = new ByteReader(payload).u32('maxChannels');
        break;
    }
  }

  #onOpen(id: number, payload: Uint8Array): void {
    if (this.#goAwaySent && !this.#admit.some((admit) => admit(payload))) {
      this.sendClose(id, Status.of(StatusCode.failedPrecondition, 'going away'));
      return;
    }
    const channel = new ToyChannel(this, id, false, payload);
    this.#channels.set(id, channel);
    for (const filter of [...this.#filters]) {
      if (filter(channel)) return;
    }
    if (this.#incomingCancelled) {
      void channel.close(Status.of(StatusCode.unavailable, 'not accepting channels'));
      return;
    }
    this.#incoming.push(channel);
  }

  async #onIdent(payload: Uint8Array): Promise<void> {
    const r = new ByteReader(payload);
    const credentialBytes = r.take(r.u16('length'));
    const intent = r.take(r.u8('intent')).slice();
    const receiver = utf8Decode(r.take(r.u8('receiver')));
    this.#identityRequested.resolve();
    const verifier = this.options.identityVerifier;
    if (verifier === undefined) {
      this.#send(Kind.identAck, 0, new Uint8Array(0));
      return;
    }
    const refused = (): void =>
      this.#fail(Status.of(StatusCode.unauthenticated, 'identification failed'));
    if (receiver.length > 0 ? receiver !== this.localIdentity : this.options.requireNamedIdent) {
      refused();
      return;
    }
    let credential: Credential;
    try {
      const now = this.options.wallTime?.();
      credential = await verifier.verify(credentialBytes, now === undefined ? undefined : { now });
    } catch {
      refused();
      return;
    }
    if (this.#closing) return;
    const identity = peerIdentityOf(credential, intent, receiver);
    this.#peerIdentity = identity;
    this.#peerIdentified.resolve(identity);
    const changed = this.#identityChanged;
    this.#identityChanged = new Deferred<void>();
    changed.resolve();
    this.#send(Kind.identAck, 0, new Uint8Array(0));
  }

  #fail(status: Status): void {
    if (this.#closing) return;
    this.#send(Kind.goAway, 0, status.encode());
    void this.#shutdown(status, this.#lostStatus());
  }

  /** GOING_AWAY after the peer's GOAWAY, the GOAWAY this side sent, else CONNECTION_LOST. */
  #lostStatus(reason = ''): Status {
    if (this.#peerGoAway !== undefined) {
      return Status.of(StatusCode.goingAway, this.#peerGoAway.reason);
    }
    return this.#ownGoAway ?? Status.of(StatusCode.connectionLost, reason);
  }

  #shutdown(status: Status, channelStatus: Status | undefined): Promise<void> {
    if (this.#closeFuture !== undefined) return this.#closeFuture;
    this.#closing = true;
    this.#done.resolve(status);
    const lost = channelStatus ?? status;
    for (const channel of [...this.#channels.values()]) channel.lost(lost);
    this.#channels.clear();
    this.#idle?.resolve();
    this.#incoming.end();
    const failure = new SwitchboardError(
      this.#peerGoAway?.code === StatusCode.unauthenticated ? this.#peerGoAway : status,
    );
    for (const wait of this.#identWaits.splice(0)) wait.reject(failure);
    for (const ping of this.#pings.values()) ping.deferred.reject(failure);
    this.#pings.clear();
    const identityChanged = this.#identityChanged;
    identityChanged.resolve();
    this.#closeFuture = this.#writer.close().then(
      () => {},
      () => {},
    );
    return this.#closeFuture;
  }
}

/** A channel of a {@link ToyMuxConnection}. */
export class ToyChannel implements MuxChannel {
  readonly #connection: ToyMuxConnection;
  readonly id: number;
  readonly isLocallyOpened: boolean;
  readonly openPayload: Uint8Array;
  priority: MuxPriority = 'normal';
  manualCredit = false;
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly #queue = new AsyncQueue<Uint8Array>();
  #state: MuxChannelState = 'open';
  #closeSent = false;
  #closeReceived = false;
  #sent: Status | undefined;
  #received: Status | undefined;
  readonly #done = new Deferred<Status>();
  #buffered = 0;

  constructor(
    connection: ToyMuxConnection,
    id: number,
    isLocallyOpened: boolean,
    openPayload: Uint8Array,
  ) {
    this.#connection = connection;
    this.id = id;
    this.isLocallyOpened = isLocallyOpened;
    this.openPayload = openPayload;
    const queue = this.#queue;
    this.readable = new ReadableStream<Uint8Array>(
      {
        pull: async (controller) => {
          const next = await queue.next();
          if (next.done) {
            controller.close();
          } else {
            this.#buffered -= next.value.length + 16;
            controller.enqueue(next.value);
          }
        },
        cancel: () => {
          void this.close(Status.of(StatusCode.cancelled, 'reader cancelled'));
        },
      },
      { highWaterMark: 0 },
    );
    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.send(chunk),
      close: () => this.close(),
      abort: (reason) =>
        this.close(reason instanceof Status ? reason : Status.of(StatusCode.cancelled)),
    });
  }

  get connection(): MuxConnection {
    return this.#connection;
  }

  get state(): MuxChannelState {
    return this.#state;
  }

  get canSend(): boolean {
    return this.#state === 'open';
  }

  send(subframe: Uint8Array): Promise<void> {
    if (!this.canSend) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'channel cannot send');
    }
    this.#connection.sendData(this.id, subframe);
    return Promise.resolve();
  }

  sendCost(length: number): number {
    return length + 16;
  }

  get maxSubframeLength(): number {
    return this.#connection.options.maxFrameSize - 16;
  }

  readonly sendWindow = 0xffffffff;

  get isWritable(): boolean {
    return this.canSend;
  }

  get whenWritable(): Promise<void> {
    return this.canSend
      ? Promise.resolve()
      : Promise.reject(SwitchboardError.of(StatusCode.failedPrecondition, 'closed'));
  }

  readonly receiveWindow = 0xffffffff;

  get bufferedBytes(): number {
    return this.#buffered;
  }

  readonly heldBytes = 0;

  consumed(): void {}

  grant(): void {}

  openAfter(openPayload: Uint8Array): MuxChannel {
    return this.#connection.openContinuation(openPayload);
  }

  close(status: Status = Status.ok): Promise<void> {
    if (!this.#closeSent && this.#state !== 'closed') {
      this.#closeSent = true;
      this.#sent = status;
      this.#connection.sendClose(this.id, status);
      if (this.#closeReceived) this.#finish();
      else this.#state = 'halfClosedLocal';
    }
    return this.#done.promise.then(() => {});
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  get done(): Promise<Status> {
    return this.#done.promise;
  }

  get hasListener(): boolean {
    return this.readable.locked || this.#queue.hasConsumer;
  }

  readonly awaitingLimits = false;
  readonly limitsKnown = Promise.resolve();

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return this.readable[Symbol.asyncIterator]();
  }

  receive(payload: Uint8Array): void {
    if (this.#closeReceived) return;
    this.#buffered += payload.length + 16;
    this.#queue.push(payload);
  }

  peerClosed(status: Status): void {
    if (this.#closeReceived) return;
    this.#closeReceived = true;
    this.#received = status;
    this.#queue.end();
    if (this.#closeSent) this.#finish();
    else void this.close(Status.ok);
  }

  lost(status: Status): void {
    if (this.#state === 'closed') return;
    this.#state = 'closed';
    this.#queue.end();
    this.#done.resolve(status);
  }

  #finish(): void {
    this.#state = 'closed';
    const sent = this.#sent;
    const received = this.#received;
    this.#done.resolve(
      sent !== undefined && !sent.isOk
        ? sent
        : received !== undefined && !received.isOk
          ? received
          : Status.ok,
    );
    this.#connection.released(this);
  }

  toString(): string {
    return `ToyChannel(${this.id})`;
  }
}
