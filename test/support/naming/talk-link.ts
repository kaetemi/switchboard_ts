/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// A pair of real `TalkChannel`s for the naming tests, over an in-memory link
// of whole Talk frames (the port of the Dart harness's `TalkChannel` over a
// `StreamChannelController`). What the tests see of the wire is as the fake
// pair (`fake-talk.ts`) shows it: every frame a side sends is recorded, as a
// {@link FakeFrame}, and may be dropped by a filter; a test may also write a
// raw frame. Closing one side with a status ends both, the other side seeing
// that status as its end, as a mux CLOSE carries it. Nothing is flow
// controlled and bulk payloads cannot travel (as on any Talk channel that
// is not a mux channel), and delivery goes through Web Streams, so it is
// never synchronous: the tests that need the fake's synchronous bursts keep
// the fake.

import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { TalkOptions } from '../../../src/core/talk/contract.ts';
import { TalkChannel, type TalkFrameChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import type { FakeFrame, FrameFilter } from './fake-talk.ts';

/** [frame] as the fake pair shows frames to the tests. */
export function fakeFrameOf(frame: TalkFrame): FakeFrame {
  switch (frame.kind) {
    case TalkKind.message:
    case TalkKind.streamItem:
      return {
        kind: 'message',
        procedure: frame.procedure ?? Name.empty,
        payload: frame.payload,
        requestId: frame.requestId,
        responseId: frame.responseId,
        expectsStream: frame.stream,
        item: frame.kind === TalkKind.streamItem,
      };
    case TalkKind.abort:
      return {
        kind: 'abort',
        requestId: frame.requestId,
        responseId: frame.responseId,
        status: frame.status,
      };
    default: {
      // A credit (from the requester) shows as an EXTEND that declares
      // nothing; the naming tests never grant item windows.
      const extension = frame.isCredit
        ? { deadline: undefined, renew: undefined }
        : frame.extension;
      return {
        kind: 'extend',
        responseId: frame.responseId,
        deadline: extension.deadline,
        renew: extension.renew,
      };
    }
  }
}

/** The wire bytes of [frame]; a `close` has none (it closes the link). */
export function encodeFakeFrame(frame: FakeFrame): Uint8Array | undefined {
  switch (frame.kind) {
    case 'message':
      return new TalkFrame({
        kind: frame.item ? TalkKind.streamItem : TalkKind.message,
        procedure: frame.procedure.isEmpty ? undefined : frame.procedure,
        requestId: frame.requestId,
        responseId: frame.responseId,
        stream: frame.expectsStream,
        payload: frame.payload,
      }).encode();
    case 'abort':
      return new TalkFrame({
        kind: TalkKind.abort,
        requestId: frame.requestId,
        responseId: frame.responseId,
        payload: frame.status.encode(),
      }).encode();
    case 'extend':
      return new TalkFrame({
        kind: TalkKind.extend,
        responseId: frame.responseId,
        payload: TalkFrame.extendPayload({ deadline: frame.deadline, renew: frame.renew }),
      }).encode();
    case 'close':
      return undefined;
  }
}

/** One end of the link: whole frames in and out, closable with a status. */
class LinkEnd implements TalkFrameChannel {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  /** Every frame this side sent, in order. */
  readonly sent: FakeFrame[] = [];
  /** Drops a frame this side sends when it returns true. */
  drop: FrameFilter | undefined;
  #controller!: ReadableStreamDefaultController<Uint8Array>;
  #peer: LinkEnd | undefined;
  #ended = false;
  readonly #done = new Deferred<Status>();

  constructor() {
    this.readable = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.#controller = controller;
      },
      cancel: () => {
        this.#ended = true;
      },
    });
    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.#send(chunk, fakeFrameOf(TalkFrame.decode(chunk))),
      close: () => this.close(),
      abort: () => this.close(Status.of(StatusCode.cancelled)),
    });
  }

  connect(peer: LinkEnd): void {
    this.#peer = peer;
  }

  #send(chunk: Uint8Array, frame: FakeFrame): void {
    this.sent.push(frame);
    if (this.drop?.(frame) === true) {
      return;
    }
    const peer = this.#peer;
    if (peer !== undefined) {
      peer.#deliver(chunk);
    }
  }

  #deliver(chunk: Uint8Array): void {
    if (!this.#ended) {
      this.#controller.enqueue(chunk.slice());
    }
  }

  /** Sends [frame] as this side's (unless the filter drops it); a `close` closes the link with its status. */
  inject(frame: FakeFrame): void {
    const bytes = encodeFakeFrame(frame);
    if (bytes === undefined) {
      this.sent.push(frame);
      void this.close(frame.kind === 'close' ? frame.status : Status.ok);
      return;
    }
    this.#send(bytes, frame);
  }

  #end(status: Status): void {
    if (!this.#ended) {
      this.#ended = true;
      try {
        this.#controller.close();
      } catch {
        // Cancelled already.
      }
    }
    this.#done.resolve(status);
  }

  async close(status: Status = Status.ok): Promise<void> {
    if (this.#done.isSettled) {
      return;
    }
    this.#end(status);
    const peer = this.#peer;
    if (peer !== undefined) {
      peer.#end(status);
    }
  }

  get done(): Promise<Status> {
    return this.#done.promise;
  }
}

/** A real Talk channel on one end of a link, with what the tests see of the wire. */
export class LinkedTalkChannel extends TalkChannel {
  readonly #end: LinkEnd;

  constructor(end: LinkEnd, options: Partial<TalkOptions>) {
    super(end, options);
    this.#end = end;
  }

  /** Every frame this side sent, in order (for tests that check the wire). */
  get sent(): FakeFrame[] {
    return this.#end.sent;
  }

  /** Replaces the filter of the frames this side sends. */
  set drop(filter: FrameFilter | undefined) {
    this.#end.drop = filter;
  }

  /** Sends the raw [frame] to the peer (unless the filter drops it), bypassing this channel. */
  write(frame: FakeFrame): void {
    this.#end.inject(frame);
  }
}

/** What {@link talkLink} takes: as `FakeTalkPairOptions`, without `sync`. */
export interface TalkLinkOptions {
  /** Options of the first channel. */
  readonly a?: Partial<TalkOptions>;
  /** Options of the second channel. */
  readonly b?: Partial<TalkOptions>;
  /** Drops a frame from a to b when it returns true. */
  readonly dropAtoB?: FrameFilter;
  /** Drops a frame from b to a when it returns true. */
  readonly dropBtoA?: FrameFilter;
}

/** Two real Talk channels linked in memory. */
export function talkLink(options: TalkLinkOptions = {}): [LinkedTalkChannel, LinkedTalkChannel] {
  const a = new LinkEnd();
  const b = new LinkEnd();
  a.connect(b);
  b.connect(a);
  a.drop = options.dropAtoB;
  b.drop = options.dropBtoA;
  return [new LinkedTalkChannel(a, options.a ?? {}), new LinkedTalkChannel(b, options.b ?? {})];
}
