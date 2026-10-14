/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The frame mux's output scheduler (wiki page "Polyverse Switchboard Mux",
 * section "Output scheduling"): local policy, nothing of it on the wire.
 * Ports the output scheduling of `mux_connection.dart`.
 *
 * DATA waits here, in one queue per channel id, while the transport is not
 * ready (`FrameTransport.isOutputReady`); a CLOSE or an `openAfter` OPEN
 * waits behind the DATA queued before it, so the order within a channel
 * is kept. Control messages and the other frames go straight to the
 * transport. Queues take turns: the ordinary ones first, one frame each,
 * then the bulk ones, with a bulk turn after `MuxOptions.bulkZipper`
 * ordinary frames while bulk frames wait. Queues are keyed by id and an id
 * is reused only once its CLOSE went out, so a new OPEN never overtakes an
 * old CLOSE.
 */

import { Fifo } from '../transport/fifo.ts';
import type { MuxChannelLink } from './frame-mux-channel.ts';
import { MuxCommand, MuxCredit, MuxFrame } from './mux-frame.ts';

/** An encoded frame waiting for the transport. */
interface OutFrame {
  readonly bytes: Uint8Array;
  readonly isData: boolean;
  /** For an OPEN of `openAfter`, the channel it opens. */
  readonly opens: MuxChannelLink | undefined;
}

/** The frames of one channel id waiting for the transport; in a turn list while it has frames and is not being written from. */
class OutQueue {
  readonly id: number;
  readonly frames = new Fifo<OutFrame>();
  list: TurnList | undefined;
  previous: OutQueue | undefined;
  next: OutQueue | undefined;

  constructor(id: number) {
    this.id = id;
  }
}

/**
 * An intrusive list of queues: taking the first turn, adding one and
 * removing the turn of a closed channel are constant time.
 */
class TurnList {
  first: OutQueue | undefined;
  last: OutQueue | undefined;

  get isEmpty(): boolean {
    return this.first === undefined;
  }

  add(queue: OutQueue): void {
    queue.list = this;
    queue.previous = this.last;
    queue.next = undefined;
    if (this.last === undefined) {
      this.first = queue;
    } else {
      this.last.next = queue;
    }
    this.last = queue;
  }

  remove(queue: OutQueue): void {
    if (queue.previous === undefined) {
      this.first = queue.next;
    } else {
      queue.previous.next = queue.next;
    }
    if (queue.next === undefined) {
      this.last = queue.previous;
    } else {
      queue.next.previous = queue.previous;
    }
    queue.list = undefined;
    queue.previous = undefined;
    queue.next = undefined;
  }

  /** Removes and returns the first queue; the list must not be empty. */
  shift(): OutQueue {
    const queue = this.first as OutQueue;
    this.remove(queue);
    return queue;
  }

  clear(): void {
    let queue = this.first;
    while (queue !== undefined) {
      const next = queue.next;
      queue.list = undefined;
      queue.previous = undefined;
      queue.next = undefined;
      queue = next;
    }
    this.first = undefined;
    this.last = undefined;
  }
}

/**
 * What the scheduler needs of its connection.
 *
 * @internal
 */
export interface OutputSchedulerHost {
  /** Whether frames may still be written. */
  writable(): boolean;
  /** Whether a frame written now goes out without waiting (`FrameTransport.isOutputReady`, true without it). */
  outputReady(): boolean;
  /** Resolves when {@link outputReady} becomes true; undefined when the transport cannot tell. */
  whenOutputReady(): Promise<void> | undefined;
  /** The channel the id belongs to now, if any. */
  link(id: number): MuxChannelLink | undefined;
  /** Writes [bytes]; [opens] is the channel an OPEN opens, made visible around the write. */
  writeOpening(bytes: Uint8Array, opens: MuxChannelLink | undefined): void;
}

/**
 * The output scheduler of a `FrameMuxConnection`: per-channel queues and
 * their turns. Made by the connection.
 *
 * @internal
 */
export class OutputScheduler {
  readonly #host: OutputSchedulerHost;
  readonly #bulkZipper: number;
  readonly #queues = new Map<number, OutQueue>();
  readonly #ordinaryTurns = new TurnList();
  readonly #bulkTurns = new TurnList();
  #ordinarySinceBulk = 0;
  #heldFrames = 0;
  #heldBytes = 0;
  #awaitingOutput = false;
  #pumping = false;

  constructor(host: OutputSchedulerHost, bulkZipper: number) {
    this.#host = host;
    this.#bulkZipper = bulkZipper;
  }

  /** Frames held because the transport was not ready to take them. */
  get heldFrames(): number {
    return this.#heldFrames;
  }

  /**
   * Bytes of the encoded frames held. DATA is held only once its
   * channel's send window took it, so this is bounded by the windows the
   * peer granted, plus a CLOSE per channel.
   */
  get heldBytes(): number {
    return this.#heldBytes;
  }

  /**
   * Writes [frame] of [link] now, or holds it in its channel's queue if it
   * is DATA and the transport is not ready (or other frames are held), or
   * anything behind DATA held for the channel.
   */
  schedule(link: MuxChannelLink, frame: MuxFrame, opens?: MuxChannelLink): void {
    if (!this.#host.writable()) {
      return;
    }
    const id = link.id;
    const isData = frame.command === MuxCommand.data;
    let queue = this.#queues.get(id);
    if (queue === undefined) {
      if (!isData || (this.#heldFrames === 0 && this.#host.outputReady())) {
        if (isData) {
          link.dataWritten(MuxCredit.costOf(frame.payload.length));
        }
        this.#host.writeOpening(frame.encode(), opens);
        return;
      }
      queue = new OutQueue(id);
      this.#queues.set(id, queue);
    }
    const bytes = frame.encode();
    queue.frames.push({ bytes, isData, opens });
    this.#heldFrames++;
    this.#heldBytes += bytes.length;
    if (queue.list === undefined) {
      this.#takeTurn(queue);
    }
    this.pump();
  }

  /** Puts [queue] at the end of the turns of its channel's tier. */
  #takeTurn(queue: OutQueue): void {
    const link = this.#host.link(queue.id);
    if (link !== undefined && link.channel.priority === 'bulk') {
      this.#bulkTurns.add(queue);
    } else {
      this.#ordinaryTurns.add(queue);
    }
  }

  /** Moves [link]'s queued output to its current tier. */
  reprioritize(link: MuxChannelLink): void {
    const queue = this.#queues.get(link.id);
    if (queue === undefined || queue.list === undefined || this.#host.link(link.id) !== link) {
      return;
    }
    queue.list.remove(queue);
    this.#takeTurn(queue);
  }

  /** The queue whose turn it is, or undefined when none holds a frame. */
  #nextTurn(): OutQueue | undefined {
    for (;;) {
      const bulkWaiting = !this.#bulkTurns.isEmpty;
      let queue: OutQueue;
      if (
        !this.#ordinaryTurns.isEmpty &&
        (!bulkWaiting || this.#ordinarySinceBulk < this.#bulkZipper)
      ) {
        queue = this.#ordinaryTurns.shift();
        if (bulkWaiting) {
          this.#ordinarySinceBulk++;
        }
      } else if (bulkWaiting) {
        queue = this.#bulkTurns.shift();
        this.#ordinarySinceBulk = 0;
      } else {
        return undefined;
      }
      if (queue.frames.length > 0) {
        return queue;
      }
    }
  }

  /** Writes held frames while the transport takes them. */
  pump(): void {
    if (this.#pumping) {
      return;
    }
    this.#pumping = true;
    try {
      while (this.#heldFrames > 0 && this.#host.writable()) {
        if (!this.#host.outputReady()) {
          this.#awaitOutput();
          return;
        }
        const queue = this.#nextTurn();
        if (queue === undefined) {
          return;
        }
        this.#writeHead(queue);
      }
    } finally {
      this.#pumping = false;
    }
  }

  /** Writes the first frame of [queue] and gives it its next turn. */
  #writeHead(queue: OutQueue): void {
    const frame = queue.frames.shift() as OutFrame;
    this.#heldFrames--;
    this.#heldBytes -= frame.bytes.length;
    if (queue.frames.length === 0) {
      this.#queues.delete(queue.id);
    } else {
      this.#takeTurn(queue);
    }
    if (frame.isData) {
      this.#host
        .link(queue.id)
        ?.dataWritten(MuxCredit.costOf(frame.bytes.length - MuxFrame.headerSizeFor(queue.id)));
    }
    this.#host.writeOpening(frame.bytes, frame.opens);
  }

  #awaitOutput(): void {
    if (this.#awaitingOutput) {
      return;
    }
    const ready = this.#host.whenOutputReady();
    if (ready === undefined) {
      return;
    }
    this.#awaitingOutput = true;
    ready.then(
      () => {
        this.#awaitingOutput = false;
        this.pump();
      },
      () => {
        this.#awaitingOutput = false;
      },
    );
  }

  /**
   * Drops the DATA queued for [id], whose CLOSE the peer sent, and writes
   * what follows it (this side's CLOSE, an OPEN) at once, so that nothing
   * stays queued for an id that is about to be free.
   */
  dropQueuedData(id: number): void {
    const queue = this.#queues.get(id);
    if (queue === undefined) {
      return;
    }
    this.#queues.delete(id);
    const rest: OutFrame[] = [];
    for (const frame of queue.frames) {
      this.#heldFrames--;
      this.#heldBytes -= frame.bytes.length;
      if (!frame.isData) {
        rest.push(frame);
      }
    }
    queue.list?.remove(queue);
    queue.frames.clear();
    for (const frame of rest) {
      this.#host.writeOpening(frame.bytes, frame.opens);
    }
  }

  /**
   * Writes everything held, in the order of the turns, whether or not the
   * transport is ready: the connection is closing, and the transport
   * writes what it was given before it closes.
   */
  flushHeld(): void {
    while (this.#heldFrames > 0 && this.#host.writable()) {
      const queue = this.#nextTurn();
      if (queue === undefined) {
        break;
      }
      this.#writeHead(queue);
    }
  }

  /** Forgets everything held: the connection ended. */
  clear(): void {
    this.#queues.clear();
    this.#ordinaryTurns.clear();
    this.#bulkTurns.clear();
    this.#heldFrames = 0;
    this.#heldBytes = 0;
  }
}
