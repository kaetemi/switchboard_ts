/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * A stand-in for the protocol core's `BindingConnection` (the "protocol
 * core" thread's `src/core/mux/binding-connection.ts`, a port of
 * `switchboard`'s `binding_connection.dart`), which the QUIC binding
 * extends. Not exported from the package: once the core's base lands, the
 * binding imports it instead and this file goes.
 *
 * What it answers is what the Dart base answers: the drain holds of an
 * independent-stream binding, and the frame mux's diagnostics as a
 * connection without them does.
 */

import type { MuxChannel } from '../mux/contract.ts';

/** The base of a binding's connection other than the frame mux. */
export abstract class BindingConnection {
  // Only independent-stream bindings need to keep ended parents alive while
  // their application still waits for already referenced streams.
  readonly #drainHolds = new Map<number, Promise<void>>();

  /** This side's identity, which a peer's IDENT must name if it names anyone. */
  localIdentity: string | undefined;

  /** The channels not mutually closed yet. */
  abstract get channels(): Iterable<MuxChannel>;

  /** Whether admitted work still waits for streams after its parent ended. */
  get hasDrainHolds(): boolean {
    return this.#drainHolds.size > 0;
  }

  /** Whether [id] is an ended parent whose reader still waits for streams. */
  retainedForDrain(id: number): boolean {
    return this.#drainHolds.has(id);
  }

  /**
   * Keeps admitted work on [parentId] alive until [until] settles. Talk
   * calls this only for its bounded backlog of lingering parents. A binding
   * includes these holds in its idle check and admits their continuation
   * streams after GOAWAY. Its grace timeout still applies.
   *
   * Part of the contract between Talk and a binding, not an application
   * call.
   */
  retainForDrain(parentId: number, until: Promise<void>): void {
    this.#drainHolds.set(parentId, until);
    const release = (): void => {
      if (this.#drainHolds.get(parentId) === until) {
        this.#drainHolds.delete(parentId);
        this.drainStateChanged();
      }
    };
    until.then(release, release);
    this.drainStateChanged();
  }

  /**
   * Called when a drain hold is added or released. A binding schedules its
   * idle check here, after channel listeners have handled their end.
   */
  drainStateChanged(): void {}

  /** Nothing counted. */
  get sentBytes(): number {
    return 0;
  }

  /** Nothing counted. */
  get receivedBytes(): number {
    return 0;
  }

  /** No stated limit. */
  get peerMaxChannels(): number {
    return 0;
  }

  /** Independent channel streams. */
  get hasOrderedChannels(): boolean {
    return false;
  }

  /** The sum of the channels' buffered bytes. */
  get bufferedBytes(): number {
    let total = 0;
    for (const channel of this.channels) {
      total += channel.bufferedBytes;
    }
    return total;
  }

  /** None held. */
  get openPayloadBytes(): number {
    return 0;
  }

  /** Never paused. */
  get isReceivePaused(): boolean {
    return false;
  }

  /** None. */
  get unconfirmedCloseCount(): number {
    return 0;
  }

  /** None held. */
  get heldOutputBytes(): number {
    return 0;
  }
}
