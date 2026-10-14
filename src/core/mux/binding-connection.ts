/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The base of a binding's connection other than the frame mux (the QUIC
 * and WebTransport binding's `QuicBindingConnection`). Ports
 * `binding_connection.dart`.
 */

import type { MuxChannel } from './contract.ts';

/**
 * The base of a binding's connection other than the frame mux: answers the
 * frame mux's own diagnostics of {@link MuxConnection} as a connection
 * without them does, keeps the drain holds of an independent-stream
 * binding, and leaves the rest of the contract to the subclass, which
 * declares `implements MuxConnection`.
 *
 * Answered here: `sentBytes` and `receivedBytes` 0 (nothing counted),
 * `peerMaxChannels` 0 (no stated limit), `bufferedBytes` the sum of the
 * channels' `bufferedBytes`, `openPayloadBytes` 0, `isReceivePaused` false,
 * `unconfirmedCloseCount` 0 and `heldOutputBytes` 0; `hasOrderedChannels`
 * false, for independent channel streams; `localIdentity` is a plain field.
 * A subclass overrides whatever it can answer better.
 *
 * The identity exchange, `adoptIncoming` and `admitAfterGoAway` are the
 * subclass's, as in Dart: the binding's identity rules (exporter binding,
 * held control messages) differ from the frame mux's (held frames).
 *
 * @see {@link MuxConnection}
 */
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
   * Keeps admitted work on [parentId] alive until [until] settles; a later
   * hold on the same parent replaces it. Talk calls this only for its
   * bounded backlog of lingering parents. A binding includes these holds in
   * its idle check and admits their continuation streams after GOAWAY. Its
   * grace timeout still applies.
   *
   * Part of the contract between Talk and a binding, not an application
   * call: it is public so that Talk and a binding's tests, standing in for
   * Talk, can make it. An application that holds a connection open waits on
   * its channels.
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

  /** Nothing counted: 0. */
  get sentBytes(): number {
    return 0;
  }

  /** Nothing counted: 0. */
  get receivedBytes(): number {
    return 0;
  }

  /** No stated limit: 0. */
  get peerMaxChannels(): number {
    return 0;
  }

  /** Independent channel streams: false. */
  get hasOrderedChannels(): boolean {
    return false;
  }

  /** The sum of the channels' `bufferedBytes`. */
  get bufferedBytes(): number {
    let total = 0;
    for (const channel of this.channels) {
      total += channel.bufferedBytes;
    }
    return total;
  }

  /** None held: 0. */
  get openPayloadBytes(): number {
    return 0;
  }

  /** Never paused: false. */
  get isReceivePaused(): boolean {
    return false;
  }

  /** None: 0. */
  get unconfirmedCloseCount(): number {
    return 0;
  }

  /** None held: 0. */
  get heldOutputBytes(): number {
    return 0;
  }
}
