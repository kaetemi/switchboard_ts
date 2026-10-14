/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import type { Clock } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';

/**
 * Bounded identity holds shared by node dispatch and the relay. Internal.
 *
 * Only one listener is attached to a connection's next identity event.
 * Closed or timed-out channels remove their waiters: an unidentified peer
 * opening and closing channels must not accumulate listeners on a promise
 * that may never settle.
 */

const waiters = new WeakMap<MuxConnection, IdentityWaiters>();

/** Waits until the identity of [channel]'s connection changes, [channel] ends, or [timeout] ms pass. */
export function waitForIdentityChange(
  channel: MuxChannel,
  timeout: number,
  clock: Clock,
): Promise<void> {
  const connection = channel.connection;
  let w = waiters.get(connection);
  if (w === undefined) {
    w = new IdentityWaiters(connection);
    waiters.set(connection, w);
  }
  return w.wait(channel, timeout, clock);
}

class IdentityWaiters {
  readonly #connection: MuxConnection;
  readonly #waiting = new Set<Deferred<void>>();
  #listening = false;

  constructor(connection: MuxConnection) {
    this.#connection = connection;
  }

  async wait(channel: MuxChannel, timeout: number, clock: Clock): Promise<void> {
    const done = new Deferred<void>();
    this.#waiting.add(done);
    if (!this.#listening) {
      this.#listening = true;
      void this.#connection.identityChanged.then(() => {
        this.#listening = false;
        for (const waiter of this.#waiting) {
          waiter.resolve();
        }
        this.#waiting.clear();
      });
    }
    const finish = (): void => {
      if (this.#waiting.delete(done)) {
        done.resolve();
      }
    };
    const timer = clock.setTimeout(finish, timeout);
    void channel.done.then(finish);
    try {
      await done.promise;
    } finally {
      clock.clearTimeout(timer);
      this.#waiting.delete(done);
    }
  }
}
