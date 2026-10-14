/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/** The events of an {@link Emitter}: a map from event name to listener argument. */
export type EventMap = Record<string, unknown>;

/** What `on` returns: call it to remove the listener. */
export type Unsubscribe = () => void;

/**
 * A small typed event emitter, the library's substitute for Dart's
 * broadcast streams: it works in every runtime (Node's `EventEmitter` does
 * not exist in browsers, and `EventTarget` types poorly), listeners are
 * removed by the function `on` returns, and {@link events} turns an event
 * into an async iterable for `for await`.
 *
 * Listeners run synchronously, in the order they were added. A listener
 * that throws does not stop the others; the error is reported through
 * `onListenerError` (default: rethrown asynchronously).
 */
export class Emitter<Events extends EventMap> {
  readonly #listeners = new Map<keyof Events, Set<(value: never) => void>>();

  /** What to do with an error thrown by a listener. */
  onListenerError: (error: unknown, event: keyof Events) => void = (error) => {
    queueMicrotask(() => {
      throw error;
    });
  };

  on<K extends keyof Events>(event: K, listener: (value: Events[K]) => void): Unsubscribe {
    let set = this.#listeners.get(event);
    if (set === undefined) {
      set = new Set();
      this.#listeners.set(event, set);
    }
    set.add(listener as (value: never) => void);
    return () => this.off(event, listener);
  }

  once<K extends keyof Events>(event: K, listener: (value: Events[K]) => void): Unsubscribe {
    const off = this.on(event, (value) => {
      off();
      listener(value);
    });
    return off;
  }

  off<K extends keyof Events>(event: K, listener: (value: Events[K]) => void): void {
    this.#listeners.get(event)?.delete(listener as (value: never) => void);
  }

  /** Delivers [value] to every listener of [event]. Returns whether any listened. */
  emit<K extends keyof Events>(event: K, value: Events[K]): boolean {
    const set = this.#listeners.get(event);
    if (set === undefined || set.size === 0) {
      return false;
    }
    for (const listener of [...set]) {
      try {
        (listener as (value: Events[K]) => void)(value);
      } catch (error) {
        this.onListenerError(error, event);
      }
    }
    return true;
  }

  listenerCount(event: keyof Events): number {
    return this.#listeners.get(event)?.size ?? 0;
  }

  /**
   * The values of [event] as an async iterable, from the moment it is called
   * (the current value, if any, is not replayed). Values arriving faster
   * than the loop takes them are queued without bound; stop with `break`,
   * `return` or [signal].
   */
  events<K extends keyof Events>(event: K, signal?: AbortSignal): AsyncIterable<Events[K]> {
    const self = this;
    return {
      [Symbol.asyncIterator](): AsyncIterator<Events[K]> {
        const queue: Events[K][] = [];
        let waiting: ((r: IteratorResult<Events[K]>) => void) | undefined;
        let done = false;
        const off = self.on(event, (value) => {
          if (waiting !== undefined) {
            const w = waiting;
            waiting = undefined;
            w({ value, done: false });
          } else {
            queue.push(value);
          }
        });
        const finish = (): void => {
          if (done) {
            return;
          }
          done = true;
          off();
          signal?.removeEventListener('abort', finish);
          const w = waiting;
          waiting = undefined;
          w?.({ value: undefined, done: true });
        };
        signal?.addEventListener('abort', finish, { once: true });
        if (signal?.aborted) {
          finish();
        }
        return {
          next(): Promise<IteratorResult<Events[K]>> {
            if (queue.length > 0) {
              return Promise.resolve({ value: queue.shift() as Events[K], done: false });
            }
            if (done) {
              return Promise.resolve({ value: undefined, done: true });
            }
            return new Promise((resolve) => {
              waiting = resolve;
            });
          },
          return(): Promise<IteratorResult<Events[K]>> {
            finish();
            return Promise.resolve({ value: undefined, done: true });
          },
        };
      },
    };
  }
}
