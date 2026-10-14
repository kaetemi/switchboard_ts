/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The two halves of a byte connection as the stream transport drives them:
 * a pull-based {@link ByteInput} and an {@link ByteOutput} that reports
 * whether it takes more. `webByteInput` and `webByteOutput` adapt Web
 * Streams; the Node TCP transport adapts a `net.Socket` directly, so that
 * its `writableLength` and `bytesWritten` accounting stay visible.
 */

/** The receiving half of a byte connection. */
export interface ByteInput {
  /**
   * The next chunk, or `undefined` once the peer has closed. One call at a
   * time. Rejects when the connection fails.
   */
  read(): Promise<Uint8Array | undefined>;
  /** Stops reading; a pending {@link read} resolves or rejects. Idempotent. */
  cancel(): void;
}

/** The sending half of a byte connection. */
export interface ByteOutput {
  /** Whether a {@link write} now is taken without waiting behind earlier writes. */
  readonly canWrite: boolean;
  /** Resolves when {@link canWrite} may have become true, or the output is gone. Never rejects. */
  ready(): Promise<void>;
  /** Hands [bytes] over. The output owns them afterwards. */
  write(bytes: Uint8Array): void;
  /**
   * Flushes what was written and closes the output (a FIN for a socket).
   * Resolves once closed; rejects when the output fails first.
   */
  close(): Promise<void>;
  /** Destroys the output, dropping what it has not written. Idempotent; never throws. */
  abort(): void;
  /**
   * Registers the callback told, once, that the output failed: nothing
   * written is delivered any more.
   */
  onFailure(callback: (error: unknown) => void): void;
}

const ignore = (): void => {};

/** A {@link ByteInput} over a Web Streams byte source. Locks [readable]. */
export function webByteInput(readable: ReadableStream<Uint8Array>): ByteInput {
  const reader = readable.getReader();
  let cancelled = false;
  return {
    async read() {
      const result = await reader.read();
      if (result.done) {
        return undefined;
      }
      const value: unknown = result.value;
      // Some sources yield ArrayBuffers or other views; frames are bytes.
      if (value instanceof Uint8Array) {
        return value;
      }
      if (ArrayBuffer.isView(value)) {
        return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
      }
      if (value instanceof ArrayBuffer) {
        return new Uint8Array(value);
      }
      throw new TypeError('byte input yielded a chunk that is not bytes');
    },
    cancel() {
      if (cancelled) {
        return;
      }
      cancelled = true;
      reader.cancel().catch(ignore);
    },
  };
}

/**
 * A {@link ByteOutput} over a Web Streams byte sink. Locks [writable]. A
 * write is taken while the sink's queue has room (`desiredSize > 0`), or
 * when nothing written is still pending (a sink with a high-water mark of
 * 0), so the sink's queuing strategy decides how much it holds; the rest
 * waits in the transport, where it is counted and throttled.
 */
export function webByteOutput(writable: WritableStream<Uint8Array>): ByteOutput {
  const writer = writable.getWriter();
  let failed = false;
  let failure: ((error: unknown) => void) | undefined;
  let pending = 0;
  // Settles once every write so far has: writes complete in order.
  let lastWrite: Promise<void> = Promise.resolve();
  const fail = (error: unknown): void => {
    if (failed) {
      return;
    }
    failed = true;
    failure?.(error);
  };
  writer.closed.then(ignore, fail);
  return {
    get canWrite() {
      const desired = writer.desiredSize;
      return !failed && desired !== null && (desired > 0 || pending === 0);
    },
    ready() {
      return Promise.race([writer.ready, lastWrite]).then(ignore, ignore);
    },
    write(bytes) {
      pending++;
      lastWrite = writer.write(bytes).then(
        () => {
          pending--;
        },
        (error: unknown) => {
          pending--;
          fail(error);
        },
      );
    },
    close() {
      return writer.close();
    },
    abort() {
      failed = true;
      writer.abort().catch(ignore);
    },
    onFailure(callback) {
      failure = callback;
    },
  };
}
