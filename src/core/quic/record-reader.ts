/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ProtocolError } from '../status.ts';
import { type QuicStream, QuicStreamReset } from './contract.ts';
import {
  type QuicRecord,
  type QuicRecordCommand,
  quicHeaderCommand,
  quicHeaderLength,
  quicRecordHeaderSize,
  readQuicRecordHeader,
} from './records.ts';

/** A record whose header announces a body over the receiver's limit: the body is not read. */
export class RecordTooLarge extends Error {
  /** The header's command code (0 to 2). */
  readonly command: number;
  /** The body length the header announced. */
  readonly length: number;
  /** The limit it exceeds. */
  readonly limit: number;

  constructor(command: number, length: number, limit: number) {
    super(`record of ${length} bytes, limit ${limit}`);
    this.name = 'RecordTooLarge';
    this.command = command;
    this.length = length;
    this.limit = limit;
  }
}

const emptyBody = new Uint8Array(0);

/**
 * Reads records from the peer's direction of a {@link QuicStream}, pulling
 * exactly the bytes of one record at a time, so that what follows stays in
 * the stack and holds back the peer's credit. A port of `record_reader.dart`.
 */
export class RecordReader {
  /** The largest chunk asked of the provider at once while a body arrives. */
  static readonly maxChunk = 256 * 1024;

  /** The stream read. */
  readonly stream: QuicStream;
  /**
   * Waited for before each read from the provider, within a record too and
   * in {@link expectEnd}: what returns a promise holds the reads back, and
   * with them the peer's credit, until it settles (a channel holding the
   * peer's `STOP_SENDING` for the session's end). A read already issued is
   * not held. Undefined: nothing holds them.
   */
  gate: (() => Promise<void> | undefined) | undefined;
  readonly #header = new Uint8Array(quicRecordHeaderSize);

  constructor(stream: QuicStream) {
    this.stream = stream;
  }

  /**
   * The next record, or undefined at the end of the direction (FIN) between
   * records.
   *
   * The header is checked before the body is read: command 3 is a
   * {@link ProtocolError}, a body over [limit] a {@link RecordTooLarge}. FIN
   * inside a record is a ProtocolError. A reset fails with the provider's
   * {@link QuicStreamReset} (the partial record is dropped), the end of the
   * session with its QuicError.
   */
  async next(limit: number): Promise<QuicRecord | undefined> {
    let have = 0;
    while (have < quicRecordHeaderSize) {
      const hold = this.gate?.();
      if (hold !== undefined) {
        await hold;
      }
      const chunk = await this.stream.read(quicRecordHeaderSize - have);
      if (chunk === undefined) {
        if (have === 0) {
          return undefined;
        }
        throw new ProtocolError('FIN inside a record header');
      }
      if (chunk.length === 0 || chunk.length > quicRecordHeaderSize - have) {
        throw new Error('the provider returned an invalid read chunk');
      }
      this.#header.set(chunk, have);
      have += chunk.length;
    }
    const header = readQuicRecordHeader(this.#header);
    const code = quicHeaderCommand(header);
    const length = quicHeaderLength(header);
    if (code === 3) {
      throw new ProtocolError('record of the reserved command 3');
    }
    const command = code as QuicRecordCommand;
    if (length > limit) {
      throw new RecordTooLarge(command, length, limit);
    }
    if (length === 0) {
      return { command, body: emptyBody };
    }
    const first = await this.#read(length);
    if (first.length === length) {
      return { command, body: first };
    }
    // Allocate in proportion to the bytes received: a header and one body
    // byte must not allocate an entire (possibly very large) limit.
    const chunks: Uint8Array[] = [first];
    let filled = first.length;
    while (filled < length) {
      const chunk = await this.#read(length - filled);
      chunks.push(chunk);
      filled += chunk.length;
    }
    const body = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.length;
    }
    return { command, body };
  }

  /** Up to [wanted] bytes of a body; FIN is a protocol error. */
  async #read(wanted: number): Promise<Uint8Array> {
    const max = wanted < RecordReader.maxChunk ? wanted : RecordReader.maxChunk;
    const hold = this.gate?.();
    if (hold !== undefined) {
      await hold;
    }
    const chunk = await this.stream.read(max);
    if (chunk === undefined) {
      throw new ProtocolError('FIN inside a record');
    }
    if (chunk.length === 0 || chunk.length > max) {
      throw new Error('the provider returned an invalid read chunk');
    }
    return chunk;
  }

  /**
   * Reads past a CLOSE record: resolves at FIN or at a reset (the peer gave
   * up on its direction after its close, which changes nothing). Anything
   * else is a {@link ProtocolError}.
   */
  async expectEnd(): Promise<void> {
    try {
      for (;;) {
        const hold = this.gate?.();
        if (hold !== undefined) {
          await hold;
        }
        const chunk = await this.stream.read(1);
        if (chunk === undefined) {
          return;
        }
        if (chunk.length > 0) {
          throw new ProtocolError('a record after the CLOSE record');
        }
      }
    } catch (error) {
      if (error instanceof QuicStreamReset) {
        // The direction ended all the same.
        return;
      }
      throw error;
    }
  }
}
