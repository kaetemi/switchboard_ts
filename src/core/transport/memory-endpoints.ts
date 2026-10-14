/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The process-wide registry of in-process (`mem://`) listeners behind
 * `listenMemory`. Ports `memory_endpoints.dart`. The only module-level
 * state of the library; only what uses it imports this module.
 */

import { StatusCode, SwitchboardError } from '../status.ts';
import type { FrameTransport } from './contract.ts';

/**
 * Takes one side of a new in-memory transport pair as an accepted
 * connection. [remote] describes the initiator, for logs.
 */
export type MemoryAcceptor = (transport: FrameTransport, remote: string) => void;

const acceptors = new Map<string, MemoryAcceptor>();
let counter = 0;
const validName = /^[A-Za-z0-9._~-]+$/;

/** A fresh id: a counter, unique in this process, and a random suffix. Never an id in use. */
function generateId(): string {
  for (;;) {
    counter++;
    const suffix = Math.floor(Math.random() * 0x1000000)
      .toString(16)
      .padStart(6, '0');
    const id = `sb${counter}-${suffix}`;
    if (!acceptors.has(id)) {
      return id;
    }
  }
}

/**
 * The `mem://<id>` listeners of this process, by id. Every node of the
 * process sees the same listeners, and nothing outside it does. Ids are
 * compared in lower case, as URI hosts are.
 */
export const MemoryEndpoints = Object.freeze({
  /** The URI scheme of in-process endpoints. */
  scheme: 'mem',

  /**
   * Registers [acceptor] under `name` (lower-cased), or under a new id
   * unique in this process when no name is given, and returns its
   * `mem://<id>` URI.
   *
   * @throws {RangeError} if the name is empty or has characters other than
   *   ASCII letters, digits, `.`, `_`, `~` and `-`.
   * @throws {SwitchboardError} `ALREADY_EXISTS` if a listener of this
   *   process already has that id.
   */
  register(acceptor: MemoryAcceptor, options: { readonly name?: string } = {}): string {
    const name = options.name;
    let id: string;
    if (name !== undefined) {
      if (!validName.test(name)) {
        throw new RangeError(
          `memory endpoint name must be ASCII letters, digits, ".", "_", "~" or "-": ${JSON.stringify(name)}`,
        );
      }
      id = name.toLowerCase();
      if (acceptors.has(id)) {
        throw SwitchboardError.of(
          StatusCode.alreadyExists,
          `memory endpoint mem://${id} is already listening`,
        );
      }
    } else {
      id = generateId();
    }
    acceptors.set(id, acceptor);
    return `mem://${id}`;
  },

  /** The acceptor listening at [id] (compared in lower case), or `undefined`. */
  lookup(id: string): MemoryAcceptor | undefined {
    return acceptors.get(id.toLowerCase());
  },

  /** Removes the listener at [id], if any. */
  unregister(id: string): void {
    acceptors.delete(id.toLowerCase());
  },

  /** The ids listening now. For tests: empty once every node that listened has closed. */
  get ids(): readonly string[] {
    return Object.freeze([...acceptors.keys()]);
  },
});
