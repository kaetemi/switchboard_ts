/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { StatusCode, SwitchboardError } from '../status.ts';
import type { MemoryAcceptor, MemoryEndpointRegistry } from './runtime.ts';

const validName = /^[A-Za-z0-9._~-]+$/;

/**
 * A table of in-process listeners (`mem://<id>`). The core runtime shares
 * one per process ({@link sharedMemoryEndpoints}); tests that want
 * isolation create their own.
 */
export class MemoryEndpointTable implements MemoryEndpointRegistry {
  readonly #acceptors = new Map<string, MemoryAcceptor>();
  #counter = 0;

  register(acceptor: MemoryAcceptor, name?: string): string {
    let id: string;
    if (name !== undefined) {
      if (!validName.test(name)) {
        throw new RangeError(
          `memory endpoint name "${name}" must be ASCII letters, digits, ".", "_", "~" or "-"`,
        );
      }
      id = name.toLowerCase();
      if (this.#acceptors.has(id)) {
        throw SwitchboardError.of(
          StatusCode.alreadyExists,
          `memory endpoint mem://${id} is already listening`,
        );
      }
    } else {
      id = this.#generateId();
    }
    this.#acceptors.set(id, acceptor);
    return `mem://${id}`;
  }

  /** A fresh id: a counter and a random suffix; never one in use. */
  #generateId(): string {
    for (;;) {
      this.#counter++;
      const suffix = Math.floor(Math.random() * 0x1000000)
        .toString(16)
        .padStart(6, '0');
      const id = `sb${this.#counter}-${suffix}`;
      if (!this.#acceptors.has(id)) {
        return id;
      }
    }
  }

  lookup(id: string): MemoryAcceptor | undefined {
    return this.#acceptors.get(id.toLowerCase());
  }

  unregister(id: string): void {
    this.#acceptors.delete(id.toLowerCase());
  }

  /** The ids listening now. */
  get ids(): string[] {
    return [...this.#acceptors.keys()];
  }
}

/** The process-wide table of the core runtime: every node using it sees the same listeners. */
export const sharedMemoryEndpoints: MemoryEndpointTable = new MemoryEndpointTable();
