/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { MemoryEndpointRegistry } from '../../../src/core/switchboard/runtime.ts';
import type { MemoryAcceptor } from '../../../src/core/transport/memory-endpoints.ts';

const validName = /^[A-Za-z0-9._~-]+$/;

/**
 * A `mem://` registry of its own for the nodes of one test, with the rules
 * of the process-wide `MemoryEndpoints`: what one test registers is never
 * seen by another, and a leak is visible in {@link ids}.
 */
export class MemoryEndpointTable implements MemoryEndpointRegistry {
  readonly #acceptors = new Map<string, MemoryAcceptor>();
  #counter = 0;

  register(acceptor: MemoryAcceptor, options: { readonly name?: string } = {}): string {
    const name = options.name;
    let id: string;
    if (name !== undefined) {
      if (!validName.test(name)) {
        throw new RangeError(`memory endpoint name "${name}" is not valid`);
      }
      id = name.toLowerCase();
      if (this.#acceptors.has(id)) {
        throw SwitchboardError.of(
          StatusCode.alreadyExists,
          `memory endpoint mem://${id} is already listening`,
        );
      }
    } else {
      do {
        const suffix = Math.floor(Math.random() * 0x1000000)
          .toString(16)
          .padStart(6, '0');
        id = `sb${++this.#counter}-${suffix}`;
      } while (this.#acceptors.has(id));
    }
    this.#acceptors.set(id, acceptor);
    return `mem://${id}`;
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
