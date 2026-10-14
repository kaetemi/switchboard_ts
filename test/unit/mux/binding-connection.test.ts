// The base of a binding's connection: drain holds and the neutral
// diagnostics. Dart reaches `BindingConnection` only through its bindings
// (`binding_drain_test.dart`, the QUIC suite); these check the base alone.
import { describe, expect, test } from 'vitest';
import { BindingConnection } from '../../../src/core/mux/binding-connection.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { pump } from '../transport/support.ts';

class TestBinding extends BindingConnection {
  readonly list: { bufferedBytes: number }[] = [];
  changes = 0;

  get channels(): Iterable<MuxChannel> {
    return this.list as unknown as MuxChannel[];
  }

  override drainStateChanged(): void {
    this.changes++;
  }
}

describe('BindingConnection', () => {
  test('answers the frame mux diagnostics neutrally', () => {
    const binding = new TestBinding();
    expect(binding.sentBytes).toBe(0);
    expect(binding.receivedBytes).toBe(0);
    expect(binding.peerMaxChannels).toBe(0);
    expect(binding.hasOrderedChannels).toBe(false);
    expect(binding.openPayloadBytes).toBe(0);
    expect(binding.isReceivePaused).toBe(false);
    expect(binding.unconfirmedCloseCount).toBe(0);
    expect(binding.heldOutputBytes).toBe(0);
    expect(binding.localIdentity).toBeUndefined();
    binding.localIdentity = 'node-1';
    expect(binding.localIdentity).toBe('node-1');
  });

  test('buffered bytes are the sum of the channels', () => {
    const binding = new TestBinding();
    expect(binding.bufferedBytes).toBe(0);
    binding.list.push({ bufferedBytes: 10 }, { bufferedBytes: 32 });
    expect(binding.bufferedBytes).toBe(42);
  });

  test('a drain hold lasts until its promise settles, either way', async () => {
    const binding = new TestBinding();
    const kept = new Deferred<void>();
    const failed = new Deferred<void>();
    binding.retainForDrain(3, kept.promise);
    binding.retainForDrain(5, failed.promise);
    expect(binding.changes).toBe(2);
    expect(binding.hasDrainHolds).toBe(true);
    expect(binding.retainedForDrain(3)).toBe(true);
    expect(binding.retainedForDrain(5)).toBe(true);
    expect(binding.retainedForDrain(7)).toBe(false);
    failed.reject(new Error('gone'));
    await pump(1);
    expect(binding.retainedForDrain(5)).toBe(false);
    expect(binding.hasDrainHolds).toBe(true);
    expect(binding.changes).toBe(3);
    kept.resolve();
    await pump(1);
    expect(binding.retainedForDrain(3)).toBe(false);
    expect(binding.hasDrainHolds).toBe(false);
    expect(binding.changes).toBe(4);
  });

  test('a later hold on the same parent replaces the earlier one', async () => {
    const binding = new TestBinding();
    const first = new Deferred<void>();
    const second = new Deferred<void>();
    binding.retainForDrain(3, first.promise);
    binding.retainForDrain(3, second.promise);
    first.resolve();
    await pump(1);
    // The first settling releases nothing: the second holds the parent.
    expect(binding.retainedForDrain(3)).toBe(true);
    expect(binding.changes).toBe(2);
    second.resolve();
    await pump(1);
    expect(binding.retainedForDrain(3)).toBe(false);
    expect(binding.changes).toBe(3);
  });
});
