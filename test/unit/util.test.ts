import { describe, expect, test } from 'vitest';
import { AsyncQueue, Deferred, Emitter, sleep, systemClock } from '../../src/core/util/index.ts';
import { ManualClock } from '../../src/testing/manual-clock.ts';

describe('Deferred', () => {
  test('settles once and never reports an unhandled rejection', async () => {
    const d = new Deferred<number>();
    expect(d.isSettled).toBe(false);
    expect(d.reject(new Error('first'))).toBe(true);
    expect(d.resolve(1)).toBe(false);
    await expect(d.promise).rejects.toThrow('first');
  });

  test('sleep aborts with the signal reason', async () => {
    const controller = new AbortController();
    const p = sleep(10_000, controller.signal);
    controller.abort(new Error('stop'));
    await expect(p).rejects.toThrow('stop');
    await sleep(1);
  });

  test('sleep runs on the clock it is given', async () => {
    const clock = new ManualClock();
    let woke = false;
    void sleep(100, undefined, clock).then(() => {
      woke = true;
    });
    await clock.elapseAsync(99);
    expect(woke).toBe(false);
    await clock.elapseAsync(1);
    expect(woke).toBe(true);
    const controller = new AbortController();
    const aborted = sleep(100, controller.signal, clock);
    controller.abort(new Error('stop'));
    await expect(aborted).rejects.toThrow('stop');
    expect(clock.pendingCount).toBe(0);
  });
});

describe('Emitter', () => {
  test('on, once, off and emit', () => {
    const e = new Emitter<{ value: number }>();
    const seen: number[] = [];
    const off = e.on('value', (v) => seen.push(v));
    e.once('value', (v) => seen.push(v * 10));
    expect(e.emit('value', 1)).toBe(true);
    expect(e.emit('value', 2)).toBe(true);
    off();
    expect(e.emit('value', 3)).toBe(false);
    expect(seen).toEqual([1, 10, 2]);
  });

  test('events() iterates until the signal aborts', async () => {
    const e = new Emitter<{ value: number }>();
    const controller = new AbortController();
    const seen: number[] = [];
    const loop = (async () => {
      for await (const v of e.events('value', controller.signal)) {
        seen.push(v);
      }
    })();
    e.emit('value', 1);
    e.emit('value', 2);
    await sleep(1);
    controller.abort();
    await loop;
    expect(seen).toEqual([1, 2]);
    expect(e.listenerCount('value')).toBe(0);
  });
});

describe('AsyncQueue', () => {
  test('delivers buffered and awaited values, then ends', async () => {
    const q = new AsyncQueue<number>();
    q.push(1);
    const seen: number[] = [];
    const loop = (async () => {
      for await (const v of q) {
        seen.push(v);
      }
    })();
    await sleep(1);
    q.push(2);
    q.end();
    q.push(3);
    await loop;
    expect(seen).toEqual([1, 2]);
    expect(q.isEnded).toBe(true);
  });

  test('fail ends the iteration with the error, after the buffered values', async () => {
    const q = new AsyncQueue<number>();
    q.push(1);
    q.fail(new Error('lost'));
    const it = q[Symbol.asyncIterator]();
    expect((await it.next()).value).toBe(1);
    await expect(it.next()).rejects.toThrow('lost');
  });

  test('breaking out calls onCancel', async () => {
    const q = new AsyncQueue<number>();
    let cancelled = 0;
    q.onCancel = () => cancelled++;
    q.push(1);
    for await (const _ of q) {
      break;
    }
    expect(cancelled).toBe(1);
    expect(q.hasConsumer).toBe(true);
  });
});

describe('ManualClock', () => {
  test('fires timers in due order while advancing', () => {
    const clock = new ManualClock();
    const fired: string[] = [];
    clock.setTimeout(() => fired.push('b'), 20);
    clock.setTimeout(() => fired.push('a'), 10);
    const c = clock.setTimeout(() => fired.push('c'), 15);
    clock.clearTimeout(c);
    clock.advance(10);
    expect(fired).toEqual(['a']);
    expect(clock.now()).toBe(10);
    clock.advance(100);
    expect(fired).toEqual(['a', 'b']);
    expect(clock.pendingCount).toBe(0);
  });
  test('fires timers armed by a callback within the advanced span', () => {
    const clock = new ManualClock(5);
    const fired: number[] = [];
    clock.setTimeout(() => {
      fired.push(clock.now());
      clock.setTimeout(() => fired.push(clock.now()), 3);
    }, 2);
    clock.advance(10);
    expect(fired).toEqual([7, 10]);
    expect(clock.now()).toBe(15);
  });
});

describe('systemClock', () => {
  test('runs and clears timers', async () => {
    let fired = 0;
    const handle = systemClock.setTimeout(() => fired++, 1);
    systemClock.clearTimeout(handle);
    await new Promise<void>((resolve) => systemClock.setTimeout(resolve, 5));
    expect(fired).toBe(0);
    expect(systemClock.now()).toBeGreaterThan(0);
  });
});
