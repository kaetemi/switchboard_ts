/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/sharding/chat_rooms_test.dart`, use
// case 5 of the wiki page "Polyverse Switchboard Sharding": chat rooms
// (`room`, eager managed, shared recovery). The wiki has N = 256; 16 rooms
// keep the test fast. Room history is in shared storage (a map shared by
// the servers), so a crashed server's rooms are reassigned at once.
// Members hold a long-lived channel per room: the server greets them with
// the history, pushes new messages, and closes their channels with
// RELOCATED naming the new owner when the room moves (they were served, so
// MOVED, which means nothing was processed, does not apply); members then
// rejoin through openTalkToSlot. Over TCP as in Dart, and over in-process
// `mem://` endpoints.

import { afterAll, describe, expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { AssignResult } from '../../../src/core/naming/naming-client-slots.ts';
import { SlotLifecycle, type SlotLoadOptions } from '../../../src/core/naming/slot-gate.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import type { MeshNode } from '../../../src/core/switchboard/mesh-node.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkChannel } from '../../../src/core/talk/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { expectNoMemoryListeners } from '../mesh-support.ts';
import { Cluster, delay, errorCode, MigrationWatch, retryable, until, within } from './cluster.ts';

const room = Name.of('room');
const rooms = 16;

/** A room server. [storage] is the shared storage: room -> messages. */
class RoomServer extends SlotLifecycle {
  readonly storage: Map<number, string[]>;
  readonly members = new Map<number, Set<IncomingChannel>>();

  constructor(storage: Map<number, string[]>) {
    super();
    this.storage = storage;
  }

  load(slot: number, { shared }: SlotLoadOptions): Promise<AssignResult> {
    expect(shared).toBe(true);
    if (!this.storage.has(slot)) {
      this.storage.set(slot, []);
    }
    this.members.set(slot, new Set());
    // Nothing is kept locally.
    return Promise.resolve(AssignResult.notHolding);
  }

  /**
   * The history is in shared storage already; the members rejoin at the
   * new owner (their posts in flight are answered first: the gate waited
   * for them).
   */
  override drain(slot: number): void {
    this.#dismiss(slot);
  }

  override unload(slot: number): void {
    this.#dismiss(slot);
    this.members.delete(slot);
  }

  #dismiss(slot: number): void {
    const relocated = this.gate.relocatedStatus(slot);
    for (const member of [...(this.members.get(slot) ?? [])]) {
      void member.reject(relocated);
    }
  }

  serve(channel: IncomingChannel, slot: number): void {
    this.gate.detach(channel);
    const present = this.members.get(slot) as Set<IncomingChannel>;
    present.add(channel);
    const talk = channel.talk();
    void talk.done.then(() => present.delete(channel));
    void talk.send('HIST', this.#history(slot).join('\n'));
    void talk.serve((m) =>
      this.gate.serveRequest(m, slot, (m) => {
        const message = m.text();
        const history = this.#history(slot);
        // Posts carry their author and number: a retried post whose answer
        // was lost is not added twice.
        if (!history.includes(message)) {
          history.push(message);
          for (const other of present) {
            try {
              void other.talk().send('MSG', message);
            } catch (e) {
              if (!(e instanceof SwitchboardError)) {
                throw e;
              }
              // Leaving.
            }
          }
        }
        void m.reply('ok');
      }),
    );
  }

  #history(slot: number): string[] {
    return this.storage.get(slot) as string[];
  }
}

/**
 * Whether a member rejoins and posts again after [error]: {@link retryable},
 * or `RELOCATED`, the room moving while the member was in it. Posts carry
 * their author and number, so one that may have been applied is safely
 * sent again.
 */
function rejoinable(error: unknown): boolean {
  return retryable(error) || errorCode(error) === StatusCode.relocated;
}

/** A chat member in one room. */
class Member {
  readonly node: Switchboard;
  readonly slot: number;
  readonly name: string;
  #talk: TalkChannel | undefined;
  #leaving = false;
  view: string[] = [];
  /** How each channel ended (while the member stayed). */
  readonly ends: number[] = [];
  joins = 0;
  posted = 0;

  constructor(node: Switchboard, slot: number, name: string) {
    this.node = node;
    this.slot = slot;
    this.name = name;
  }

  async #join(): Promise<TalkChannel> {
    for (;;) {
      try {
        const talk = await this.node.openTalkToSlot(room, this.slot);
        const greeted = new Deferred<void>();
        void talk.serve((m) => {
          const message = m.text();
          if (m.procedureName === 'HIST') {
            this.view = message === '' ? [] : message.split('\n');
            greeted.resolve();
          } else {
            this.view.push(message);
          }
        });
        const ended = talk.done.then((status) => {
          throw new SwitchboardError(status);
        });
        ended.catch(() => {});
        await within(Promise.race([greeted.promise, ended]));
        this.joins++;
        void talk.done.then((status) => {
          if (!this.#leaving) {
            this.ends.push(status.code);
          }
          if (this.#talk === talk) {
            this.#talk = undefined;
          }
        });
        this.#talk = talk;
        return talk;
      } catch (e) {
        if (!rejoinable(e)) {
          throw e;
        }
        await delay(5);
      }
    }
  }

  async join(): Promise<void> {
    await this.#join();
  }

  async post(): Promise<void> {
    const message = `${this.name}:${++this.posted}`;
    for (;;) {
      const talk = this.#talk ?? (await this.#join());
      try {
        await talk.request('POST', message);
        return;
      } catch (e) {
        if (!rejoinable(e)) {
          throw e;
        }
        if (this.#talk === talk) {
          this.#talk = undefined;
        }
      }
    }
  }

  async leave(): Promise<void> {
    this.#leaving = true;
    await this.#talk?.close();
  }
}

afterAll(() => {
  expectNoMemoryListeners();
});

for (const scheme of ['tcp', 'mem'] as const) {
  describe(scheme, () => {
    test("a crashed server's rooms are reassigned at once; members follow the rooms through RELOCATED", async () => {
      const cluster = new Cluster(scheme);
      await cluster.start();
      const storage = new Map<number, string[]>();
      const server = async (id: number): Promise<MeshNode> => {
        const mesh = await cluster.member();
        await within(
          mesh.publishSharded(room, new RoomServer(storage), {
            count: rooms,
            shared: true,
            instance: id,
            trackChannels: false,
            forwardGrace: 200,
          }),
        );
        return mesh;
      };

      const observer = await cluster.member({ listen: false });
      const watch = new MigrationWatch(observer.client, room);
      const table = observer.client;
      const ownerOf = (slot: number): number => table.slotOwner(room, slot)?.owner ?? 0;
      const slotsOf = (id: number): number[] => table.slotTable(room)?.slotsOf(id) ?? [];
      const r1 = await server(0x41);
      await server(0x42);
      await server(0x43);
      await until(
        () =>
          watch.idle &&
          [0x41, 0x42, 0x43].every((id) => slotsOf(id).length >= Math.trunc(rooms / 3)),
        'the rooms spread over three servers',
      );

      // A member in every room, a second one in four rooms.
      const onR1 = slotsOf(0x41);
      const members = [
        ...Array.from(
          { length: rooms },
          (_, slot) => new Member(observer.switchboard, slot, `m${slot}`),
        ),
        ...Array.from(
          { length: 4 },
          (_, slot) => new Member(observer.switchboard, slot, `n${slot}`),
        ),
      ];
      for (const member of members) {
        await member.join();
      }
      let stop = false;
      let failure: unknown;
      const loops = members.map(async (member) => {
        try {
          while (!stop) {
            await member.post();
            await delay(10);
          }
        } catch (e) {
          failure ??= e;
          throw e;
        }
      });
      for (const loop of loops) {
        loop.catch(() => {});
      }
      /** Whether every member posted [n] more than [since] has; throws what stopped a member. */
      const postedMore = (n: number, since?: ReadonlyMap<string, number>): boolean => {
        if (failure !== undefined) {
          throw failure;
        }
        return members.every((m) => m.posted >= (since?.get(m.name) ?? 0) + n);
      };
      await until(() => postedMore(3));

      // The first server crashes: its rooms go to the others at once.
      const crashed = performance.now();
      await cluster.crash(r1);
      await until(
        () =>
          table.slotOwners(room).size === 2 &&
          Array.from({ length: rooms }, (_, s) => ownerOf(s)).every((o) => o !== 0),
        'every room has a new owner',
      );
      expect(performance.now() - crashed).toBeLessThan(10_000);
      expect(table.slotOwners(room)).toEqual(new Set([0x42, 0x43]));
      const posted = new Map(members.map((m) => [m.name, m.posted]));
      await until(() => postedMore(3, posted), 'members of the crashed server rejoined');
      for (const member of members) {
        if (onR1.includes(member.slot)) {
          expect(member.joins, member.name).toBe(2);
          expect(member.ends, member.name).toEqual([StatusCode.connectionLost]);
        }
      }

      // It comes back: rooms move to it one at a time, and members on their
      // old channels are told RELOCATED.
      const movesBefore = watch.moves.length;
      await server(0x41);
      await until(
        () => watch.idle && slotsOf(0x41).length >= 5,
        'rooms back on the restarted server',
      );
      expect(watch.maxConcurrent).toBe(1);
      const movedRooms = new Set(
        watch.moves
          .slice(movesBefore)
          .filter(([, , to]) => to === 0x41)
          .map(([slot]) => slot),
      );
      const followers = members.filter((m) => movedRooms.has(m.slot));
      expect(followers.length).toBeGreaterThan(0);
      for (const m of members) {
        posted.set(m.name, m.posted);
      }
      await until(() => postedMore(3, posted), 'members rejoined after the moves');
      stop = true;
      await within(Promise.all(loops));
      for (const m of followers) {
        expect(m.ends.at(-1), m.name).toBe(StatusCode.relocated);
      }

      // Every post is in its room once, in order; members see the room as it
      // is.
      await delay(20);
      for (const member of members) {
        const history = storage.get(member.slot) ?? [];
        expect(history.filter((message) => message.startsWith(`${member.name}:`))).toEqual(
          Array.from({ length: member.posted }, (_, n) => `${member.name}:${n + 1}`),
        );
        expect(member.view, member.name).toEqual(history);
        await member.leave();
      }
    });
  });
}
