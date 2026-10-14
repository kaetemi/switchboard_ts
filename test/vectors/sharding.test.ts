// Vectors from the wiki page "Polyverse Switchboard Test Vectors", section
// "Sharding payloads", and the Dart `test/vectors/sharding_vectors_test.dart`.
// The wiki's are read from test/vectors/wiki-vectors.json by id (through
// test/support/vectors.ts), so test/vectors/coverage.test.ts checks the
// sharding tables.
import { describe, expect, test } from 'vitest';
import { maxInstance, ServiceAddress } from '../../src/core/address/index.ts';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import { Name } from '../../src/core/name.ts';
import {
  AssignRequest,
  AssignResponse,
  ClaimRequest,
  ClaimResponse,
  DrainRequest,
  ForwardRequest,
  HoldingRequest,
  HoldingResponse,
  LocateRequest,
  LocateResponse,
  MigrateRequest,
  MigrationPhase,
  MovedStatus,
  maxU32,
  PhaseItem,
  Procedures,
  ReleaseRequest,
  ResumeRequest,
  SlotEntry,
  SlotItem,
  SlotMode,
  SlotSpace,
  SlotState,
  SlotsRequest,
} from '../../src/core/naming/index.ts';
import { ProtocolError, Status, StatusCode } from '../../src/core/status.ts';
import { type WikiVector, wikiBytes, wikiVector } from '../support/vectors.ts';

const kv = Name.of('kv');
const userq = Name.of('userq');
const room = Name.of('room');
const discord = Name.of('discord');
const zone = Name.of('zone');

interface Encodable {
  encode(): Uint8Array;
}

interface Vector {
  name: string;
  bytes: Uint8Array;
  decode: (bytes: Uint8Array) => Encodable;
  check: (value: Encodable) => void;
}

/** Wiki vector [v] (table sharding-payloads#12), its decoder, and what the decoded value must be. */
function vector<T extends Encodable>(
  v: WikiVector,
  decode: (bytes: Uint8Array) => T,
  check: (value: T) => void,
): Vector {
  return {
    name: v.case,
    bytes: hexBytes(v.hex ?? ''),
    decode,
    check: check as (value: Encodable) => void,
  };
}

const vectors: Vector[] = [
  vector(
    wikiVector('test-vectors/sharding-payloads/slots-kv-1024-slots-managed-holder-only-capacity-1'),

    SlotsRequest.decode,
    (r) => {
      expect(r.space.equals(new SlotSpace(kv, { count: 1024 }))).toBe(true);
      expect(r.capacity).toBe(1);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/slots-userq-65536-slots-managed-lazy-capacity-0'),

    SlotsRequest.decode,
    (r) => {
      expect(r.space.equals(new SlotSpace(userq, { count: 65536, lazy: true }))).toBe(true);
      expect(r.capacity).toBe(0);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/slotspc-room-256-slots-managed-shared-recovery'),

    SlotSpace.decode,
    (v) => expect(v.equals(new SlotSpace(room, { count: 256, shared: true }))).toBe(true),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/slotspc-discord-16-slots-static'),

    SlotSpace.decode,
    (v) =>
      expect(v.equals(new SlotSpace(discord, { count: 16, mode: SlotMode.static }))).toBe(true),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/holding-kv-slots-3-and-258'),

    HoldingRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slots).toEqual([3, 258]);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/holding-response-discard-slot-258'),

    HoldingResponse.decode,
    (r) => expect(r.discard).toEqual([258]),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/holding-response-nothing-to-discard'),
    HoldingResponse.decode,
    (r) => expect(r.discard).toEqual([]),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/claim-discord-slot-5-no-storage-epoch-0'),

    ClaimRequest.decode,
    (r) => {
      expect(r.type.equals(discord)).toBe(true);
      expect(r.slot).toBe(5);
      expect(r.holding).toBe(false);
      expect(r.epoch).toBe(0);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/claim-kv-slot-7-holding-last-epoch-9'),

    ClaimRequest.decode,
    (r) => {
      expect(r.slot).toBe(7);
      expect(r.holding).toBe(true);
      expect(r.epoch).toBe(9);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/claim-response-epoch-7'),
    ClaimResponse.decode,
    (r) => expect(r.epoch).toBe(7),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/release-kv-slot-7-keep-storage'),

    ReleaseRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slot).toBe(7);
      expect(r.keepStorage).toBe(true);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/locate-userq-slot-0x1234'),

    LocateRequest.decode,
    (r) => {
      expect(r.type.equals(userq)).toBe(true);
      expect(r.slot).toBe(0x1234);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/locate-response-owned-by-0x1a2b-epoch-7'),

    LocateResponse.decode,
    (r) => expect(r.equals(new LocateResponse(SlotState.owned, 0x1a2b, 7))).toBe(true),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/locate-response-free-epoch-3'),

    LocateResponse.decode,
    (r) => expect(r.equals(new LocateResponse(SlotState.free, 0, 3))).toBe(true),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/migrate-zone-slot-4-to-0xb'),

    MigrateRequest.decode,
    (r) => {
      expect(r.type.equals(zone)).toBe(true);
      expect(r.slot).toBe(4);
      expect(r.to).toBe(0xb);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/migrate-zone-slot-4-allocator-chooses'),

    MigrateRequest.decode,
    (r) => expect(r.to).toBe(0),
  ),
  vector(wikiVector('test-vectors/sharding-payloads/phase-draining'), PhaseItem.decode, (r) =>
    expect(r.phase).toBe(MigrationPhase.draining),
  ),
  vector(wikiVector('test-vectors/sharding-payloads/phase-rolled-back'), PhaseItem.decode, (r) =>
    expect(r.phase).toBe(MigrationPhase.rolledBack),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/assign-kv-slot-7-epoch-8-holder-2'),

    AssignRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slot).toBe(7);
      expect(r.epoch).toBe(8);
      expect(r.holder).toBe(2);
      expect(r.shared).toBe(false);
    },
  ),
  vector(
    wikiVector(
      'test-vectors/sharding-payloads/assign-room-slot-1-epoch-1-no-holder-shared-recovery',
    ),

    AssignRequest.decode,
    (r) => {
      expect(r.holder).toBe(0);
      expect(r.shared).toBe(true);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/assign-response-holding'),
    AssignResponse.decode,
    (r) => expect(r.notHolding).toBe(false),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/assign-response-not-holding'),
    AssignResponse.decode,
    (r) => expect(r.notHolding).toBe(true),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/drain-kv-slot-7-epoch-9-to-3'),

    DrainRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slot).toBe(7);
      expect(r.epoch).toBe(9);
      expect(r.to).toBe(3);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/forward-kv-slot-7-epoch-9-to-3'),

    ForwardRequest.decode,
    (r) => {
      expect(r.slot).toBe(7);
      expect(r.epoch).toBe(9);
      expect(r.to).toBe(3);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/resume-kv-slot-7-epoch-8'),

    ResumeRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slot).toBe(7);
      expect(r.epoch).toBe(8);
    },
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/slot-kv-slot-7-owned-by-2-holder-2-epoch-8'),

    SlotItem.decode,
    (v) =>
      expect(v.equals(new SlotItem(kv, 7, SlotEntry.owned(2, { holder: 2, epoch: 8 })))).toBe(true),
  ),
  vector(
    wikiVector(
      'test-vectors/sharding-payloads/slot-kv-slot-7-migrating-from-2-to-3-holder-2-epoch-8',
    ),

    SlotItem.decode,
    (v) =>
      expect(
        v.equals(new SlotItem(kv, 7, SlotEntry.migrating(2, 3, { holder: 2, epoch: 8 }))),
      ).toBe(true),
  ),
  vector(
    wikiVector('test-vectors/sharding-payloads/slot-kv-slot-7-free-holder-2-epoch-8'),

    SlotItem.decode,
    (v) =>
      expect(v.equals(new SlotItem(kv, 7, SlotEntry.free({ holder: 2, epoch: 8 })))).toBe(true),
  ),
  vector(
    wikiVector(
      'test-vectors/sharding-payloads/slot-x-slot-0xffffffff-owned-by-0xffffffffffff-holder-0x123456789abc-epoch-0xfff',
    ),

    SlotItem.decode,
    (v) =>
      expect(
        v.equals(
          new SlotItem(
            'x',
            0xffffffff,
            SlotEntry.owned(0xffffffffffff, { holder: 0x123456789abc, epoch: 0xffffffff }),
          ),
        ),
      ).toBe(true),
  ),
];

/** The negative wiki vectors (table sharding-payloads#13), each with its decoder. */
const negative: [WikiVector, (bytes: Uint8Array) => unknown][] = [
  [wikiVector('test-vectors/sharding-payloads/slotspc-with-mode-2'), SlotSpace.decode],
  [wikiVector('test-vectors/sharding-payloads/slot-with-state-3'), SlotItem.decode],
  [
    wikiVector('test-vectors/sharding-payloads/holding-announcing-2-slots-carrying-1'),
    HoldingRequest.decode,
  ],
  [wikiVector('test-vectors/sharding-payloads/phase-0'), PhaseItem.decode],
  [wikiVector('test-vectors/sharding-payloads/phase-6'), PhaseItem.decode],
  [
    wikiVector('test-vectors/sharding-payloads/locate-response-with-state-3'),
    LocateResponse.decode,
  ],
  [
    wikiVector(
      'test-vectors/sharding-payloads/claim-without-epoch-wiki-layout-before-the-amendment',
    ),
    ClaimRequest.decode,
  ],
];

describe('sharding payloads', () => {
  test.each(vectors.map((v) => [v.name, v] as const))('%s', (_name, v) => {
    const value = v.decode(v.bytes);
    v.check(value);
    expect(hexString(value.encode())).toBe(hexString(v.bytes));
  });

  test.each(negative.map(([v, decode]) => [v.case, v.id, decode] as const))(
    'negative: %s',
    (_name, id, decode) => {
      expect(() => decode(wikiBytes(id))).toThrow(ProtocolError);
    },
  );
});

describe('codecs', () => {
  test('every structure: truncation is a protocol error, trailing bytes are ignored', () => {
    for (const v of vectors) {
      const bytes = v.bytes;
      // An ASSIGN response is a flags byte or nothing: any prefix is valid.
      if (bytes.length === 0 || v.name.startsWith('ASSIGN response')) {
        continue;
      }
      for (let n = 0; n < bytes.length; n++) {
        expect(() => v.decode(bytes.subarray(0, n)), `${v.name}, prefix ${n}`).toThrow(
          ProtocolError,
        );
      }
      const longer = Uint8Array.of(...bytes, 0xee, 0x01);
      expect(hexString(v.decode(longer).encode()), v.name).toBe(hexString(bytes));
    }
  });

  test('unknown flag bits are ignored', () => {
    // The wiki's paragraph: flags FE on a SLOTSPC, shared recovery only.
    const space = SlotSpace.decode(
      wikiBytes('test-vectors/sharding-payloads/prose-6b-76-00-00-00-00-00-00-00-04-00-00-00-fe'),
    );
    expect(space.lazy).toBe(false);
    expect(space.shared).toBe(true);
    expect(AssignResponse.decode(hexBytes('FE')).notHolding).toBe(false);
    expect(
      ClaimRequest.decode(hexBytes('6B 76 00 00 00 00 00 00 07 00 00 00 FE 00 00 00 00')).holding,
    ).toBe(false);
  });

  test('a slot list longer than the payload is refused before reading', () => {
    expect(() => HoldingResponse.decode(hexBytes('FF FF FF FF 01 00 00 00'))).toThrow(
      ProtocolError,
    );
  });

  test('values outside u32 and u48 are refused on encode', () => {
    expect(() => new ClaimRequest(kv, 0x100000000).encode()).toThrow(RangeError);
    expect(() => new SlotItem(kv, 1, SlotEntry.owned(0x1000000000000)).encode()).toThrow(
      RangeError,
    );
    expect(() => new ClaimResponse(-1).encode()).toThrow(RangeError);
  });

  test('procedure names', () => {
    expect(
      [
        Procedures.slots,
        Procedures.holding,
        Procedures.claim,
        Procedures.release,
        Procedures.locate,
        Procedures.migrate,
        Procedures.phase,
        Procedures.assign,
        Procedures.drain,
        Procedures.forward,
        Procedures.resume,
        Procedures.slotSpace,
        Procedures.slot,
      ].map((n) => n.toString()),
    ).toEqual([
      'SLOTS',
      'HOLDING',
      'CLAIM',
      'RELEASE',
      'LOCATE',
      'MIGRATE',
      'PHASE',
      'ASSIGN',
      'DRAIN',
      'FORWARD',
      'RESUME',
      'SLOTSPC',
      'SLOT',
    ]);
    expect(hexString(Procedures.slotSpace.bytes)).toBe('53 4C 4F 54 53 50 43 00');
  });

  test('status codes', () => {
    expect(StatusCode.moved).toBe(37);
    expect(StatusCode.relocated).toBe(38);
    expect(new Status(39).known).toBeUndefined();
  });
});

describe('MOVED and RELOCATED status', () => {
  test('owner and epoch', () => {
    const moved = new MovedStatus({ owner: 0x1a2b, epoch: 7 });
    const wire = wikiBytes('test-vectors/sharding-payloads/moved-owner-0x1a2b-epoch-7');
    expect(hexString(moved.encode())).toBe(hexString(wire));
    expect(MovedStatus.parse(wire).equals(moved)).toBe(true);
    expect(moved.hasOwner).toBe(true);
    expect(moved.ownerOf(userq)?.equals(new ServiceAddress(userq, 0x1a2b))).toBe(true);
  });

  test('RELOCATED, owner and epoch', () => {
    const relocated = new MovedStatus({ owner: 0x1a2b, epoch: 7 });
    const wire = wikiBytes('test-vectors/sharding-payloads/relocated-owner-0x1a2b-epoch-7');
    expect(hexString(relocated.encode({ relocated: true }))).toBe(hexString(wire));
    expect(MovedStatus.parse(wire).equals(relocated)).toBe(true);
    const status = relocated.toStatus({ relocated: true });
    expect(status.known).toBe(StatusCode.relocated);
    expect(MovedStatus.carriesFields(status)).toBe(true);
    // ABORTED has no fields (the wiki's paragraph).
    expect(
      MovedStatus.parse(
        wikiBytes('test-vectors/sharding-payloads/prose-0a-00-2b-1a-00-00-00-00-07-00-00-00'),
      ).equals(MovedStatus.unknown),
    ).toBe(true);
    expect(MovedStatus.carriesFields(Status.of(StatusCode.aborted))).toBe(false);
  });

  test('unknown', () => {
    const wire = wikiBytes('test-vectors/sharding-payloads/moved-owner-and-epoch-unknown');
    expect(hexString(MovedStatus.unknown.encode())).toBe(hexString(wire));
    expect(new MovedStatus().equals(MovedStatus.unknown)).toBe(true);
    expect(MovedStatus.parse(wire).equals(MovedStatus.unknown)).toBe(true);
    expect(MovedStatus.unknown.hasOwner).toBe(false);
    expect(MovedStatus.unknown.ownerOf(userq)).toBeUndefined();
  });

  test('largest values, a reason, and RELOCATED', () => {
    const moved = new MovedStatus({ owner: maxInstance, epoch: maxU32, reason: 'é' });
    const hex = 'FF FF FF FF FF FF FF FF FF FF C3 A9';
    expect(hexString(moved.encode())).toBe(`25 00 ${hex}`);
    expect(hexString(moved.encode({ relocated: true }))).toBe(`26 00 ${hex}`);
    expect(MovedStatus.parse(hexBytes(`25 00 ${hex}`)).equals(moved)).toBe(true);
    expect(MovedStatus.parse(hexBytes(`26 00 ${hex}`)).equals(moved)).toBe(true);
    expect(() => new MovedStatus({ owner: maxInstance + 1 })).toThrow(RangeError);
    expect(() => new MovedStatus({ epoch: maxU32 + 1 })).toThrow(RangeError);
  });

  test('through Status, losslessly', () => {
    const moved = new MovedStatus({ owner: 0xfedcba987654, epoch: 0x80000001 });
    for (const relocated of [false, true]) {
      const status = moved.toStatus({ relocated });
      expect(status.known).toBe(relocated ? StatusCode.relocated : StatusCode.moved);
      expect(hexString(status.encode())).toBe(hexString(moved.encode({ relocated })));
      expect(MovedStatus.fromStatus(status).equals(moved)).toBe(true);
      // A status decoded from the wire re-encodes byte for byte.
      const relayed = Status.decode(status.encode());
      expect(hexString(relayed.encode())).toBe(hexString(moved.encode({ relocated })));
      expect(relayed.equals(status)).toBe(true);
      expect(MovedStatus.fromStatus(relayed).equals(moved)).toBe(true);
    }
    expect(hexString(moved.encode())).toBe('25 00 54 76 98 BA DC FE 01 00 00 80');
    expect(MovedStatus.fromStatus(Status.of(StatusCode.moved)).equals(MovedStatus.unknown)).toBe(
      true,
    );
    const elsewhere = moved.encode();
    elsewhere[0] = StatusCode.unavailable;
    expect(MovedStatus.fromStatus(Status.decode(elsewhere)).equals(MovedStatus.unknown)).toBe(true);
  });

  test('hostile input never throws', () => {
    for (const hex of [
      '',
      '25',
      '25 00',
      '25 00 2B 1A 00 00 00 00 07 00 00',
      '26 00 2B 1A 00 00 00 00 07 00 00',
      '0E 00 2B 1A 00 00 00 00 07 00 00 00',
      '0A 00 2B 1A 00 00 00 00 07 00 00 00',
      '25 01 2B 1A 00 00 00 00 07 00 00 00',
      '27 00 2B 1A 00 00 00 00 07 00 00 00',
    ]) {
      expect(MovedStatus.parse(hexBytes(hex)).equals(MovedStatus.unknown), hex).toBe(true);
    }
    expect(
      MovedStatus.parse(hexBytes('25 00 2B 1A 00 00 00 00 07 00 00 00 FF')).equals(
        new MovedStatus({ owner: 0x1a2b, epoch: 7, reason: '�' }),
      ),
    ).toBe(true);
  });
});
