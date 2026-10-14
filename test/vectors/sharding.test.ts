// Vectors from the wiki page "Polyverse Switchboard Test Vectors", section
// "Sharding payloads", and the Dart `test/vectors/sharding_vectors_test.dart`.
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
  hex: string;
  decode: (bytes: Uint8Array) => Encodable;
  check: (value: Encodable) => void;
}

function vector<T extends Encodable>(
  name: string,
  hex: string,
  decode: (bytes: Uint8Array) => T,
  check: (value: T) => void,
): Vector {
  return { name, hex, decode, check: check as (value: Encodable) => void };
}

const vectors: Vector[] = [
  vector(
    'SLOTS "kv", 1024 slots, managed, holder-only, capacity 1',
    '6B 76 00 00 00 00 00 00 00 04 00 00 00 00 01 00 00 00',
    SlotsRequest.decode,
    (r) => {
      expect(r.space.equals(new SlotSpace(kv, { count: 1024 }))).toBe(true);
      expect(r.capacity).toBe(1);
    },
  ),
  vector(
    'SLOTS "userq", 65536 slots, managed lazy, capacity 0',
    '75 73 65 72 71 00 00 00 00 00 01 00 00 01 00 00 00 00',
    SlotsRequest.decode,
    (r) => {
      expect(r.space.equals(new SlotSpace(userq, { count: 65536, lazy: true }))).toBe(true);
      expect(r.capacity).toBe(0);
    },
  ),
  vector(
    'SLOTSPC "room", 256 slots, managed, shared recovery',
    '72 6F 6F 6D 00 00 00 00 00 01 00 00 00 02',
    SlotSpace.decode,
    (v) => expect(v.equals(new SlotSpace(room, { count: 256, shared: true }))).toBe(true),
  ),
  vector(
    'SLOTSPC "discord", 16 slots, static',
    '64 69 73 63 6F 72 64 00 10 00 00 00 01 00',
    SlotSpace.decode,
    (v) =>
      expect(v.equals(new SlotSpace(discord, { count: 16, mode: SlotMode.static }))).toBe(true),
  ),
  vector(
    'HOLDING "kv", slots 3 and 258',
    '6B 76 00 00 00 00 00 00 02 00 00 00 03 00 00 00 02 01 00 00',
    HoldingRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slots).toEqual([3, 258]);
    },
  ),
  vector(
    'HOLDING response, discard slot 258',
    '01 00 00 00 02 01 00 00',
    HoldingResponse.decode,
    (r) => expect(r.discard).toEqual([258]),
  ),
  vector('HOLDING response, nothing to discard', '00 00 00 00', HoldingResponse.decode, (r) =>
    expect(r.discard).toEqual([]),
  ),
  vector(
    'CLAIM "discord" slot 5, no storage, epoch 0',
    '64 69 73 63 6F 72 64 00 05 00 00 00 00 00 00 00 00',
    ClaimRequest.decode,
    (r) => {
      expect(r.type.equals(discord)).toBe(true);
      expect(r.slot).toBe(5);
      expect(r.holding).toBe(false);
      expect(r.epoch).toBe(0);
    },
  ),
  vector(
    'CLAIM "kv" slot 7, holding, last epoch 9',
    '6B 76 00 00 00 00 00 00 07 00 00 00 01 09 00 00 00',
    ClaimRequest.decode,
    (r) => {
      expect(r.slot).toBe(7);
      expect(r.holding).toBe(true);
      expect(r.epoch).toBe(9);
    },
  ),
  vector('CLAIM response, epoch 7', '07 00 00 00', ClaimResponse.decode, (r) =>
    expect(r.epoch).toBe(7),
  ),
  vector(
    'RELEASE "kv" slot 7, keep storage',
    '6B 76 00 00 00 00 00 00 07 00 00 00 01',
    ReleaseRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slot).toBe(7);
      expect(r.keepStorage).toBe(true);
    },
  ),
  vector(
    'LOCATE "userq" slot 0x1234',
    '75 73 65 72 71 00 00 00 34 12 00 00',
    LocateRequest.decode,
    (r) => {
      expect(r.type.equals(userq)).toBe(true);
      expect(r.slot).toBe(0x1234);
    },
  ),
  vector(
    'LOCATE response, owned by 0x1A2B, epoch 7',
    '01 2B 1A 00 00 00 00 07 00 00 00',
    LocateResponse.decode,
    (r) => expect(r.equals(new LocateResponse(SlotState.owned, 0x1a2b, 7))).toBe(true),
  ),
  vector(
    'LOCATE response, free, epoch 3',
    '00 00 00 00 00 00 00 03 00 00 00',
    LocateResponse.decode,
    (r) => expect(r.equals(new LocateResponse(SlotState.free, 0, 3))).toBe(true),
  ),
  vector(
    'MIGRATE "zone" slot 4 to 0xB',
    '7A 6F 6E 65 00 00 00 00 04 00 00 00 0B 00 00 00 00 00',
    MigrateRequest.decode,
    (r) => {
      expect(r.type.equals(zone)).toBe(true);
      expect(r.slot).toBe(4);
      expect(r.to).toBe(0xb);
    },
  ),
  vector(
    'MIGRATE "zone" slot 4, allocator chooses',
    '7A 6F 6E 65 00 00 00 00 04 00 00 00 00 00 00 00 00 00',
    MigrateRequest.decode,
    (r) => expect(r.to).toBe(0),
  ),
  vector('PHASE draining', '01', PhaseItem.decode, (r) =>
    expect(r.phase).toBe(MigrationPhase.draining),
  ),
  vector('PHASE rolled back', '05', PhaseItem.decode, (r) =>
    expect(r.phase).toBe(MigrationPhase.rolledBack),
  ),
  vector(
    'ASSIGN "kv" slot 7, epoch 8, holder 2',
    '6B 76 00 00 00 00 00 00 07 00 00 00 08 00 00 00 02 00 00 00 00 00 00',
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
    'ASSIGN "room" slot 1, epoch 1, no holder, shared recovery',
    '72 6F 6F 6D 00 00 00 00 01 00 00 00 01 00 00 00 00 00 00 00 00 00 01',
    AssignRequest.decode,
    (r) => {
      expect(r.holder).toBe(0);
      expect(r.shared).toBe(true);
    },
  ),
  vector('ASSIGN response, holding (empty)', '', AssignResponse.decode, (r) =>
    expect(r.notHolding).toBe(false),
  ),
  vector('ASSIGN response, not holding', '01', AssignResponse.decode, (r) =>
    expect(r.notHolding).toBe(true),
  ),
  vector(
    'DRAIN "kv" slot 7, epoch 9, to 3',
    '6B 76 00 00 00 00 00 00 07 00 00 00 09 00 00 00 03 00 00 00 00 00',
    DrainRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slot).toBe(7);
      expect(r.epoch).toBe(9);
      expect(r.to).toBe(3);
    },
  ),
  vector(
    'FORWARD "kv" slot 7, epoch 9, to 3',
    '6B 76 00 00 00 00 00 00 07 00 00 00 09 00 00 00 03 00 00 00 00 00',
    ForwardRequest.decode,
    (r) => {
      expect(r.slot).toBe(7);
      expect(r.epoch).toBe(9);
      expect(r.to).toBe(3);
    },
  ),
  vector(
    'RESUME "kv" slot 7, epoch 8',
    '6B 76 00 00 00 00 00 00 07 00 00 00 08 00 00 00',
    ResumeRequest.decode,
    (r) => {
      expect(r.type.equals(kv)).toBe(true);
      expect(r.slot).toBe(7);
      expect(r.epoch).toBe(8);
    },
  ),
  vector(
    'SLOT "kv" slot 7, owned by 2, holder 2, epoch 8',
    '6B 76 00 00 00 00 00 00 07 00 00 00 01 02 00 00 00 00 00 00 00 00 00 ' +
      '00 00 02 00 00 00 00 00 08 00 00 00',
    SlotItem.decode,
    (v) =>
      expect(v.equals(new SlotItem(kv, 7, SlotEntry.owned(2, { holder: 2, epoch: 8 })))).toBe(true),
  ),
  vector(
    'SLOT "kv" slot 7, migrating from 2 to 3, holder 2, epoch 8',
    '6B 76 00 00 00 00 00 00 07 00 00 00 02 02 00 00 00 00 00 03 00 00 00 ' +
      '00 00 02 00 00 00 00 00 08 00 00 00',
    SlotItem.decode,
    (v) =>
      expect(
        v.equals(new SlotItem(kv, 7, SlotEntry.migrating(2, 3, { holder: 2, epoch: 8 }))),
      ).toBe(true),
  ),
  vector(
    'SLOT "kv" slot 7, free, holder 2, epoch 8',
    '6B 76 00 00 00 00 00 00 07 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ' +
      '00 00 02 00 00 00 00 00 08 00 00 00',
    SlotItem.decode,
    (v) =>
      expect(v.equals(new SlotItem(kv, 7, SlotEntry.free({ holder: 2, epoch: 8 })))).toBe(true),
  ),
  vector(
    'SLOT "x" slot 0xFFFFFFFF, owned by 0xFFFFFFFFFFFF, holder 0x123456789ABC, epoch 0xFFFFFFFF',
    '78 00 00 00 00 00 00 00 FF FF FF FF 01 FF FF FF FF FF FF 00 00 00 00 ' +
      '00 00 BC 9A 78 56 34 12 FF FF FF FF',
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

const negative: [string, string, (bytes: Uint8Array) => unknown][] = [
  ['SLOTSPC with mode 2', '6B 76 00 00 00 00 00 00 00 04 00 00 02 00', SlotSpace.decode],
  [
    'SLOT with state 3',
    '6B 76 00 00 00 00 00 00 07 00 00 00 03 02 00 00 00 00 00 00 00 00 00 ' +
      '00 00 02 00 00 00 00 00 08 00 00 00',
    SlotItem.decode,
  ],
  [
    'HOLDING announcing 2 slots, carrying 1',
    '6B 76 00 00 00 00 00 00 02 00 00 00 03 00 00 00',
    HoldingRequest.decode,
  ],
  ['PHASE 0', '00', PhaseItem.decode],
  ['PHASE 6', '06', PhaseItem.decode],
  ['LOCATE response with state 3', '03 00 00 00 00 00 00 00 00 00 00', LocateResponse.decode],
  ['CLAIM without epoch', '6B 76 00 00 00 00 00 00 07 00 00 00 01', ClaimRequest.decode],
];

describe('sharding payloads', () => {
  test.each(vectors.map((v) => [v.name, v] as const))('%s', (_name, v) => {
    const value = v.decode(hexBytes(v.hex));
    v.check(value);
    expect(hexString(value.encode())).toBe(hexString(hexBytes(v.hex)));
  });

  test.each(negative)('negative: %s', (_name, hex, decode) => {
    expect(() => decode(hexBytes(hex))).toThrow(ProtocolError);
  });
});

describe('codecs', () => {
  test('every structure: truncation is a protocol error, trailing bytes are ignored', () => {
    for (const v of vectors) {
      const bytes = hexBytes(v.hex);
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
    const space = SlotSpace.decode(hexBytes('6B 76 00 00 00 00 00 00 00 04 00 00 00 FE'));
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
    expect(hexString(moved.encode())).toBe('25 00 2B 1A 00 00 00 00 07 00 00 00');
    expect(MovedStatus.parse(hexBytes('25 00 2B 1A 00 00 00 00 07 00 00 00')).equals(moved)).toBe(
      true,
    );
    expect(moved.hasOwner).toBe(true);
    expect(moved.ownerOf(userq)?.equals(new ServiceAddress(userq, 0x1a2b))).toBe(true);
  });

  test('RELOCATED, owner and epoch', () => {
    const relocated = new MovedStatus({ owner: 0x1a2b, epoch: 7 });
    expect(hexString(relocated.encode({ relocated: true }))).toBe(
      '26 00 2B 1A 00 00 00 00 07 00 00 00',
    );
    expect(
      MovedStatus.parse(hexBytes('26 00 2B 1A 00 00 00 00 07 00 00 00')).equals(relocated),
    ).toBe(true);
    const status = relocated.toStatus({ relocated: true });
    expect(status.known).toBe(StatusCode.relocated);
    expect(MovedStatus.carriesFields(status)).toBe(true);
    // ABORTED has no fields.
    expect(
      MovedStatus.parse(hexBytes('0A 00 2B 1A 00 00 00 00 07 00 00 00')).equals(
        MovedStatus.unknown,
      ),
    ).toBe(true);
    expect(MovedStatus.carriesFields(Status.of(StatusCode.aborted))).toBe(false);
  });

  test('unknown', () => {
    expect(hexString(MovedStatus.unknown.encode())).toBe('25 00 00 00 00 00 00 00 00 00 00 00');
    expect(new MovedStatus().equals(MovedStatus.unknown)).toBe(true);
    expect(
      MovedStatus.parse(hexBytes('25 00 00 00 00 00 00 00 00 00 00 00')).equals(
        MovedStatus.unknown,
      ),
    ).toBe(true);
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
