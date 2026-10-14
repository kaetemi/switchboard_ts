/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The codec oracle: the Dart reference's decoders, through the Dart peer.
//
// First the wiki vectors against the reference: every vector of the tables
// the oracle has a codec for decodes (or is refused) the way the wiki says,
// which checks the extracted vector file as much as the reference. Then the
// TS codecs against the reference (names, statuses, addresses, mux frames
// and control messages so far), on the vectors and on random and mutated
// inputs: both must accept or refuse the same bytes and, when they accept,
// decode the same fields and re-encode the same bytes. SWITCHBOARD_FUZZ_SEED
// and SWITCHBOARD_FUZZ_COUNT vary the random inputs (the seed is in the test
// names, so a failure can be replayed).

import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/channel-address.ts';
import { concatBytes, hexBytes, hexString } from '../../src/core/bytes.ts';
import {
  MuxControlMessage,
  MuxControlType,
  MuxCredit,
  MuxFrame,
  MuxIdent,
  MuxLimits,
  muxControlTypeName,
} from '../../src/core/mux/index.ts';
import { Name } from '../../src/core/name.ts';
import { Status } from '../../src/core/status.ts';
import {
  type WikiVector,
  wikiBytes,
  wikiVector,
  wikiVectors,
  wikiVectorsIn,
} from '../support/vectors.ts';
import type { Peer } from './launcher.ts';
import { startPeer, unavailable } from './peers.ts';
import { le } from './probe/raw.ts';
import type {
  ControlFields,
  DecodedFields,
  DecodeResult,
  MuxFields,
  NameFields,
  StatusFields,
  TypedControlFields,
} from './protocol.ts';

const dartMissing = unavailable('dart');
const { SWITCHBOARD_FUZZ_SEED, SWITCHBOARD_FUZZ_COUNT } = process.env;
const seed = Number(SWITCHBOARD_FUZZ_SEED ?? 0x5357424e);
const count = Number(SWITCHBOARD_FUZZ_COUNT ?? 3000);

let dart: Peer;

type Fields = Record<string, unknown>;

async function decode<T extends DecodedFields = DecodedFields>(
  kind: string,
  hex: string,
): Promise<DecodeResult<T>> {
  const [r] = await decodeMany<T>(kind, [hex]);
  if (r === undefined) {
    throw new Error('decodeMany answered nothing');
  }
  return r;
}

async function decodeMany<T extends DecodedFields = DecodedFields>(
  kind: string,
  hexes: readonly string[],
): Promise<DecodeResult<T>[]> {
  const out: DecodeResult<T>[] = [];
  for (let i = 0; i < hexes.length; i += 500) {
    out.push(
      ...(await dart.rpc<DecodeResult<T>[]>('decodeMany', {
        kind,
        hexes: hexes.slice(i, i + 500),
      })),
    );
  }
  return out;
}

/**
 * Decodes [v] with the reference: refused when negative (no fields), else
 * re-encoded to itself.
 */
async function expectReference<T extends DecodedFields = DecodedFields>(
  kind: string,
  v: WikiVector,
  hex = v.hex,
): Promise<T | undefined> {
  if (hex === null) {
    throw new Error(`${v.id} has no single value`);
  }
  const r = await decode<T>(kind, hex);
  if (v.negative) {
    expect(r.ok, `${v.id} must be refused`).toBe(false);
    return undefined;
  }
  if (!r.ok) {
    throw new Error(`${v.id}: the reference refuses it: ${r.error.message}`);
  }
  expect(r.value.reencoded, v.id).toBe(hex);
  return r.value;
}

/** The typed payload fields of a decoded control message. */
function typedFields(r: DecodeResult<ControlFields>): TypedControlFields | null | undefined {
  return r.ok ? r.value.fields : undefined;
}

const tables = (prefix: string): WikiVector[] => wikiVectorsIn(prefix).filter((v) => !v.prose);

/** The control payloads the wiki says must be refused (its prose after the frame tables). */
const refusedControlPayloads = wikiVectors(
  'test-vectors/mux-frames/prose-00-04-00-00-04-00-00-00',
  'test-vectors/mux-frames/prose-00-04-00-00-04-00-00-00-00-00-00-00',
  'test-vectors/mux-frames/prose-02-00-00-00-00-00-00-80',
  'test-vectors/mux-frames/prose-56',
  'test-vectors/mux-frames/prose-56-00-01-01',
  'test-vectors/mux-frames/prose-02-00-aa-bb',
  'test-vectors/mux-frames/prose-00-00-05-01-02',
  'test-vectors/mux-frames/prose-00-00-41',
  'test-vectors/mux-frames/prose-00-00-01-aa',
  'test-vectors/mux-frames/prose-00-00-00-04-61-62',
);

/** A refused control payload as a whole control message: its type byte first. */
function controlMessageOf(v: WikiVector): string {
  // The payloads are LIMITS, CREDIT or IDENT ones.
  const type = /LIMITS/.test(v.case) ? '04' : /CREDIT/.test(v.case) ? '07' : '06';
  // `00 00 41` is followed by 65 bytes of intent and a receiver length.
  const tail = v.hex === '00 00 41' ? ` ${Array(66).fill('00').join(' ')}` : '';
  return `${type} ${v.hex}${tail}`;
}

/** The control messages of the wiki's frame vectors: the payloads of DATA on channel 0. */
const controlMessages = (): string[] =>
  tables('test-vectors/mux-frames/')
    .filter((v) => !v.negative && v.hex?.startsWith('02 00 00 '))
    .map((v) => (v.hex ?? '').slice(9));

describe.skipIf(dartMissing !== undefined)(
  `codec oracle${dartMissing === undefined ? '' : ` (skipped: dart peer: ${dartMissing})`}`,
  () => {
    beforeAll(async () => {
      dart = await startPeer('dart', { config: { listen: [] } });
    });
    afterAll(async () => {
      await dart?.stop();
    });

    describe('the reference decodes the wiki vectors as the wiki says', () => {
      test('stream binding: the preamble, then a frame', async () => {
        const preamble = wikiVector('test-vectors/transport-stream-binding/preamble-version-1');
        const frame = wikiVector(
          'test-vectors/transport-stream-binding/frame-carrying-body-02-02-00-aa',
        );
        expect(await decode('stream', preamble.hex ?? '')).toEqual({
          ok: true,
          value: { preamble: true, frames: [], buffered: 0 },
        });
        expect(await decode('stream', `${preamble.hex} ${frame.hex}`)).toEqual({
          ok: true,
          value: { preamble: true, frames: ['02 02 00 AA'], buffered: 0 },
        });
        expect((await decode('stream', frame.hex ?? '')).ok, 'a frame before the preamble').toBe(
          false,
        );
      });

      test.each(tables('test-vectors/mux-frames/'))('mux: $case', async (v) => {
        const fields = await expectReference<MuxFields>('mux', v);
        if (fields?.channelId === 0) {
          // Control messages decode again as such, typed payload included.
          const control = await decode<ControlFields>('control', fields.payload);
          expect(control.ok, v.id).toBe(true);
          expect(typedFields(control)?.error, v.id).toBeUndefined();
        }
      });

      test('mux: compatible reserved bits are ignored', async () => {
        const v = wikiVector('test-vectors/mux-frames/prose-42-02-00-01');
        expect(await decode('mux', v.hex ?? '')).toMatchObject({
          ok: true,
          value: { command: 'data', channelId: 2, payload: '01', reencoded: '02 02 00 01' },
        });
      });

      test.each(refusedControlPayloads)('control payload refused: $case', async (v) => {
        const r = await decode<ControlFields>('control', controlMessageOf(v));
        expect(r.ok, v.id).toBe(true);
        expect(typedFields(r)?.error, v.id).toBeDefined();
      });

      test.each(tables('test-vectors/talk-messages/'))('talk: $case', async (v) => {
        await expectReference('talk', v);
      });

      test.each(tables('test-vectors/address-headers/').filter((v) => v.hex !== null))(
        'address: $case',
        async (v) => {
          await expectReference('address', v);
        },
      );

      test('address: an empty OPEN payload is the default service', async () => {
        const v = wikiVector('test-vectors/address-headers/empty-open-payload-zero-bytes');
        expect(v.hex).toBeNull();
        expect(await decode('address', '')).toMatchObject({
          ok: true,
          value: { type: null, instance: 0, shard: null, host: null, payload: '' },
        });
      });

      test.each(wikiVectorsIn('test-vectors/names/'))('name: $case', async (v) => {
        const fields = await expectReference<NameFields>('name', v);
        if (fields !== undefined) {
          expect(fields.text, v.id).toBe(v.case.replace(/\s*\(.*\)$/, '').trim());
        }
      });

      test.each(
        wikiVectors(
          'test-vectors/sharding-payloads/moved-owner-0x1a2b-epoch-7',
          'test-vectors/sharding-payloads/moved-owner-and-epoch-unknown',
          'test-vectors/sharding-payloads/relocated-owner-0x1a2b-epoch-7',
          'test-vectors/sharding-payloads/prose-0a-00-2b-1a-00-00-00-00-07-00-00-00',
        ),
      )('status: $case', async (v) => {
        const fields = await expectReference<StatusFields>('status', v);
        expect(fields?.body, 'the fields after the code are kept as they are').toBe(
          (v.hex ?? '').slice(6),
        );
      });
    });

    describe('TS and the reference decode alike', () => {
      test.each(wikiVectorsIn('test-vectors/names/'))('name vector: $case', async (v) => {
        await expectAgreement('name', [v.hex ?? ''], tsName);
      });

      test.each(wikiVectorsIn('test-vectors/address-headers/').filter((v) => v.hex !== null))(
        'address vector: $case',
        async (v) => {
          await expectAgreement('address', [v.hex ?? ''], tsAddress);
        },
      );

      test.each(wikiVectorsIn('test-vectors/mux-frames/').filter((v) => v.hex !== null))(
        'mux frame vector: $case',
        async (v) => {
          await expectAgreement('mux', [v.hex ?? ''], tsMux);
        },
      );

      test('control vectors: the messages on channel 0 and the refused payloads', async () => {
        const inputs = [...controlMessages(), ...refusedControlPayloads.map(controlMessageOf)];
        await expectAgreement('control', inputs, tsControl, sameControlVerdicts);
      });

      test('status vectors: the CLOSE and GOAWAY payloads and the sharding statuses', async () => {
        const payloads = [
          '', // CLOSE without a payload
          hexString(
            wikiBytes('test-vectors/mux-frames/close-short-id-2-status-ok-no-reason').slice(3),
          ),
          hexString(
            wikiBytes('test-vectors/mux-frames/close-short-id-2-not-found-reason-x').slice(3),
          ),
          hexString(
            wikiBytes('test-vectors/mux-frames/control-goaway-going-away-no-reason').slice(4),
          ),
          hexString(wikiBytes('test-vectors/sharding-payloads/moved-owner-0x1a2b-epoch-7')),
          hexString(wikiBytes('test-vectors/sharding-payloads/relocated-owner-0x1a2b-epoch-7')),
        ];
        await expectAgreement('status', payloads, tsStatus);
      });

      test(`names, ${count} random inputs, seed ${seed}`, async () => {
        const rand = random(seed);
        await expectAgreement(
          'name',
          Array.from({ length: count }, () => randomName(rand)),
          tsName,
        );
      });

      test(`statuses, ${count} random inputs, seed ${seed}`, async () => {
        const rand = random(seed + 1);
        await expectAgreement(
          'status',
          Array.from({ length: count }, () => randomStatus(rand)),
          tsStatus,
        );
      });

      test(`addresses, ${count} random and mutated inputs, seed ${seed}`, async () => {
        const rand = random(seed + 2);
        const seeds = wikiVectorsIn('test-vectors/address-headers/')
          .map((v) => v.hex)
          .filter((h) => h !== null);
        const inputs = Array.from({ length: count }, () =>
          rand() < 0.5 ? randomAddress(rand) : mutate(rand, hexBytes(pick(rand, seeds))),
        );
        await expectAgreement('address', inputs.map(hexString), tsAddress);
      });

      test(`mux frames, ${count} random and mutated inputs, seed ${seed}`, async () => {
        const rand = random(seed + 4);
        const seeds = tables('test-vectors/mux-frames/')
          .map((v) => v.hex)
          .filter((h) => h !== null);
        const inputs = Array.from({ length: count }, () =>
          rand() < 0.5 ? randomMuxFrame(rand) : mutate(rand, hexBytes(pick(rand, seeds))),
        );
        await expectAgreement('mux', inputs.map(hexString), tsMux);
      });

      test(`control messages, ${count} random and mutated inputs, seed ${seed}`, async () => {
        const rand = random(seed + 5);
        const seeds = controlMessages();
        const inputs = Array.from({ length: count }, () =>
          rand() < 0.5 ? randomControl(rand) : mutate(rand, hexBytes(pick(rand, seeds))),
        );
        await expectAgreement('control', inputs.map(hexString), tsControl, sameControlVerdicts);
      });

      test(`addresses the TS side encodes, the reference encodes alike, seed ${seed}`, async () => {
        const rand = random(seed + 3);
        for (let i = 0; i < 200; i++) {
          const a = ChannelAddress.decode(randomAddress(rand));
          const fields = {
            type: a.type === undefined ? null : hexString(a.type.bytes),
            instance: a.instance,
            shard: a.shard ?? null,
            host: a.host ?? null,
            payload: hexString(a.payload),
          };
          expect(await dart.rpc('encode', { kind: 'address', fields })).toBe(hexString(a.encode()));
        }
      });
    });
  },
);

/** The TS codec's canonical fields, as the Dart oracle writes them. */
type TsDecoder = (bytes: Uint8Array) => Fields;

const tsName: TsDecoder = (bytes) => {
  const n = Name.fromBytes(bytes);
  return { text: n.toString(), length: n.length, reencoded: hexString(n.bytes) };
};

const tsStatus: TsDecoder = (bytes) => {
  const s = Status.decode(bytes);
  const encoded = s.encode();
  return {
    code: s.code,
    reason: s.reason,
    body: hexString(encoded.subarray(2)),
    reencoded: hexString(encoded),
  };
};

const tsAddress: TsDecoder = (bytes) => {
  const a = ChannelAddress.decode(bytes);
  return {
    type: a.type === undefined ? null : hexString(a.type.bytes),
    instance: a.instance,
    shard: a.shard ?? null,
    host: a.host ?? null,
    payload: hexString(a.payload),
    reencoded: hexString(a.encode()),
  };
};

const muxCommandNames = ['data', 'open', 'close'] as const;

const tsMux: TsDecoder = (bytes) => {
  const f = MuxFrame.decode(bytes);
  return {
    command: muxCommandNames[f.command],
    channelId: f.channelId,
    longForm: f.longForm,
    payload: hexString(f.payload),
    reencoded: hexString(f.encode()),
  };
};

const tsControl: TsDecoder = (bytes) => {
  const m = MuxControlMessage.decode(bytes);
  return {
    type: m.type,
    typeName: muxControlTypeName(m.type) ?? null,
    payload: hexString(m.payload),
    fields: tsControlFields(m),
    reencoded: hexString(m.encode()),
  };
};

/** The typed payload of LIMITS, CREDIT, GOAWAY and IDENT, as the oracle's `fields`. */
function tsControlFields(m: MuxControlMessage): Fields | null {
  try {
    switch (m.type) {
      case MuxControlType.limits: {
        const l = MuxLimits.decode(m.payload);
        return {
          maxFrameSize: l.maxFrameSize,
          maxChannels: l.maxChannels,
          initialWindow: l.initialWindow,
        };
      }
      case MuxControlType.credit: {
        const c = MuxCredit.decode(m.payload);
        return { channelId: c.channelId, bytes: c.bytes };
      }
      case MuxControlType.goAway: {
        const s = m.goAwayStatus;
        return { code: s.code, reason: s.reason };
      }
      case MuxControlType.ident: {
        const i = MuxIdent.decode(m.payload);
        return {
          credential: hexString(i.credential),
          intent: hexString(i.intent),
          receiver: hexString(i.receiver),
          proof: hexString(i.proof),
        };
      }
      default:
        return null;
    }
  } catch (e) {
    return { error: e instanceof Error ? e.name : String(e) };
  }
}

/**
 * Control fields with a typed payload that does not decode compare by that
 * verdict alone: the error classes and messages differ between the
 * implementations.
 */
function sameControlVerdicts(value: DecodedFields): DecodedFields {
  const { fields } = value;
  return typeof fields === 'object' && fields !== null && 'error' in fields
    ? { ...value, fields: { error: true } }
    : value;
}

/**
 * Decodes every input with both codecs and expects the same verdicts and,
 * where both accept, the same fields (both passed through [normalize]).
 * Lists every disagreement.
 */
async function expectAgreement(
  kind: string,
  hexes: readonly string[],
  ts: TsDecoder,
  normalize: (value: DecodedFields) => DecodedFields = (value) => value,
): Promise<void> {
  const reference = await decodeMany(kind, hexes);
  const disagreements: string[] = [];
  hexes.forEach((hex, i) => {
    const d = reference[i];
    let t: { ok: true; value: Fields } | { ok: false; error: string };
    try {
      t = { ok: true, value: ts(hexBytes(hex)) };
    } catch (e) {
      t = { ok: false, error: e instanceof Error ? `${e.name}: ${e.message}` : String(e) };
    }
    if (d === undefined) {
      disagreements.push(`[${hex}] no reference answer`);
    } else if (d.ok !== t.ok) {
      const dart = d.ok ? 'accepts' : `refuses (${d.error.message})`;
      const own = t.ok ? 'accepts' : `refuses (${t.error})`;
      disagreements.push(`[${hex}] Dart ${dart}, TS ${own}`);
    } else if (d.ok && t.ok) {
      const dart = JSON.stringify(normalize(d.value));
      const own = JSON.stringify(normalize(t.value));
      if (dart !== own) {
        disagreements.push(`[${hex}] Dart ${dart}, TS ${own}`);
      }
    }
  });
  expect(disagreements.slice(0, 20), `${disagreements.length} of ${hexes.length} disagree`).toEqual(
    [],
  );
}

/** A deterministic generator of numbers in [0, 1) (mulberry32). */
function random(state: number): () => number {
  let s = state >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function int(rand: () => number, below: number): number {
  return Math.floor(rand() * below);
}

function pick<T>(rand: () => number, items: readonly T[]): T {
  return items[int(rand, items.length)] as T;
}

const textPieces = ['a', 'Z', '_', '-', ' ', 'é', '€', '😀', '\u0000', 'ß'];

/** Random bytes, mostly of the kind a codec sees: text, zeros, and anything. */
function randomBytes(rand: () => number, length: number): Uint8Array {
  const out = new Uint8Array(length);
  const mode = int(rand, 3);
  for (let i = 0; i < length; i++) {
    out[i] =
      mode === 0
        ? int(rand, 256)
        : mode === 1
          ? 0x20 + int(rand, 0x5f)
          : pick(rand, [0, 0xff, 0x80, 0xc3, 0x41]);
  }
  return out;
}

function randomText(rand: () => number, maxPieces: number): Uint8Array {
  let s = '';
  for (let n = int(rand, maxPieces + 1); n > 0; n--) {
    s += pick(rand, textPieces);
  }
  return new TextEncoder().encode(s);
}

function randomName(rand: () => number): string {
  const r = rand();
  if (r < 0.1) {
    return hexString(randomBytes(rand, int(rand, 11)));
  }
  const bytes = new Uint8Array(8);
  if (r < 0.6) {
    bytes.set(randomText(rand, 4).subarray(0, 8));
  } else {
    bytes.set(randomBytes(rand, int(rand, 9)));
  }
  if (rand() < 0.2) {
    bytes[int(rand, 8)] = int(rand, 256);
  }
  return hexString(bytes);
}

function randomStatus(rand: () => number): string {
  const r = rand();
  if (r < 0.1) {
    return hexString(randomBytes(rand, int(rand, 3)));
  }
  const code = pick(rand, [0, 5, 32, 37, 38, 255, 256, 0x7fff, 0xffff, int(rand, 0x10000)]);
  const body = r < 0.6 ? randomText(rand, 8) : randomBytes(rand, int(rand, 16));
  return hexString(Uint8Array.of(code & 0xff, code >> 8, ...body));
}

function randomAddress(rand: () => number): Uint8Array {
  const type = rand() < 0.8 ? randomText(rand, 3).subarray(0, 8) : undefined;
  let typeName: Name | undefined;
  try {
    typeName =
      type === undefined
        ? undefined
        : Name.fromBytes(Uint8Array.from({ length: 8 }, (_, i) => type[i] ?? 0));
  } catch {
    typeName = undefined;
  }
  const instance = pick(rand, [
    0,
    0,
    1,
    0x2a,
    0xffffffff,
    0x100000000,
    0xffffffffffff,
    int(rand, 2 ** 48),
  ]);
  const shard = rand() < 0.4 ? pick(rand, [0, 7, 0x7fffffff, 0x80000000, 0xffffffff]) : undefined;
  const host = rand() < 0.3 ? new TextDecoder().decode(randomText(rand, 6)) : undefined;
  return new ChannelAddress({
    type: typeName,
    instance,
    shard,
    host,
    payload: randomBytes(rand, int(rand, 6)),
  }).encode();
}

/** A mux frame from its parts: any command, flags, id and header form, valid or not. */
function randomMuxFrame(rand: () => number): Uint8Array {
  const command = pick(rand, [0, 0, 1, 2, 3]);
  const short = rand() < 0.7;
  let flags = (command << 4) | (short ? MuxFrame.flagShort : 0);
  if (rand() < 0.1) {
    flags |= pick(rand, [0x01, 0x04, 0x08, 0x40, 0x80]);
  }
  const id = pick(rand, [
    0,
    1,
    2,
    3,
    0xfffe,
    0xffff,
    0x10000,
    0xffff00000000,
    0xffffffffffff,
    int(rand, short ? 0x10000 : 2 ** 48),
  ]);
  return concatBytes([flags], le(id, short ? 2 : 6), randomBytes(rand, int(rand, 12)));
}

/** A control message of any type, its payload mostly shaped like that type's. */
function randomControl(rand: () => number): Uint8Array {
  const type = pick(rand, [1, 2, 3, 4, 5, 6, 7, 0, 8, 0xff]);
  let payload: Uint8Array;
  switch (type) {
    case MuxControlType.goAway:
      payload = hexBytes(randomStatus(rand));
      break;
    case MuxControlType.limits:
      payload = randomBytes(rand, pick(rand, [8, 11, 12, 12, 13, int(rand, 16)]));
      break;
    case MuxControlType.credit:
      payload = randomBytes(rand, pick(rand, [9, 10, 10, 11, int(rand, 16)]));
      break;
    case MuxControlType.ident:
      payload = new MuxIdent({
        credential: randomBytes(rand, int(rand, 48)),
        intent: randomBytes(rand, pick(rand, [0, 4, 64])),
        receiver: randomText(rand, 4),
        proof: randomBytes(rand, pick(rand, [0, 64])),
      }).encode();
      break;
    default:
      payload = randomBytes(rand, rand() < 0.02 ? pick(rand, [1024, 1025]) : int(rand, 40));
  }
  return concatBytes([type], payload);
}

/** [bytes] truncated, extended, or with a byte or a flag changed. */
function mutate(rand: () => number, bytes: Uint8Array): Uint8Array {
  const out = Uint8Array.from(bytes);
  switch (int(rand, 4)) {
    case 0:
      return out.subarray(0, int(rand, out.length + 1));
    case 1:
      return Uint8Array.of(...out, ...randomBytes(rand, 1 + int(rand, 4)));
    case 2:
      if (out.length > 0) {
        out[int(rand, out.length)] = int(rand, 256);
      }
      return out;
    default:
      if (out.length > 0) {
        out[0] = (out[0] as number) ^ (1 << int(rand, 8));
      }
      return out;
  }
}
