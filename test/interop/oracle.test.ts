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
// TS codecs against the reference (names, statuses, addresses, mux frames,
// control messages, Talk frames, the stream binding, the naming and
// sharding payloads, credentials, slot keys, QUIC records and HELLO), on the
// vectors and on random and mutated inputs: both must accept or refuse the
// same bytes and, when they accept, decode the same fields and re-encode the
// same bytes. The TS side is the TS peer's oracle (ts_peer/oracle.ts), which
// answers the same canonical JSON as the Dart peer's. SWITCHBOARD_FUZZ_SEED
// and SWITCHBOARD_FUZZ_COUNT vary the random inputs (the seed is in the test
// names, so a failure can be replayed).

import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/channel-address.ts';
import { concatBytes, hexBytes, hexString } from '../../src/core/bytes.ts';
import { MuxControlType, MuxFrame, MuxIdent } from '../../src/core/mux/index.ts';
import { Name } from '../../src/core/name.ts';
import { TalkFrame, TalkKind } from '../../src/core/talk/talk-frame.ts';
import { StreamFraming } from '../../src/core/transport/stream-framing.ts';
import {
  type WikiVector,
  wikiBlob,
  wikiBytes,
  wikiVariants,
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
import { decodeKind, namingKinds, shardingKinds } from './ts_peer/oracle.ts';

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
      test.each(namingVectors().filter(([v]) => !v.composite || v.hex !== null))(
        'naming: $0.case',
        async (v, kind) => {
          await expectReference(kind, v);
        },
      );

      test('naming: the payloads the wiki says must be refused', async () => {
        for (const [kind, hex] of refusedNamingPayloads()) {
          expect((await decode(kind, hex)).ok, `${kind} ${hex}`).toBe(false);
        }
      });

      test('naming: a record without metadata has none, and encodes it', async () => {
        const v = wikiVector(
          'test-vectors/naming-service-payloads/prose-6e-70-63-00-00-00-00-00-05-00-00-00-00-00-00',
        );
        expect(await decode('up', v.hex ?? '')).toMatchObject({
          ok: true,
          value: { up: true, record: { instance: 5, endpoints: [], metadata: '' } },
        });
        expect((await decode('up', v.hex ?? '')).ok && true).toBe(true);
      });

      test.each(shardingVectors())('sharding: $0.case', async (v, kind) => {
        await expectReference(kind, v);
      });

      test('sharding: unknown SLOTSPC flag bits are ignored', async () => {
        const v = wikiVector(
          'test-vectors/sharding-payloads/prose-6b-76-00-00-00-00-00-00-00-04-00-00-00-fe',
        );
        expect(await decode('slotSpace', v.hex ?? '')).toMatchObject({
          ok: true,
          value: { count: 1024, mode: 0, lazy: false, shared: true },
        });
      });

      test('credentials: C1 and C2 decode and encode again as they are', async () => {
        for (const blob of ['C1', 'C2']) {
          const hex = hexString(wikiBlob(blob));
          const r = await decode('credential', hex);
          expect(r.ok, blob).toBe(true);
          expect(r.ok && r.value.reencoded, blob).toBe(hex);
        }
      });

      test.each(credentialVectors())('credential refused: $case', async (v) => {
        for (const bytes of credentialInputs(v)) {
          expect((await decode('credential', hexString(bytes))).ok, v.id).toBe(false);
        }
      });

      test.each(quicRecordVectors())('QUIC records: $case', async (v) => {
        const r = await decode('quicRecords', v.hex ?? '');
        if (v.negative && !stateDependent.has(v.id)) {
          // A malformed record: refused, or never complete.
          expect(r.ok && r.value['buffered'] === 0, v.id).toBe(false);
        } else {
          expect(r.ok, v.id).toBe(true);
        }
      });

      test.each(wikiVectorsIn('quic-binding/test-vectors/status-codes/'))(
        'QUIC status code: $case',
        async (v) => {
          const code = Number.parseInt(v.case, 10);
          const r = await decode('quicCode', hexString(le(code, 2)));
          const raw = (v.columns?.['Raw QUIC code on the wire'] ?? '').replaceAll('`', '');
          expect(r).toMatchObject({
            ok: true,
            value: {
              rawVarint: raw,
              http3: Number(v.columns?.['HTTP/3 code of the WebTransport code']),
              http3Varint: v.hex,
              back: code,
            },
          });
        },
      );

      test('QUIC exporter labels and contexts', async () => {
        const [webTransportLabel, context0, context4, rawQuicLabel] = wikiVectorsIn(
          'quic-binding/test-vectors/exporter/',
        ).map((v) => v.hex);
        for (const [id, context] of [
          [0, context0],
          [4, context4],
        ] as const) {
          expect(await decode('exporter', hexString(le(id, 8).reverse()))).toMatchObject({
            ok: true,
            value: { context, webTransportLabel, rawQuicLabel },
          });
        }
      });

      test.each(helloVectors())('QUIC HELLO: $case', async (v) => {
        const body = hexBytes(v.hex ?? '').subarray(5);
        const r = await decode('quicHello', hexString(body));
        expect(r.ok, v.id).toBe(!v.negative);
      });
    });

    describe('TS and the reference decode alike', () => {
      test.each(wikiVectorsIn('test-vectors/names/'))('name vector: $case', async (v) => {
        await expectAgreement('name', [v.hex ?? '']);
      });

      test.each(wikiVectorsIn('test-vectors/address-headers/').filter((v) => v.hex !== null))(
        'address vector: $case',
        async (v) => {
          await expectAgreement('address', [v.hex ?? '']);
        },
      );

      test.each(wikiVectorsIn('test-vectors/mux-frames/').filter((v) => v.hex !== null))(
        'mux frame vector: $case',
        async (v) => {
          await expectAgreement('mux', [v.hex ?? '']);
        },
      );

      test('control vectors: the messages on channel 0 and the refused payloads', async () => {
        const inputs = [...controlMessages(), ...refusedControlPayloads.map(controlMessageOf)];
        await expectAgreement('control', inputs, sameControlVerdicts);
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
        await expectAgreement('status', payloads);
      });

      test('Talk vectors', async () => {
        const inputs = wikiVectorsIn('test-vectors/talk-messages/')
          .map((v) => v.hex)
          .filter((h) => h !== null);
        await expectAgreement('talk', inputs);
      });

      test('stream binding vectors: the preamble, frames, and what breaks them', async () => {
        const preamble = wikiVector('test-vectors/transport-stream-binding/preamble-version-1');
        const frame = wikiVector(
          'test-vectors/transport-stream-binding/frame-carrying-body-02-02-00-aa',
        );
        await expectAgreement('stream', [
          '',
          preamble.hex ?? '',
          `${preamble.hex} ${frame.hex}`,
          `${preamble.hex} ${frame.hex} ${frame.hex}`,
          `${preamble.hex} 04 00`,
          `${preamble.hex} 04 00 00 00 02`,
          frame.hex ?? '',
          '53 57 42 44 02 00 00 00',
          '53 57 42 44 01 FF 00 00',
          '53 57 42',
        ]);
      });

      test.each(namingVectors())('naming vector: $0.case', async (v, kind) => {
        await expectAgreement(kind, [v.hex ?? '']);
      });

      test('naming: the payloads the wiki says must be refused', async () => {
        for (const [kind, hex] of refusedNamingPayloads()) {
          await expectAgreement(kind, [hex]);
        }
      });

      test.each(shardingVectors())('sharding vector: $0.case', async (v, kind) => {
        await expectAgreement(kind, [v.hex ?? '']);
      });

      test('credential vectors: C1, C2 and their modifications', async () => {
        await expectAgreement('credential', [
          hexString(wikiBlob('C1')),
          hexString(wikiBlob('C2')),
          ...credentialVectors().flatMap((v) => credentialInputs(v).map(hexString)),
        ]);
      });

      test('QUIC record and HELLO vectors', async () => {
        await expectAgreement(
          'quicRecords',
          [...quicRecordVectors(), ...helloVectors()].map((v) => v.hex ?? ''),
        );
        await expectAgreement(
          'quicHello',
          helloVectors().map((v) => hexString(hexBytes(v.hex ?? '').subarray(5))),
        );
      });

      test(`names, ${count} random inputs, seed ${seed}`, async () => {
        const rand = random(seed);
        await expectAgreement(
          'name',
          Array.from({ length: count }, () => randomName(rand)),
        );
      });

      test(`statuses, ${count} random inputs, seed ${seed}`, async () => {
        const rand = random(seed + 1);
        await expectAgreement(
          'status',
          Array.from({ length: count }, () => randomStatus(rand)),
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
        await expectAgreement('address', inputs.map(hexString));
      });

      test(`mux frames, ${count} random and mutated inputs, seed ${seed}`, async () => {
        const rand = random(seed + 4);
        const seeds = tables('test-vectors/mux-frames/')
          .map((v) => v.hex)
          .filter((h) => h !== null);
        const inputs = Array.from({ length: count }, () =>
          rand() < 0.5 ? randomMuxFrame(rand) : mutate(rand, hexBytes(pick(rand, seeds))),
        );
        await expectAgreement('mux', inputs.map(hexString));
      });

      test(`control messages, ${count} random and mutated inputs, seed ${seed}`, async () => {
        const rand = random(seed + 5);
        const seeds = controlMessages();
        const inputs = Array.from({ length: count }, () =>
          rand() < 0.5 ? randomControl(rand) : mutate(rand, hexBytes(pick(rand, seeds))),
        );
        await expectAgreement('control', inputs.map(hexString), sameControlVerdicts);
      });

      test(`Talk frames, ${count} random and mutated inputs, seed ${seed}`, async () => {
        const rand = random(seed + 6);
        const seeds = wikiVectorsIn('test-vectors/talk-messages/')
          .map((v) => v.hex)
          .filter((h) => h !== null);
        const inputs = Array.from({ length: count }, () =>
          rand() < 0.5 ? randomTalkFrame(rand) : mutate(rand, hexBytes(pick(rand, seeds))),
        );
        await expectAgreement('talk', inputs.map(hexString));
      });

      test(`stream binding, ${count} random byte streams, seed ${seed}`, async () => {
        const rand = random(seed + 7);
        const inputs = Array.from({ length: count }, () => randomStream(rand));
        await expectAgreement('stream', inputs.map(hexString));
      });

      test(`naming and sharding payloads, ${count} mutated inputs per kind, seed ${seed}`, async () => {
        const rand = random(seed + 8);
        const byKind = new Map<string, Uint8Array[]>();
        for (const [v, kind] of [...namingVectors(), ...shardingVectors()]) {
          byKind.set(kind, [...(byKind.get(kind) ?? []), hexBytes(v.hex ?? '')]);
        }
        for (const kind of [...namingKinds, ...shardingKinds]) {
          const seeds = byKind.get(kind) ?? [randomBytes(rand, 20)];
          const inputs = Array.from({ length: count }, () =>
            rand() < 0.15 ? randomBytes(rand, int(rand, 40)) : mutate(rand, pick(rand, seeds)),
          );
          await expectAgreement(kind, inputs.map(hexString));
        }
      });

      test(`credentials, ${count} mutated inputs, seed ${seed}`, async () => {
        const rand = random(seed + 9);
        const seeds = [wikiBlob('C1'), wikiBlob('C2')];
        const inputs = Array.from({ length: count }, () => mutate(rand, pick(rand, seeds)));
        await expectAgreement('credential', inputs.map(hexString));
      });

      test(`slot keys, ${count} random keys, seed ${seed}`, async () => {
        const rand = random(seed + 10);
        const inputs = Array.from({ length: count }, () =>
          rand() < 0.5 ? randomText(rand, 12) : randomBytes(rand, int(rand, 40)),
        );
        await expectAgreement('slotKey', ['', ...inputs.map(hexString)]);
      });

      test(`QUIC records and HELLO, ${count} random and mutated inputs, seed ${seed}`, async () => {
        const rand = random(seed + 11);
        const seeds = [...quicRecordVectors(), ...helloVectors()]
          .map((v) => v.hex ?? '')
          .filter((h) => h !== '')
          .map(hexBytes);
        const records = Array.from({ length: count }, () =>
          rand() < 0.5 ? randomRecords(rand) : mutate(rand, pick(rand, seeds)),
        );
        await expectAgreement('quicRecords', records.map(hexString));
        const hellos = Array.from({ length: count }, () =>
          randomBytes(rand, pick(rand, [8, 9, 9, 9, 10, 12])),
        );
        await expectAgreement('quicHello', hellos.map(hexString));
      });

      test('QUIC codes of every status code below 1024, and at random', async () => {
        const rand = random(seed + 12);
        const codes = [
          ...Array.from({ length: 1024 }, (_, i) => i),
          ...Array.from({ length: count }, () => int(rand, 0x10000)),
          0xffff,
        ];
        await expectAgreement(
          'quicCode',
          codes.map((c) => hexString(le(c, 2))),
        );
      });

      test(`exporter contexts, ${count} random session ids, seed ${seed}`, async () => {
        const rand = random(seed + 13);
        const ids = Array.from({ length: count }, () =>
          pick(rand, [0, 1, 4, 0xffffffff, 0x100000000, 2 ** 53 - 1, int(rand, 2 ** 53)]),
        );
        await expectAgreement(
          'exporter',
          ids.map((id) => hexString(le(id, 8).reverse())),
        );
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

      test('the TS peer answers decodeMany as its oracle does in process', async () => {
        const ts = await startPeer('ts', { config: { listen: [] } });
        try {
          const hexes = wikiVectorsIn('test-vectors/talk-messages/')
            .map((v) => v.hex)
            .filter((h) => h !== null);
          const answers = await ts.rpc<DecodeResult[]>('decodeMany', { kind: 'talk', hexes });
          expect(answers.map((a) => a.ok)).toEqual(
            hexes.map((h) => {
              try {
                decodeKind('talk', hexBytes(h));
                return true;
              } catch {
                return false;
              }
            }),
          );
          expect((await ts.rpc<{ oracle: string[] }>('info')).oracle).toContain('quicHello');
        } finally {
          await ts.stop();
        }
      });
    });
  },
);

/** The naming payload vectors, each with the oracle kind it is a message of. */
function namingVectors(): [WikiVector, string][] {
  const kinds: [RegExp, string][] = [
    [/\/register-response-/, 'registerResponse'],
    [/\/register-/, 'register'],
    [/\/unregstr-/, 'unregister'],
    [/\/(un)?watch-/, 'watch'],
    [/\/up-/, 'up'],
    [/\/down-/, 'down'],
    [/\/connect-request-/, 'connect'],
    [/\/connect-response-/, 'connectResponse'],
    [/\/connect-to-the-instance-/, 'dialBack'],
  ];
  return wikiVectorsIn('test-vectors/naming-service-payloads/').flatMap((v) => {
    const kind = kinds.find(([re]) => re.test(v.id))?.[1];
    return kind === undefined || v.hex === null ? [] : [[v, kind] as [WikiVector, string]];
  });
}

/**
 * The naming payloads the wiki's prose says each MUST be a protocol error,
 * built around its CONNECT vectors: an intent of 65 bytes, no intent, an
 * endpoint without a scheme, a requester that is not UTF-8, a response cut
 * short.
 */
function refusedNamingPayloads(): [string, string][] {
  const connect = wikiBytes(
    'test-vectors/naming-service-payloads/connect-request-worker-g-instance-0x2a-tcp-10-0-0-7-9300-intent-01-02-03-04',
  );
  const dialBack = wikiBytes(
    'test-vectors/naming-service-payloads/connect-to-the-instance-requester-consumer-1-the-same-endpoint-and-intent',
  );
  const head = connect.subarray(0, 14);
  const endpoint = connect.subarray(14, connect.length - 5);
  const prose = wikiVectors(
    'test-vectors/naming-service-payloads/prose-41',
    'test-vectors/naming-service-payloads/prose-01-78',
    'test-vectors/naming-service-payloads/prose-01-ff',
    'test-vectors/naming-service-payloads/prose-09-77-6f',
  ).map((v) => hexBytes(v.hex ?? ''));
  const [intent65, noScheme, notUtf8, short] = prose as [
    Uint8Array,
    Uint8Array,
    Uint8Array,
    Uint8Array,
  ];
  return [
    ['connect', hexString(concatBytes(head, endpoint, intent65, new Uint8Array(65)))],
    ['connect', hexString(concatBytes(head, endpoint))],
    ['connect', hexString(concatBytes(head, noScheme, [0]))],
    ['dialBack', hexString(concatBytes(notUtf8, dialBack.subarray(11)))],
    ['connectResponse', hexString(short)],
  ];
}

/** The sharding payload vectors, each with the oracle kind it is a message of. */
function shardingVectors(): [WikiVector, string][] {
  const kinds: [RegExp, string][] = [
    [/\/slots-/, 'slots'],
    [/\/slotspc-/, 'slotSpace'],
    [/\/holding-response-/, 'holdingResponse'],
    [/\/holding-/, 'holding'],
    [/\/claim-response-/, 'claimResponse'],
    [/\/claim-/, 'claim'],
    [/\/release-/, 'release'],
    [/\/locate-response-/, 'locateResponse'],
    [/\/locate-/, 'locate'],
    [/\/migrate-/, 'migrate'],
    [/\/phase-/, 'phase'],
    [/\/assign-response-/, 'assignResponse'],
    [/\/assign-/, 'assign'],
    [/\/drain-/, 'drain'],
    [/\/forward-/, 'forward'],
    [/\/resume-/, 'resume'],
    [/\/slot-/, 'slot'],
    [/\/(moved|relocated)-/, 'moved'],
  ];
  return wikiVectorsIn('test-vectors/sharding-payloads/').flatMap((v) => {
    const kind = kinds.find(([re]) => re.test(v.id))?.[1];
    return kind === undefined || v.hex === null || v.prose === true
      ? []
      : [[v, kind] as [WikiVector, string]];
  });
}

/** The credential vectors: modifications of C1 that must be refused. */
function credentialVectors(): WikiVector[] {
  return wikiVectorsIn('test-vectors/credentials/');
}

/** The inputs of credential vector [v]: its value, its variants, or 638 bytes. */
function credentialInputs(v: WikiVector): Uint8Array[] {
  const values = wikiVariants(v.id);
  if (values.length > 0) {
    return values;
  }
  // "any 638 bytes": C1 with its signature padded out.
  const c1 = wikiBlob('C1');
  return [concatBytes(c1, new Uint8Array(638 - c1.length))];
}

/**
 * QUIC record vectors the codec accepts though the binding refuses them, by
 * the state they arrive in (a reserved channel id, a record after CLOSE).
 */
const stateDependent = new Set([
  'quic-binding/test-vectors/records/open-of-reserved-channel-1',
  'quic-binding/test-vectors/records/open-of-reserved-channel-0xffff',
  'quic-binding/test-vectors/records/a-record-after-a-close-record',
]);

/** The QUIC record vectors that have one value. */
function quicRecordVectors(): WikiVector[] {
  return [
    ...wikiVectorsIn('quic-binding/test-vectors/records/'),
    ...wikiVectorsIn('quic-binding/test-vectors/bulk-channels/'),
  ].filter((v) => v.hex !== null);
}

/** The control stream vectors that carry a HELLO (type 08 after the record header). */
function helloVectors(): WikiVector[] {
  return wikiVectorsIn('quic-binding/test-vectors/control-stream/').filter(
    (v) => v.hex !== null && v.hex.slice(12, 14) === '08',
  );
}

/** The naming kinds that carry endpoints. */
const endpointKinds = new Set(['record', 'register', 'up', 'down', 'connect', 'dialBack']);

/** The TS codec's canonical fields, as the TS peer's oracle answers them. */
function tsDecode(kind: string, bytes: Uint8Array): Fields {
  return decodeKind(kind, bytes);
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
  normalize: (value: DecodedFields) => DecodedFields = (value) => value,
): Promise<void> {
  const reference = await decodeMany(kind, hexes);
  const disagreements: string[] = [];
  const normalizedByDart: { hex: string; dart: DecodedFields }[] = [];
  hexes.forEach((hex, i) => {
    const d = reference[i];
    let t: { ok: true; value: Fields } | { ok: false; error: string };
    try {
      t = { ok: true, value: tsDecode(kind, hexBytes(hex)) };
    } catch (e) {
      t = { ok: false, error: e instanceof Error ? `${e.name}: ${e.message}` : String(e) };
    }
    if (d === undefined) {
      disagreements.push(`[${hex}] no reference answer`);
    } else if (
      d.ok &&
      endpointKinds.has(kind) &&
      d.value.reencoded !== hex &&
      (t.ok ? JSON.stringify(t.value) !== JSON.stringify(d.value) : t.error.includes('endpoint'))
    ) {
      // An endpoint the reference normalises (Uri.parse: the scheme and host
      // lower-cased, characters escaped) where the TS codec carries
      // endpoints as written and refuses what a URI cannot hold: the
      // documented difference of endpointProblem in
      // src/core/naming/naming-protocol.ts. It holds as long as what the
      // reference sends instead, its normalised form, decodes alike.
      normalizedByDart.push({ hex, dart: d.value });
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
  for (const { hex, dart } of normalizedByDart) {
    const sent = String(dart.reencoded);
    let own: string;
    try {
      own = JSON.stringify(normalize(tsDecode(kind, hexBytes(sent))));
    } catch (e) {
      own = `refuses (${e instanceof Error ? e.message : String(e)})`;
    }
    if (own !== JSON.stringify(normalize(dart))) {
      disagreements.push(
        `[${hex}] Dart normalises it to [${sent}], which TS ${own.startsWith('refuses') ? own : `decodes as ${own}`}`,
      );
    }
  }
  expect(
    disagreements.slice(0, 20),
    `${kind}: ${disagreements.length} of ${hexes.length} disagree`,
  ).toEqual([]);
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

/** A Talk frame from its parts: any kind, flags, ids, window and payload, valid or not. */
function randomTalkFrame(rand: () => number): Uint8Array {
  if (rand() < 0.6) {
    try {
      const kind = pick(rand, [
        TalkKind.message,
        TalkKind.streamItem,
        TalkKind.abort,
        TalkKind.extend,
      ]);
      const hasRequest = rand() < 0.6;
      const hasResponse = !hasRequest || rand() < 0.3;
      return new TalkFrame({
        kind,
        procedure:
          rand() < 0.5
            ? Name.fromBytes(concatBytes(randomText(rand, 3), new Uint8Array(8)).subarray(0, 8))
            : undefined,
        requestId: hasRequest ? pick(rand, [1, 2, 0xffff, 0xffffff, 1 + int(rand, 0xfffffe)]) : 0,
        responseId: hasResponse ? pick(rand, [1, 7, 0xffffff, 1 + int(rand, 0xfffffe)]) : 0,
        stream: rand() < 0.2,
        window: pick(rand, [0, 1, 16, 0x7fffffff, 0x80000000, 0xfffffffe, 0xffffffff]),
        bulk: rand() < 0.1,
        payload: randomBytes(rand, int(rand, 16)),
      }).encode();
    } catch {
      // A combination the encoder refuses: fall through to raw bytes.
    }
  }
  const flags = int(rand, 256);
  return concatBytes([flags], randomBytes(rand, int(rand, 24)));
}

/** A byte stream of the stream binding: a preamble, valid or not, and frames, whole or cut. */
function randomStream(rand: () => number): Uint8Array {
  const parts: Uint8Array[] = [];
  const r = rand();
  if (r < 0.8) {
    parts.push(StreamFraming.encodePreamble());
  } else if (r < 0.9) {
    parts.push(Uint8Array.of(0x53, 0x57, 0x42, 0x44, pick(rand, [0, 1, 2]), int(rand, 256), 0, 0));
  } else {
    parts.push(randomBytes(rand, int(rand, 9)));
  }
  for (let n = int(rand, 4); n > 0; n--) {
    // Mostly frames, now and then a length of 0 (refused) or an oversized one.
    const r2 = rand();
    parts.push(
      r2 < 0.9
        ? StreamFraming.encodeFrame(randomBytes(rand, 1 + int(rand, 20)))
        : r2 < 0.95
          ? new Uint8Array(4)
          : le(0x7fffffff + int(rand, 2), 4),
    );
  }
  const all = concatBytes(...parts);
  return rand() < 0.3 ? all.subarray(0, int(rand, all.length + 1)) : all;
}

/** A byte stream of QUIC records, valid or not. */
function randomRecords(rand: () => number): Uint8Array {
  const parts: Uint8Array[] = [];
  for (let n = 1 + int(rand, 3); n > 0; n--) {
    const command = pick(rand, [0, 0, 1, 2, 3]);
    let body: Uint8Array;
    switch (command) {
      case 1:
        body = concatBytes(
          le(pick(rand, [0, 1, 2, 3, 0xffff, 0xffffffffffff]), 6),
          randomBytes(rand, int(rand, 12)),
        );
        break;
      case 2:
        body = randomBytes(rand, pick(rand, [0, 1, 2, 3, 8]));
        break;
      default:
        body = randomBytes(rand, int(rand, 16));
    }
    const header = command * 0x40000000 + (rand() < 0.1 ? body.length + int(rand, 3) : body.length);
    parts.push(concatBytes(le(header, 4), body));
  }
  return concatBytes(...parts);
}
