// The inputs of the wiki page "Polyverse Switchboard Test Vectors",
// section "Credentials" (C1, C2) and the NONCE and IDENT rows of "Mux
// frames" (P), shared by the identity vector tests as Dart shares them from
// `test/vectors/identity_vectors_test.dart`.
import { hexBytes } from '../../../src/core/bytes.ts';

/** 2026-01-01T00:00:00Z, in Unix seconds. */
export const issuedAt = 1767225600;

/** Eight hours later. */
export const expiresAt = issuedAt + 8 * 3600;

/** RFC 8032, section 7.1, test 1: the authority. */
export const authoritySeed = hexBytes(
  '9D 61 B1 9D EF FD 5A 60 BA 84 4A F4 92 EC 2C C4 ' +
    '44 49 C5 69 7B 32 69 19 70 3B AC 03 1C AE 7F 60',
);
export const authorityPublic =
  'D7 5A 98 01 82 B1 0A B7 D5 4B FE D3 C9 64 07 3A ' +
  '0E E1 72 F3 DA A6 23 25 AF 02 1A 68 F7 07 51 1A';

/** RFC 8032, section 7.1, test 2: the holder. */
export const holderSeed = hexBytes(
  '4C CD 08 9B 28 FF 96 DA 9D B6 C3 46 EC 11 4E 0F ' +
    '5B 8A 31 9F 35 AB A6 24 DA 8C F6 ED 4F B8 A6 FB',
);
export const holderPublic =
  '3D 40 17 C3 E8 43 89 5A 92 B7 0A A7 4D 1B 7E BC ' +
  '9C 98 2C CF 2E C4 96 8C C0 CD 55 F1 2A F4 66 0C';

/** C1: an HMAC-SHA256 bearer credential for `npc-7`. */
export const hmacCredential =
  '01 01 6B 31 00 00 00 00 00 00 05 6E 70 63 2D 37 ' +
  '00 B9 55 69 80 29 56 69 00 03 01 6E 70 63 00 00 ' +
  '00 00 00 02 2A 00 00 00 00 00 00 00 03 6E 70 63 ' +
  '2D 2A 00 00 00 01 84 DB E9 02 5A 34 E5 C3 7A C8 ' +
  'BC C2 B7 7C 7B 6D E6 84 51 7A F1 7B C8 41 66 34 ' +
  '8A BE 22 D9 E3 C9';

/** C2: an Ed25519 credential for `worker-a1` with the holder's key. */
export const ed25519Credential =
  '01 01 61 75 74 68 31 00 00 00 09 77 6F 72 6B 65 ' +
  '72 2D 61 31 00 B9 55 69 80 29 56 69 20 3D 40 17 ' +
  'C3 E8 43 89 5A 92 B7 0A A7 4D 1B 7E BC 9C 98 2C ' +
  'CF 2E C4 96 8C C0 CD 55 F1 2A F4 66 0C 01 01 77 ' +
  '6F 72 6B 65 72 2D 2A 02 AF F7 12 8F FF 57 0A 4F ' +
  'F6 08 6D F4 20 36 FF 70 29 30 5D E6 86 32 D5 E2 ' +
  '95 CC 95 0E 78 B8 FC D9 E2 65 6D F2 AE 58 CB 87 ' +
  '43 28 A1 73 DD 13 77 56 DF 89 91 45 74 59 E4 91 ' +
  'FE AC 35 CB EC A3 2F 0A';

export const senderNonce = new Uint8Array(32).fill(0x11);
export const receiverNonce = new Uint8Array(32).fill(0x22);
export const intent = hexBytes('01 02 03 04');

/** The receiver `consumer`, in UTF-8. */
export const receiver = hexBytes('63 6F 6E 73 75 6D 65 72');

/** P: the holder's proof over the nonces, the intent and the receiver. */
export const proof =
  '4A 11 15 AE FE D7 6E 3A 3E 00 0A FA C8 64 73 BC ' +
  'D5 16 3C 32 7E 54 80 8F 44 A2 34 9E 33 C5 36 DF ' +
  '05 A5 BA 33 FD 04 F2 81 E3 11 11 25 45 23 48 37 ' +
  'B3 6B DB 22 CA 8A 17 45 00 23 9F D7 1D EC 8B 0B';

/** A minute after the credentials were issued, in milliseconds since the epoch. */
export const at = (issuedAt + 60) * 1000;
