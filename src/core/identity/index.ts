// Owned by the "protocol core" thread: the credential codec, issuer and
// verifier, holder keys, Ed25519 and SHA-256/HMAC over WebCrypto, secure
// random, and their tests (the wiki's credential vectors). Keep the
// contract in `contract.ts`.
export * from './contract.ts';
export { createPeerIdentity, type PeerIdentityInit } from './peer-identity.ts';
export { secureRandomBytes } from './secure-random.ts';
