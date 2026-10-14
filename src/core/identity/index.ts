// Owned by the "protocol core" thread: the credential codec, issuer and
// verifier, holder keys, Ed25519 and SHA-256/HMAC over WebCrypto, secure
// random, and their tests (the wiki's credential vectors). Keep the
// contract in `contract.ts`.
export * from './contract.ts';
// The classes `Credential`, `Scope` and `CredentialVerifier` shadow the
// contract interfaces of the same name: an explicit export wins over
// `export *`. The holder key class is `Ed25519HolderKey`, apart from the
// contract's `HolderKey` interface, which `identify` takes.
export {
  Credential,
  type CredentialInit,
  credentialKindName,
  signatureKindName,
  signatureLength,
} from './credential.ts';
export {
  CredentialIssuer,
  type CredentialIssuerOptions,
  type IssueOptions,
} from './credential-issuer.ts';
export {
  type CredentialKeys,
  CredentialVerifier,
  type CredentialVerifierOptions,
} from './credential-verifier.ts';
export { Ed25519HolderKey } from './holder-key.ts';
export { createPeerIdentity, type PeerIdentityInit } from './peer-identity.ts';
export { rightFromCode, rightName, Scope } from './scope.ts';
export { secureRandomBytes } from './secure-random.ts';
