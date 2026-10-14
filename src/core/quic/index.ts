// Owned by the "QUIC transport" thread: the record codecs (`records.ts`),
// `record-reader.ts`, `QuicBindingConnection` and `QuicChannel`, the
// control stream, `QuicIdentityRules`, `connectQuicBinding` and their
// tests (the binding page's vectors, the contracts over a fake provider).
// The providers live in `src/node/quic` and `src/web`. Keep the provider
// contract in `contract.ts`.
export * from './contract.ts';
