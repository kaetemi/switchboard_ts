// Owned by the "protocol core" thread: the frame codecs (`mux-frame.ts`),
// what every binding shares (`mux-protocol.ts`: IDENT, verifyIdent, status
// rules), the frame mux (`frame-mux-connection.ts`, `frame-mux-channel.ts`),
// the `BindingConnection` base and their tests. Keep the contract in
// `contract.ts`.
export * from './contract.ts';
export * from './mux-frame.ts';
export {
  checkMuxStatus,
  decodeStatusPayload,
  identReceiverProblem,
  MuxIdent,
  type MuxIdentInit,
  maxControlPayloadLength,
  statusForWire,
  statusFromWire,
  truncateStatus,
  type VerifyIdentOptions,
  verifyIdent,
} from './mux-protocol.ts';
