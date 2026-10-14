// Owned by the "Talk" thread: `talk-frame.ts`, `talk-bulk.ts`,
// `talk-channel.ts`, `talk-message.ts`, `talk-request.ts`,
// `talk-stream.ts`, `talk-bulk-transfer.ts`, `talk-bulk-routes.ts`,
// `talk-forward.ts` and their tests. Keep the contract in `contract.ts`.
export * from './contract.ts';
export { TalkBulkOpen, TalkBulkReference } from './talk-bulk.ts';
export {
  type BulkTarget,
  followBulkTarget,
  heldBulkTimeout,
  installBulkRouting,
  isBulkChannel,
  lingerBulkTarget,
  markBulkChannel,
  maxHeldBulk,
  registerBulkTarget,
  routeBulk,
  routeBulkChannel,
} from './talk-bulk-routes.ts';
// The classes shadow the contract interfaces of the same name: an explicit
// export wins over `export *`.
export {
  TalkAbortError,
  TalkChannel,
  type TalkFrameChannel,
  type TalkRawChannel,
} from './talk-channel.ts';
export {
  type TalkExtension,
  TalkFrame,
  type TalkFrameInit,
  TalkKind,
  talkKindName,
} from './talk-frame.ts';
