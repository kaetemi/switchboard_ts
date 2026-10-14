/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The reconnecting client and its persistent channels. `Backoff` is
// internal (shared with `PeerSet`).

export { PersistentChannel, PersistentTalk } from './persistent-channel.ts';
export {
  type ChannelAddressLike,
  type ClientEvents,
  type ClientOpenOptions,
  ClientPhase,
  ClientState,
  type ClientStateInit,
  type ConnectionConnector,
  ReconnectingClient,
  type ReconnectingClientOptions,
} from './reconnecting-client.ts';
