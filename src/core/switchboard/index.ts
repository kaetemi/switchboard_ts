/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

export { ChannelPolicies } from './channel-policy.ts';
export * from './contract.ts';
export { IncomingChannel } from './incoming-channel.ts';
export {
  LeaveReport,
  MeshNode,
  type MeshNodeOptions,
  type MeshPublishOptions,
  type NamingClientForOptions,
  namingClientFor,
} from './mesh-node.ts';
export {
  Peer,
  PeerEvent,
  PeerEventType,
  type PeerOpenOptions,
  PeerSet,
  type PeerSetEvents,
  type PeerSetOptions,
  PeerState,
} from './peer-set.ts';
export { type ProxyHandlerOptions, pipeChannels, proxyHandler } from './proxy.ts';
export { RelayService, type RelayServiceOptions } from './relay.ts';
export { defaultRelayType, RelayConfig } from './relay-config.ts';
export {
  EndpointResolver,
  isBrokeringResolver,
  isSlotResolver,
  StaticResolver,
} from './resolver.ts';
export type {
  BindingDialSettings,
  MemoryEndpointRegistry,
  MuxFactory,
  SwitchboardBinding,
  SwitchboardRuntime,
  TransportDialer,
  TransportDialSettings,
} from './runtime.ts';
export {
  type ListenerHandle,
  type OpenChannelOptions,
  type OpenSelectedOptions,
  type OwnListener,
  type RegisterServiceOptions,
  type SelectedInstance,
  type SelectOptions,
  Switchboard,
  type SwitchboardOptions,
} from './switchboard.ts';
