/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

export { ChannelPolicies } from './channel-policy.ts';
export * from './contract.ts';
export { IncomingChannel } from './incoming-channel.ts';
export { defaultRelayType, RelayConfig } from './relay-config.ts';
export {
  type BrokeringResolver,
  EndpointResolver,
  isBrokeringResolver,
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
  type ServiceAddressLike,
  Switchboard,
  type SwitchboardOptions,
  toServiceAddress,
} from './switchboard.ts';
