// Owned by the "naming and sharding" thread: the naming protocol codecs,
// the naming client, service and resolver, slot tables, the slot gate,
// slot channels and their tests (the wiki's naming and sharding vectors).
export {
  type ConnectHandler,
  type DefineSlotsOptions,
  NamingClient,
  type NamingClientEvents,
  type NamingClientOptions,
  type RegisterOptions,
  type TalkConnector,
} from './naming-client.ts';
export {
  AssignResult,
  type SlotHandler,
  type SlotRequestContext,
} from './naming-client-slots.ts';
export * from './naming-protocol.ts';
export { NamingResolver, type NamingResolverOptions } from './naming-resolver.ts';
export {
  type NamingConnection,
  type NamingCredentialIssuer,
  NamingService,
  type NamingServiceEvents,
  type NamingServiceOptions,
} from './naming-service.ts';
export * from './slot-key.ts';
export * from './slot-table.ts';
