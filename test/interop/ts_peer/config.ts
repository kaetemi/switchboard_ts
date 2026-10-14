/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The peer's configuration, from `--config <json>` (README, "Configuration"),
// as dart_peer/lib/src/config.dart reads it: absent keys keep the defaults.

import type { MuxOptions, TalkOptions } from '../../../src/node/index.ts';
import {
  ArgumentProblem,
  arrayArg,
  boolArg,
  intArg,
  type Json,
  objectArg,
  requiredString,
  stringArg,
} from './protocol.ts';

/** The transports a peer can listen on. */
export const transports = ['tcp', 'ws', 'quic', 'webtransport'] as const;

/** A transport. */
export type Transport = (typeof transports)[number];

/** The behaviours a service can have. */
export const behaviours = ['echo', 'raw', 'sink', 'forward'] as const;

/** What a `forward` service forwards to. */
export interface ForwardTarget {
  /** The endpoint; undefined for this peer's first listener. */
  readonly endpoint: string | undefined;
  /** The service type; undefined for `echo`. */
  readonly type: string | undefined;
  /** The instance. */
  readonly instance: number;
}

/** One registered service: a type, an instance id, and the behaviour that serves it. */
export interface ServiceSpec {
  /** The service type. */
  readonly type: string;
  /** `echo` (the interop procedures), `raw` (channels as handles), `sink` (never read), `forward`. */
  readonly behaviour: (typeof behaviours)[number];
  /** The instance id; 0 registers the type's default handler. */
  readonly instance: number;
  /** `forward`: what it forwards to. */
  readonly target: ForwardTarget;
}

/** Everything the command line configures. */
export interface PeerConfig {
  /** The transports to listen on. */
  readonly listen: readonly Transport[];
  /** The WebSocket and WebTransport path. */
  readonly path: string;
  /** The options of every connection. */
  readonly mux: Partial<MuxOptions>;
  /** The Talk options of the services and of the client channels. */
  readonly talk: Partial<TalkOptions>;
  /** The services the node registers. */
  readonly services: readonly ServiceSpec[];
}

const noTarget: ForwardTarget = { endpoint: undefined, type: undefined, instance: 0 };

/** `echo`, `raw` and `sink` at instance 1. */
export const defaultServices: readonly ServiceSpec[] = [
  { type: 'echo', behaviour: 'echo', instance: 1, target: noTarget },
  { type: 'raw', behaviour: 'raw', instance: 1, target: noTarget },
  { type: 'sink', behaviour: 'sink', instance: 1, target: noTarget },
];

/** Reads the JSON configuration; absent keys keep their defaults. */
export function peerConfig(json: Json): PeerConfig {
  const listen = arrayArg(json, 'listen');
  const services = arrayArg(json, 'services');
  return {
    listen:
      listen === undefined
        ? transports
        : listen.map((t) => {
            if (typeof t === 'string' && (transports as readonly string[]).includes(t)) {
              return t as Transport;
            }
            throw new ArgumentProblem(`unknown transport ${String(t)}`);
          }),
    path: stringArg(json, 'path') ?? '/sb',
    mux: muxConfig(objectArg(json, 'mux') ?? {}),
    talk: talkConfig(objectArg(json, 'talk') ?? {}),
    services:
      services === undefined
        ? defaultServices
        : services.map((s) => {
            const spec = s as Json;
            const behaviour = stringArg(spec, 'behaviour') ?? 'echo';
            if (!(behaviours as readonly string[]).includes(behaviour)) {
              throw new ArgumentProblem(`unknown service behaviour ${behaviour}`);
            }
            const target = objectArg(spec, 'target') ?? {};
            return {
              type: requiredString(spec, 'type'),
              behaviour: behaviour as ServiceSpec['behaviour'],
              instance: intArg(spec, 'instance') ?? 0,
              target: {
                endpoint: stringArg(target, 'endpoint'),
                type: stringArg(target, 'type'),
                instance: intArg(target, 'instance') ?? 0,
              },
            };
          }),
  };
}

/** Copies the defined values of [entries] into a partial options object. */
function defined<T>(entries: { [K in keyof T]?: T[K] | undefined }): Partial<T> {
  const out: Partial<T> = {};
  for (const [key, value] of Object.entries(entries)) {
    if (value !== undefined) {
      (out as Record<string, unknown>)[key] = value;
    }
  }
  return out;
}

function muxConfig(j: Json): Partial<MuxOptions> {
  const keepAlive = intArg(j, 'keepAliveIntervalMs');
  const options = defined<MuxOptions>({
    maxFrameSize: intArg(j, 'maxFrameSize'),
    maxChannels: intArg(j, 'maxChannels'),
    shortIdsOnly: j['shortIdsOnly'] === undefined ? undefined : boolArg(j, 'shortIdsOnly'),
    keepAliveTimeout: intArg(j, 'keepAliveTimeoutMs'),
    goAwayGrace: intArg(j, 'goAwayGraceMs'),
    announceLimits: j['announceLimits'] === undefined ? undefined : boolArg(j, 'announceLimits'),
    closeConfirmTimeout: intArg(j, 'closeConfirmTimeoutMs'),
    identityTimeout: intArg(j, 'identityTimeoutMs'),
    requireNamedIdent:
      j['requireNamedIdent'] === undefined ? undefined : boolArg(j, 'requireNamedIdent'),
    initialWindow: intArg(j, 'initialWindow'),
    awaitPeerLimits: j['awaitPeerLimits'] === undefined ? undefined : boolArg(j, 'awaitPeerLimits'),
  });
  // 0 disables keep-alive, which the TS options spell as undefined.
  return keepAlive === undefined
    ? options
    : { ...options, keepAliveInterval: keepAlive === 0 ? undefined : keepAlive };
}

function talkConfig(j: Json): Partial<TalkOptions> {
  return defined<TalkOptions>({
    requestTimeout: intArg(j, 'requestTimeoutMs'),
    replyTimeout: intArg(j, 'replyTimeoutMs'),
    bulkThreshold: intArg(j, 'bulkThreshold'),
    maxInlinePayload: intArg(j, 'maxInlinePayload'),
    reassemblyBudget: intArg(j, 'reassemblyBudget'),
    bulkIdleTimeout: intArg(j, 'bulkIdleTimeoutMs'),
    creditTimeout: intArg(j, 'creditTimeoutMs'),
  });
}
