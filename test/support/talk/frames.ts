/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Talk frames the tests write by hand: credits, stream requests and items
 * for the item window tests, BULK references and `_bulk` open payloads for
 * the bulk tests.
 */

import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { TalkBulkOpen, TalkBulkReference } from '../../../src/core/talk/talk-bulk.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';

/** A credit for request [id] of [items] items. */
export function credit(id: number, items: number): TalkFrame {
  return new TalkFrame({
    kind: TalkKind.extend,
    requestId: id,
    payload: TalkFrame.creditPayload(items),
  });
}

/** A stream request [id] "LIST" with [window]. */
export function streamRequestFrame(id: number, window: number): TalkFrame {
  return new TalkFrame({
    kind: TalkKind.message,
    procedure: Name.of('LIST'),
    requestId: id,
    stream: true,
    window,
  });
}

/** A stream item for request [id], its payload the one byte [value]. */
export function itemFrame(id: number, value: number): TalkFrame {
  return new TalkFrame({
    kind: TalkKind.streamItem,
    responseId: id,
    payload: Uint8Array.of(value),
  });
}

/** The encoded frame of [init] (a message unless `kind` says otherwise) with the `BULK` flag, referencing bulk channel [number] of [length] bytes (unknown when undefined). */
export function bulkFrame(
  init: {
    procedure?: string | undefined;
    requestId?: number | undefined;
    responseId?: number | undefined;
    kind?: TalkKind | undefined;
  },
  number: number,
  length?: number,
): Uint8Array {
  return new TalkFrame({
    kind: init.kind ?? TalkKind.message,
    procedure: init.procedure === undefined ? undefined : Name.of(init.procedure),
    requestId: init.requestId,
    responseId: init.responseId,
    bulk: true,
    payload: new TalkBulkReference(number, length).encode(),
  }).encode();
}

/** The encoded frame of a BULK message [procedure] referencing bulk [number]. */
export function bulkMessage(
  procedure: string,
  number: number,
  options: { requestId?: number | undefined; length?: number | undefined } = {},
): Uint8Array {
  return bulkFrame({ procedure, requestId: options.requestId }, number, options.length);
}

/** The `_bulk` open payload of bulk channel [number] for [parent]. */
export function bulkOpen(parent: MuxChannel | number, number: number): Uint8Array {
  return new TalkBulkOpen(typeof parent === 'number' ? parent : parent.id, number).encode();
}
