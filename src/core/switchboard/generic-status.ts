/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { Status, type StatusCode, statusCodeName } from '../status.ts';

/**
 * [code] with a short generic reason (`'not found'`, `'permission
 * denied'`, ...), for rejections sent to peers.
 *
 * Rejections must not describe the mesh (instance ids, internal endpoints,
 * resolver state) to a peer that may be untrusted; the details belong in
 * the local log. See the wiki page "Polyverse Switchboard Addressing and
 * Dispatch", section "Dispatch of incoming channels". Internal.
 */
export function genericStatus(code: StatusCode): Status {
  const name = statusCodeName(code) ?? `code ${code}`;
  return Status.of(
    code,
    name.replace(/[A-Z]/g, (c) => ` ${c.toLowerCase()}`),
  );
}
