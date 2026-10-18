/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (Talk, docs): the wiki page Channels-and-Talk says a
// received message's `signal` is "aborted when the requester cancels, the
// responder timeout passes or the channel closes: pass it to what the
// handler awaits". For a plain message (no reply expected) it never
// aborts, also when the channel closes: `IncomingMessage.#markCancelled`
// runs only for requests (`_abandon` from `#terminate` covers the requests
// in `#incoming`), as `onCancel` never resolves for non-requests (Dart's
// contract, kept by contract.ts). A handler that passes the signal of a
// plain message to its work is never stopped. The request case is the
// control. The plain-message test is `test.fails` while the two disagree:
// either the signal aborts on the channel's end, or the wiki says "of a
// request".

import { expect, test } from 'vitest';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { flush } from '../../support/talk/helpers.ts';
import { memoryMuxPair } from '../../support/talk/memory-mux.ts';

const empty = new Uint8Array(0);

async function received(procedure: string, request: boolean): Promise<[TalkChannel, TalkMessage]> {
  const [client, server] = memoryMuxPair();
  const talk = new TalkChannel(client.open(empty));
  const responder = new TalkChannel(await server.accept());
  const got: TalkMessage[] = [];
  void responder.serve((m) => {
    got.push(m);
  });
  if (request) {
    void talk.request(procedure, empty).catch(() => {});
  } else {
    await talk.send(procedure, empty);
  }
  await flush();
  expect(got).toHaveLength(1);
  return [talk, got[0] as TalkMessage];
}

test('control: the signal of a request aborts when the channel closes', async () => {
  const [talk, message] = await received('Q', true);
  const signal = message.signal;
  await talk.close();
  await flush();
  expect(signal.aborted).toBe(true);
});

test.fails('the signal of a plain message aborts when the channel closes, as the wiki says', async () => {
  const [talk, message] = await received('P', false);
  const signal = message.signal;
  await talk.close();
  await flush();
  expect(signal.aborted).toBe(true);
});
