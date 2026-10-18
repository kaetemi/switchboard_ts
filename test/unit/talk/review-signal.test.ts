/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (Talk, docs): the wiki page Channels-and-Talk said a
// received message's `signal` is "aborted when the requester cancels, the
// responder timeout passes or the channel closes: pass it to what the
// handler awaits", without saying that this is a request's. For a plain
// message (no reply expected) it never aborts, also when the channel
// closes: `IncomingMessage.#markCancelled` runs only for requests
// (`_abandon` from `#terminate` covers the requests in `#incoming`), as
// `onCancel` never resolves for non-requests (Dart's contract, kept by
// contract.ts). The wiki now says it is a request's signal; these tests
// pin both halves of what it says.

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

test('the signal of a request aborts when the channel closes', async () => {
  const [talk, message] = await received('Q', true);
  const signal = message.signal;
  await talk.close();
  await flush();
  expect(signal.aborted).toBe(true);
});

test('the signal of a plain message does not abort when the channel closes: only a request is cancelled', async () => {
  const [talk, message] = await received('P', false);
  const signal = message.signal;
  await talk.close();
  await flush();
  expect(signal.aborted).toBe(false);
});
