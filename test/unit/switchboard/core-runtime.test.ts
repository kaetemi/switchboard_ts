/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The core runtime end to end: two nodes with no runtime given run the
// frame mux over the process-wide `mem://` listeners. The other node suites
// run over the toy mux of `test/support/node/`.

import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { Name } from '../../../src/core/name.ts';
import { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { nextText } from '../../support/node/channels.ts';
import { bytes, onCleanup } from '../../support/node/harness.ts';

/** A node over the core runtime, closed after the test. */
function coreNode(): Switchboard {
  const s = new Switchboard();
  onCleanup(() => s.close());
  return s;
}

describe('the core runtime', () => {
  test('runs the frame mux over mem:// listeners', async () => {
    const server = coreNode();
    const client = coreNode();
    const echo = Name.of('echo');
    server.registerService(echo, (incoming) => {
      void (async () => {
        for await (const frame of incoming.channel.readable) {
          await incoming.channel.send(bytes(`echo ${utf8Decode(frame)}`));
        }
      })().catch(() => {});
    });
    const uri = await server.listenMemory();
    const connection = await client.connect(uri);
    expect(connection).toBeInstanceOf(FrameMuxConnection);
    const channel = await client.openChannelAt(uri, new ChannelAddress({ type: echo }));
    await channel.send(bytes('one'));
    expect(await nextText(channel)).toBe('echo one');
    await channel.send(bytes('two'));
    expect(await nextText(channel)).toBe('echo two');
    await channel.close();
  });
});
