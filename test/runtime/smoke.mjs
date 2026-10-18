/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The built package on a runtime, as a consumer imports it: a service node and
// a client node in one process exchange Talk requests (a request, a streamed
// request and a 256 KiB payload) over TCP, WebSocket, raw QUIC and
// WebTransport, through `@kaetemi/switchboard/node` alone. The `deno` CI job
// runs it in a project where the package, the prebuilt addon of the platform
// and `ws` are installed from tarballs, with the permissions a deployment
// grants:
//
//   deno run --allow-net --allow-read --allow-env --allow-ffi smoke.mjs
//
// It runs as well on Node 22 or later and on Bun (`node smoke.mjs`,
// `bun smoke.mjs`). `--transports tcp,ws` limits it to some transports, such
// as where no addon is installed. It prints one line per transport and exits
// non-zero on the first failure.
import {
  EndpointResolver,
  nativeQuic,
  nativeQuicAvailability,
  Status,
  StatusCode,
  Switchboard,
  selfSignedCertificate,
} from '@kaetemi/switchboard/node';

const all = ['tcp', 'ws', 'quic', 'webtransport'];
const flag = process.argv.indexOf('--transports');
const transports = flag === -1 ? all : (process.argv[flag + 1] ?? '').split(',');
for (const t of transports) {
  if (!all.includes(t)) throw new Error(`unknown transport ${t}; one of ${all.join(', ')}`);
}

/** [promise], failing after [ms]. */
function within(promise, ms, what) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${what}: not settled within ${ms} ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function check(actual, expected, what) {
  if (actual !== expected) {
    throw new Error(`${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

/** The service: UPPER, COUNT (streamed), ECHO (bytes). */
function service(incoming) {
  void (async () => {
    for await (const message of incoming.talk().messages) {
      switch (message.procedureName) {
        case 'UPPER':
          await message.reply(message.text().toUpperCase());
          break;
        case 'COUNT':
          for (let i = 1; i <= Number(message.text()); i++) await message.replyItem(String(i));
          await message.reply('done');
          break;
        case 'ECHO':
          await message.reply(message.payload);
          break;
        default:
          await message.replyAbort(Status.of(StatusCode.unimplemented));
      }
    }
  })().catch(() => {});
}

/** A client node opens a Talk channel to the service at [endpoint] and exercises it. */
async function exercise(endpoint, options) {
  const client = new Switchboard({ resolver: new EndpointResolver(endpoint), ...options });
  try {
    const talk = await within(client.openTalk('echo'), 10_000, `${endpoint}: openTalk`);
    const reply = await within(talk.request('UPPER', 'hello'), 10_000, 'UPPER');
    check(reply.text(), 'HELLO', 'UPPER');
    const stream = talk.streamRequest('COUNT', '3');
    const items = [];
    for await (const item of stream) items.push(item.text());
    check(items.join(' '), '1 2 3', 'the COUNT items');
    check((await within(stream.done, 10_000, 'COUNT')).text(), 'done', 'the COUNT reply');
    const big = Uint8Array.from({ length: 256 * 1024 }, (_, i) => (i * 31) & 0xff);
    const echoed = await within(talk.request('ECHO', big), 10_000, 'ECHO');
    check(echoed.payload.length, big.length, 'the ECHO length');
    check(
      echoed.payload.every((b, i) => b === big[i]),
      true,
      'the ECHO bytes',
    );
    await talk.close();
    return talk.raw?.connection?.constructor.name ?? 'unknown';
  } finally {
    await client.close();
  }
}

const runtime =
  typeof globalThis.Deno !== 'undefined'
    ? `Deno ${globalThis.Deno.version.deno}`
    : typeof globalThis.Bun !== 'undefined'
      ? `Bun ${globalThis.Bun.version}`
      : `Node ${process.versions.node}`;
console.log(`smoke: ${runtime} on ${process.platform}-${process.arch}`);

const needsQuic = transports.includes('quic') || transports.includes('webtransport');
if (needsQuic) {
  const availability = nativeQuicAvailability();
  if (!availability.available) throw new Error(availability.reason);
  console.log(`smoke: the QUIC addon from ${availability.path}`);
}

const certificate = needsQuic ? selfSignedCertificate('127.0.0.1') : undefined;
const server = new Switchboard(needsQuic ? { quic: nativeQuic() } : {});
server.registerService('echo', service);
try {
  for (const transport of transports) {
    let endpoint;
    let options = {};
    switch (transport) {
      case 'tcp':
        endpoint = await server.listenTcp('127.0.0.1', 0);
        break;
      case 'ws':
        endpoint = await server.listenWebSocket('127.0.0.1', 0);
        break;
      case 'quic':
        endpoint = await server.listenQuic('127.0.0.1', 0, { certificate });
        options = { quic: nativeQuic() };
        break;
      case 'webtransport':
        endpoint = await server.listenWebTransport('127.0.0.1', 0, { path: '/sb', certificate });
        options = {
          quic: nativeQuic(),
          quicBinding: { serverCertificateHashes: [certificate.sha256] },
        };
        break;
    }
    const connection = await exercise(endpoint, options);
    console.log(`smoke: ${transport} ok (${endpoint}, ${connection})`);
  }
} finally {
  await server.close();
}
console.log('smoke: done');
