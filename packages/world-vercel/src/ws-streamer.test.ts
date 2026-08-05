import { decode } from 'cbor-x';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  closeStreamOverWs,
  readStreamOverWs,
  writeMultiStreamOverWs,
  writeStreamOverWs,
} from './ws-streamer.js';
import { WsTransportError } from './ws-transport.js';

interface SubscriptionCallbacks {
  reqId: number;
  onFrame: (frame: {
    meta: Record<string, unknown>;
    body: Uint8Array;
  }) => boolean;
  onError: (err: unknown) => void;
  unsubscribed: boolean;
}

const { fakeTransport } = vi.hoisted(() => ({
  fakeTransport: {
    nextReqId: 1,
    sent: [] as Uint8Array[],
    replies: [] as Array<{
      meta: Record<string, unknown>;
      body: Uint8Array;
    }>,
    errors: [] as unknown[],
    subscriptions: [] as SubscriptionCallbacks[],
    async request(buildFrame: (reqId: number) => Uint8Array) {
      const reqId = this.nextReqId++;
      this.sent.push(buildFrame(reqId));
      const error = this.errors.shift();
      if (error) throw error;
      return (
        this.replies.shift() ?? {
          meta: { reqId, type: 'stream_ack', status: 200 },
          body: new Uint8Array(0),
        }
      );
    },
    async subscribe(
      buildFrame: (reqId: number) => Uint8Array,
      onFrame: SubscriptionCallbacks['onFrame'],
      onError: SubscriptionCallbacks['onError']
    ) {
      const reqId = this.nextReqId++;
      this.sent.push(buildFrame(reqId));
      const record = { reqId, onFrame, onError, unsubscribed: false };
      this.subscriptions.push(record);
      return {
        reqId,
        unsubscribe: () => {
          record.unsubscribed = true;
        },
      };
    },
  },
}));

vi.mock('./ws-transport.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./ws-transport.js')>()),
  resolveWsTransport: () => ({
    transport: fakeTransport,
    wsUrl: 'wss://example.test/api/websockets/v1/runs/run-1',
  }),
}));

function metaOf(raw: Uint8Array): Record<string, unknown> {
  const length = new DataView(
    raw.buffer,
    raw.byteOffset,
    raw.byteLength
  ).getUint32(0, false);
  return decode(raw.subarray(4, 4 + length)) as Record<string, unknown>;
}

function bodyOf(raw: Uint8Array): Uint8Array {
  const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
  const metaLength = view.getUint32(0, false);
  const bodyOffset = 4 + metaLength;
  const bodyLength = view.getUint32(bodyOffset, false);
  return raw.slice(bodyOffset + 4, bodyOffset + 4 + bodyLength);
}

beforeEach(() => {
  fakeTransport.nextReqId = 1;
  fakeTransport.sent.length = 0;
  fakeTransport.replies.length = 0;
  fakeTransport.errors.length = 0;
  fakeTransport.subscriptions.length = 0;
  delete process.env.WORKFLOW_MAX_CHUNKS_PER_REQUEST;
});

describe('WS stream writes', () => {
  it('encodes single and multi writes with the shared body formats', async () => {
    await writeStreamOverWs('run-1', 'output', 'one');
    await writeMultiStreamOverWs('run-1', 'output', ['two', 'three']);

    expect(metaOf(fakeTransport.sent[0])).toEqual({
      reqId: 1,
      type: 'stream_write',
      stream: { name: 'output', count: 1 },
    });
    expect(new TextDecoder().decode(bodyOf(fakeTransport.sent[0]))).toBe('one');
    expect(metaOf(fakeTransport.sent[1])).toEqual({
      reqId: 2,
      type: 'stream_write',
      stream: { name: 'output', count: 2 },
    });
    expect([...bodyOf(fakeTransport.sent[1])]).toEqual([
      0, 0, 0, 3, 116, 119, 111, 0, 0, 0, 5, 116, 104, 114, 101, 101,
    ]);
  });

  it('encodes close as an empty, idempotent request', async () => {
    await closeStreamOverWs('run-1', 'output');
    expect(metaOf(fakeTransport.sent[0])).toEqual({
      reqId: 1,
      type: 'stream_close',
      stream: { name: 'output' },
    });
    expect(bodyOf(fakeTransport.sent[0])).toHaveLength(0);
  });

  it('pages multi-writes at the configured request limit', async () => {
    process.env.WORKFLOW_MAX_CHUNKS_PER_REQUEST = '2';
    await writeMultiStreamOverWs('run-1', 'output', ['a', 'b', 'c']);
    expect(fakeTransport.sent.map((frame) => metaOf(frame))).toMatchObject([
      { stream: { count: 2 } },
      { stream: { count: 1 } },
    ]);
    expect(new TextDecoder().decode(bodyOf(fakeTransport.sent[1]))).toBe('c');
  });

  it('does not retry an ambiguous append failure', async () => {
    fakeTransport.errors.push(new WsTransportError('ack lost'));
    await expect(writeStreamOverWs('run-1', 'output', 'one')).rejects.toThrow(
      'ack lost'
    );
    expect(fakeTransport.sent).toHaveLength(1);
  });

  it('retries an append rejected with 429', async () => {
    fakeTransport.replies.push(
      {
        meta: {
          reqId: 1,
          type: 'stream_ack',
          status: 429,
          retryAfter: '0',
        },
        body: new Uint8Array(0),
      },
      {
        meta: { reqId: 2, type: 'stream_ack', status: 200 },
        body: new Uint8Array(0),
      }
    );
    await writeStreamOverWs('run-1', 'output', 'one');
    expect(fakeTransport.sent).toHaveLength(2);
  });

  it('retries close after an ambiguous transport failure', async () => {
    fakeTransport.errors.push(new WsTransportError('ack lost'));
    await closeStreamOverWs('run-1', 'output');
    expect(fakeTransport.sent).toHaveLength(2);
  });
});

describe('WS stream reads', () => {
  it('forwards chunk bytes, resumes at the server-authoritative index, and completes', async () => {
    const stream = await readStreamOverWs('run-1', 'output', 2);
    const reader = stream.getReader();
    expect(metaOf(fakeTransport.sent[0])).toEqual({
      reqId: 1,
      type: 'stream_read',
      stream: { name: 'output', startIndex: 2 },
    });

    const firstRead = reader.read();
    fakeTransport.subscriptions[0].onFrame({
      meta: { reqId: 1, type: 'stream_chunk', index: 2 },
      body: new Uint8Array([7, 8]),
    });
    expect(await firstRead).toEqual({
      done: false,
      value: new Uint8Array([7, 8]),
    });

    fakeTransport.subscriptions[0].onFrame({
      meta: { reqId: 1, type: 'stream_end', reason: 'resume', nextIndex: 9 },
      body: new Uint8Array(0),
    });
    await vi.waitFor(() => expect(fakeTransport.subscriptions).toHaveLength(2));
    expect(metaOf(fakeTransport.sent[1])).toEqual({
      reqId: 2,
      type: 'stream_read',
      stream: { name: 'output', startIndex: 9 },
    });

    const done = reader.read();
    fakeTransport.subscriptions[1].onFrame({
      meta: {
        reqId: 2,
        type: 'stream_end',
        reason: 'complete',
        nextIndex: 9,
      },
      body: new Uint8Array(0),
    });
    expect(await done).toEqual({ done: true, value: undefined });
  });

  it('sends stream_read_cancel when the consumer cancels', async () => {
    const stream = await readStreamOverWs('run-1', 'output', 0);
    await stream.cancel();

    expect(fakeTransport.subscriptions[0].unsubscribed).toBe(true);
    expect(metaOf(fakeTransport.sent[1])).toEqual({
      reqId: 2,
      type: 'stream_read_cancel',
      readReqId: 1,
    });
  });

  it('errors the readable when stream_end reports an error', async () => {
    const stream = await readStreamOverWs('run-1', 'output', 0);
    const read = stream.getReader().read();
    fakeTransport.subscriptions[0].onFrame({
      meta: {
        reqId: 1,
        type: 'stream_end',
        reason: 'error',
        nextIndex: 0,
        status: 500,
      },
      body: new TextEncoder().encode('upstream failed'),
    });
    await expect(read).rejects.toThrow(/status 500.*upstream failed/);
  });

  it('resumes an abrupt socket loss after the last delivered index', async () => {
    const stream = await readStreamOverWs('run-1', 'output', 3);
    const reader = stream.getReader();
    const first = reader.read();
    fakeTransport.subscriptions[0].onFrame({
      meta: { reqId: 1, type: 'stream_chunk', index: 3 },
      body: new Uint8Array([1]),
    });
    await first;

    fakeTransport.subscriptions[0].onError(new Error('socket lost'));
    await vi.waitFor(() => expect(fakeTransport.subscriptions).toHaveLength(2));
    expect(metaOf(fakeTransport.sent[1])).toMatchObject({
      type: 'stream_read',
      stream: { startIndex: 4 },
    });
    await reader.cancel();
  });
});
