import { envNumber, type Streamer } from '@workflow/world';
import { encodeFrame } from './frames.js';
import { encodeMultiChunks, getMaxChunksPerRequest } from './streamer.js';
import type { APIConfig } from './utils.js';
import {
  resolveWsTransport,
  type WsFrameReply,
  type WsSubscription,
  WsTransportError,
} from './ws-transport.js';

const EMPTY = new Uint8Array(0);
const MAX_RETRIES = 5;
const MAX_CONSECUTIVE_RECONNECTS = 50;
const MAX_TOTAL_RECONNECTS = 1000;

function asBytes(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
}

function responseError(operation: string, reply: WsFrameReply): Error {
  const status = reply.meta.status;
  const detail = new TextDecoder().decode(reply.body);
  return new Error(
    `Stream ${operation} failed over WS: status ${String(status ?? 'unknown')}` +
      (detail ? `: ${detail}` : '')
  );
}

function retryDelay(attempt: number, retryAfter?: unknown): number {
  if (typeof retryAfter === 'string') {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
    const date = Date.parse(retryAfter);
    if (Number.isFinite(date)) return Math.max(0, date - Date.now());
  }
  return Math.min(100 * 2 ** attempt, 5_000);
}

async function wait(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms);
    timer.unref?.();
  });
}

function isRetryableStatus(
  operation: 'write' | 'close' | 'cancel',
  status: unknown
): boolean {
  return (
    status === 429 ||
    (operation !== 'write' &&
      typeof status === 'number' &&
      status >= 500 &&
      status <= 599)
  );
}

function assertSuccessfulReply(
  operation: 'write' | 'close' | 'cancel',
  reply: WsFrameReply
): void {
  const status = reply.meta.status;
  if (
    reply.meta.type !== 'stream_ack' ||
    typeof status !== 'number' ||
    status < 200 ||
    status >= 300
  ) {
    throw responseError(operation, reply);
  }
}

async function requestWithRetry(args: {
  runId: string;
  config?: APIConfig;
  operation: 'write' | 'close' | 'cancel';
  buildFrame: (reqId: number) => Uint8Array;
}): Promise<WsFrameReply> {
  const resolved = resolveWsTransport(args.runId, args.config);
  if (!resolved) throw new Error('WebSocket stream transport is unavailable');

  for (let attempt = 0; ; attempt++) {
    try {
      const reply = await resolved.transport.request(args.buildFrame);
      const status = reply.meta.status;
      if (isRetryableStatus(args.operation, status) && attempt < MAX_RETRIES) {
        await wait(retryDelay(attempt, reply.meta.retryAfter));
        continue;
      }
      assertSuccessfulReply(args.operation, reply);
      return reply;
    } catch (err) {
      const retryableTransport =
        err instanceof WsTransportError &&
        (args.operation !== 'write' || err.delivery === 'not_sent');
      if (!retryableTransport || attempt >= MAX_RETRIES) throw err;
      await wait(retryDelay(attempt));
    }
  }
}

export async function writeStreamOverWs(
  runId: string,
  name: string,
  chunk: string | Uint8Array,
  config?: APIConfig
): Promise<void> {
  const body = asBytes(chunk);
  await requestWithRetry({
    runId,
    config,
    operation: 'write',
    buildFrame: (reqId) =>
      encodeFrame(
        { reqId, type: 'stream_write', stream: { name, count: 1 } },
        body
      ),
  });
}

export async function writeMultiStreamOverWs(
  runId: string,
  name: string,
  chunks: (string | Uint8Array)[],
  config?: APIConfig
): Promise<void> {
  const maxChunksPerRequest = getMaxChunksPerRequest();
  for (let i = 0; i < chunks.length; i += maxChunksPerRequest) {
    const batch = chunks.slice(i, i + maxChunksPerRequest);
    const body =
      batch.length === 1 ? asBytes(batch[0]) : encodeMultiChunks(batch);
    await requestWithRetry({
      runId,
      config,
      operation: 'write',
      buildFrame: (reqId) =>
        encodeFrame(
          {
            reqId,
            type: 'stream_write',
            stream: { name, count: batch.length },
          },
          body
        ),
    });
  }
}

export async function closeStreamOverWs(
  runId: string,
  name: string,
  config?: APIConfig
): Promise<void> {
  await requestWithRetry({
    runId,
    config,
    operation: 'close',
    buildFrame: (reqId) =>
      encodeFrame({ reqId, type: 'stream_close', stream: { name } }, EMPTY),
  });
}

export async function readStreamOverWs(
  runId: string,
  name: string,
  startIndex: number | undefined,
  config?: APIConfig
): Promise<ReadableStream<Uint8Array>> {
  const resolved = resolveWsTransport(runId, config);
  if (!resolved) throw new Error('WebSocket stream transport is unavailable');
  const { transport } = resolved;

  let active: WsSubscription | undefined;
  let cancelled = false;
  let nextIndex = startIndex;
  let controllerRef: ReadableStreamDefaultController<Uint8Array>;
  let consecutiveReconnects = 0;
  let totalReconnects = 0;

  const cancelRead = async (readReqId: number): Promise<void> => {
    await requestWithRetry({
      runId,
      config,
      operation: 'cancel',
      buildFrame: (reqId) =>
        encodeFrame({ reqId, type: 'stream_read_cancel', readReqId }, EMPTY),
    });
  };

  const recordReconnect = (): void => {
    consecutiveReconnects++;
    totalReconnects++;
    const maxConsecutive = envNumber(
      'WORKFLOW_FRAMED_STREAM_MAX_RECONNECTS',
      MAX_CONSECUTIVE_RECONNECTS,
      { integer: true, min: 1 }
    );
    const maxTotal = envNumber(
      'WORKFLOW_FRAMED_STREAM_MAX_TOTAL_RECONNECTS',
      MAX_TOTAL_RECONNECTS,
      { integer: true, min: 1 }
    );
    if (consecutiveReconnects > maxConsecutive) {
      throw new Error(
        `Stream "${name}" exceeded maximum reconnection attempts (${maxConsecutive})`
      );
    }
    if (totalReconnects > maxTotal) {
      throw new Error(
        `Stream "${name}" exceeded maximum total reconnection attempts (${maxTotal})`
      );
    }
  };

  let subscribe: (index: number | undefined) => Promise<void>;

  const reconnect = (index: number | undefined): void => {
    try {
      recordReconnect();
      void subscribe(index).catch((err) => controllerRef.error(err));
    } catch (err) {
      controllerRef.error(err);
    }
  };

  const handleEnd = (reply: WsFrameReply): boolean => {
    if (reply.meta.reason === 'complete') {
      controllerRef.close();
      return true;
    }
    if (
      reply.meta.reason === 'resume' &&
      typeof reply.meta.nextIndex === 'number'
    ) {
      nextIndex = reply.meta.nextIndex;
      reconnect(nextIndex);
      return true;
    }
    controllerRef.error(responseError('read', reply));
    return true;
  };

  const handleFrame = (reply: WsFrameReply): boolean => {
    if (cancelled) return true;
    if (reply.meta.type === 'stream_chunk') {
      if (typeof reply.meta.index !== 'number') {
        controllerRef.error(new Error('stream_chunk carried no index'));
        return true;
      }
      nextIndex = reply.meta.index + 1;
      consecutiveReconnects = 0;
      // Stream chunks are opaque bytes. Higher layers put their own
      // serialization framing inside each stored chunk, so forwarding the
      // WS body verbatim preserves the same byte stream as the HTTP path.
      controllerRef.enqueue(reply.body);
      return false;
    }
    if (reply.meta.type === 'stream_end') return handleEnd(reply);
    if (reply.meta.type === 'error') {
      controllerRef.error(responseError('read', reply));
      return true;
    }
    controllerRef.error(
      new Error(`Unexpected WS stream frame: ${String(reply.meta.type)}`)
    );
    return true;
  };

  const handleTransportError = (err: unknown): void => {
    if (cancelled) return;
    // Once the server has supplied an absolute chunk index, an abrupt socket
    // loss can safely resume there. A negative tail-relative read that failed
    // before its first chunk has no absolute resume point.
    if (nextIndex === undefined || nextIndex >= 0) reconnect(nextIndex);
    else controllerRef.error(err);
  };

  subscribe = async (index: number | undefined): Promise<void> => {
    try {
      const subscription = await transport.subscribe(
        (reqId) =>
          encodeFrame(
            {
              reqId,
              type: 'stream_read',
              stream: {
                name,
                ...(typeof index === 'number' ? { startIndex: index } : {}),
              },
            },
            EMPTY
          ),
        handleFrame,
        handleTransportError
      );
      if (cancelled) {
        subscription.unsubscribe();
        await cancelRead(subscription.reqId);
      } else active = subscription;
    } catch (err) {
      if (!cancelled) throw err;
    }
  };

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      controllerRef = controller;
      await subscribe(startIndex);
    },
    async cancel() {
      cancelled = true;
      const readReqId = active?.reqId;
      active?.unsubscribe();
      active = undefined;
      if (readReqId === undefined) return;
      await cancelRead(readReqId);
    },
  });
}

export type WsStreamMethods = Pick<
  Streamer['streams'],
  'write' | 'writeMulti' | 'close' | 'get'
>;
