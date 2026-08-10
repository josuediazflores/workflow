/**
 * Benchmark runner measuring the workflow runtime's core latency metrics
 * against a deployed workbench app.
 *
 * Every run is triggered through an in-deployment route (`POST /api/bench` on
 * the workbench app) rather than by calling `start()` from this CI process. The
 * route stamps `clientStart` with the deployment's own clock immediately before
 * `start()`, so the CI runner's request — and its entire path through
 * api.vercel.com — sits OUTSIDE every measured window. As a result none of the
 * metrics below depend on the CI runner's clock or its network path to the
 * proxy; they are computed purely from Vercel-side timestamps.
 *
 * Metrics (all in milliseconds, reported as best/p75/p90/p99; avg is kept in
 * the JSON for reference but not shown in the PR comment):
 *
 * The best (fastest) sample is reported alongside the upper percentiles so
 * warm-start latency (the fast floor) is visible next to the cold-start tail:
 * the workbench deployment cold-starts the `/flow` invocation for a large
 * fraction of runs (bursty, low-traffic), which inflates p75+. Cold starts are
 * kept in the numbers on purpose — they are part of real bursty-workload
 * latency — and the best sample shows what a fully warm trigger looks like.
 *
 * - TTFS  (time to first step): `steps[0].start` (first step body execution,
 *          deployment clock) minus the in-deployment `clientStart` returned by
 *          the trigger route. Because `start()` runs inside the deployment, the
 *          turbo path (no hooks) can exercise the runtime's in-process fast
 *          path; the non-turbo path (a hook registered before the step)
 *          exercises the dispatch path. Both are proxy-independent. TTFS
 *          includes the VQS dispatch hop and any `/flow` cold start (see the
 *          best-sample note above).
 * - STSO  (step-to-step overhead): gap between consecutive step body
 *          executions (`steps[i].start - steps[i-1].end`) in a workflow with
 *          many trivial sequential steps. Both timestamps come from step
 *          bodies on the deployment. Reported split by whether the step
 *          ending the gap was 'inline' (same warm process as the step before
 *          it) or a 'queue-hop' (first step of a fresh process — cold start
 *          or a redispatch after the prior invocation's duration limit) —
 *          the two have very different cost profiles and averaging them
 *          together hides that. The workflow itself tags each step's kind
 *          (see workflows/97_bench.ts's `stepKind`).
 * - WO    (workflow overhead): total time the run spends outside of step
 *          bodies over the whole sequential run, from the in-deployment
 *          `clientStart` to the end of the last step body:
 *          `(lastStep.end - clientStart) - Σ(step durations)`. Measured on the
 *          sequential scenario only — on a single-step workflow WO reduces
 *          algebraically to TTFS.
 * - SL    (stream latency): live write->read propagation for the default
 *          output stream, measured entirely on the deployment by
 *          `benchSlWorkflow`: a reader step and a writer step run in parallel,
 *          the reader blocks on the first chunk, and the workflow returns both
 *          the writer's `writtenAt` and the reader's `readAt`. SL is
 *          `readAt - writtenAt`, so it excludes the api.vercel.com read path
 *          the old client-observed metric included.
 * - SO    (stream overhead): end-to-end write+consume time in excess of a
 *          modelled generation window, measured on the deployment by
 *          `benchSoWorkflow`. A writer streams deterministic variable-length
 *          LLM-token deltas at a fixed rate for a fixed duration while a
 *          parallel reader drains the whole stream; SO is
 *          `(doneAt - writtenAt) - chunkCount*intervalMs`, i.e. the
 *          overhead/backpressure the stream adds on top of the token rate. Same
 *          setup as SL, but the reader stamps `doneAt` after the last chunk
 *          rather than `readAt` on the first. Measured for two payload shapes
 *          (raw text vs AI-SDK-style structured deltas) so the SO delta between
 *          them isolates serialization cost.
 * - CRTT  (chunk round-trip time): per-chunk write->read latency for the same
 *          paced LLM-shaped stream, measured on the deployment by
 *          `benchCrttWorkflow`. The "round trip" is deployment -> stream
 *          backend -> reader on the same deployment (which is what keeps both
 *          timestamps on one clock domain) — not an echo back to the writer.
 *          Every delta embeds `{ seq, writtenAt }` (the SL scenario's
 *          payload-embedded-timestamp trick applied to every chunk) and the
 *          reader stamps each chunk's arrival. Samples are aggregated INSIDE
 *          the reader step into chunk-index buckets (seq 0 = stream-open
 *          write / seq 1-20 = warmup / seq 21+ = steady state), a fixed
 *          log-bin histogram per bucket, and two mean-RTT profiles: per tenth
 *          of the stream (the drift readout — trends don't bucket well) and
 *          per log size bin (the size→latency curve, fed by a size-sweep
 *          variant whose deltas are padded in rotation across log-spaced
 *          sizes, ~160B to ~12KB serialized). The
 *          runner merges the per-iteration summaries (exact best/avg/count and
 *          histograms; percentile-of-percentiles for p50-p99 — see
 *          mergeRttSummaries). Only the two per-variant pooled rows render in
 *          the PR comment (main table + a collapsed sparkline drill-down with
 *          the profile lines); the per-index rows are flagged `detail` and
 *          live in the results JSON only — measured flat across runs, they
 *          are kept as investigation data, not rendered rows. No targets yet:
 *          targets come from provider-cadence measurement, separately.
 *
 * Scenarios (defined in workbench/example/workflows/97_bench.ts):
 *
 * 1. benchStepWorkflow            — 1 no-op step, turbo mode → TTFS (turbo)
 * 2. benchStreamWorkflow          — 1 streaming step, turbo mode → TTFS (turbo)
 * 3. benchHookStreamWorkflow      — hook + 1 step, non-turbo → TTFS (non-turbo)
 * 4. benchSequentialStepsWorkflow — 1020 trivial sequential steps → STSO + WO
 * 5. benchSlWorkflow              — parallel reader/writer steps → SL
 * 6. benchSoWorkflow              — paced LLM-shaped stream, drained → SO
 *                                   (run in text and structured payload modes)
 * 7. benchCrttWorkflow            — paced stream of self-timestamping chunks →
 *                                   CRTT (run in llm-shaped and size-sweep
 *                                   variants)
 *
 * Each scenario runs many iterations (env-tunable, see BENCH_* below) so the
 * percentiles are computed from real samples.
 *
 * The backend is selected exactly like the e2e tests (setupWorld): Vercel when
 * WORKFLOW_VERCEL_ENV is set, Postgres when WORKFLOW_TARGET_WORLD is
 * @workflow/world-postgres, local filesystem otherwise. Because SL is now
 * measured inside the workflow (not by a reader in this process), it no longer
 * depends on `run.getReadable()` working across processes; CI still runs this
 * file against Vercel only.
 *
 * All timestamps are deployment-side, so the only residual skew is intra-Vercel
 * (between step-runner instances in the same region), NTP-bounded and small
 * relative to the measured values.
 */

import fs from 'node:fs';
import path from 'node:path';
import { afterAll, beforeAll, describe, test } from 'vitest';
import { getTrustedSourcesHeaders } from '../../../scripts/trusted-sources-headers.mjs';
import {
  type BenchRttMeanProfile,
  type BenchRttSummary,
  mergeMeanProfiles,
  mergeRttSummaries,
  RTT_HIST_EDGES_MS,
  RTT_INDEX_BUCKETS,
} from '../../../workbench/example/workflows/97_bench_rtt';
import { getRun } from '../src/runtime';
import { setupWorld } from './utils';

const deploymentUrl = process.env.DEPLOYMENT_URL;
if (!deploymentUrl) {
  throw new Error('`DEPLOYMENT_URL` environment variable is not set');
}

setupWorld(deploymentUrl);

const envInt = (name: string, fallback: number, min = 1): number => {
  const raw = process.env[name];
  if (!raw) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value) || value < min) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }
  return value;
};

// Iteration counts. The stream/hook/SL scenarios yield one sample per
// iteration; the sequential scenario yields (stepCount - 1) STSO samples per
// iteration, so a single long run already provides solid percentiles.
const STREAM_ITERATIONS = envInt('BENCH_STREAM_ITERATIONS', 30);
const SL_ITERATIONS = envInt('BENCH_SL_ITERATIONS', STREAM_ITERATIONS);
const SO_ITERATIONS = envInt('BENCH_SO_ITERATIONS', STREAM_ITERATIONS);
// Each CRTT iteration yields one RTT sample per chunk (300 by default), so
// fewer iterations than SO already give thousands of samples per bucket.
const CRTT_ITERATIONS = envInt('BENCH_CRTT_ITERATIONS', 10);
const SEQUENTIAL_ITERATIONS = envInt('BENCH_SEQUENTIAL_ITERATIONS', 1);
const SEQUENTIAL_STEP_COUNT = envInt('BENCH_SEQUENTIAL_STEP_COUNT', 1020);
const WARMUP_ITERATIONS = envInt('BENCH_WARMUP_ITERATIONS', 2, 0);

// Methodology version — bump whenever the measurement window changes in a way
// that makes numbers incomparable across runs (e.g. the switch from a
// CI/proxy-inclusive clock to the in-deployment trigger). The PR-comment
// renderer keys baseline deltas on this, so old-methodology baselines on `main`
// are not diffed against new-methodology runs (deltas stay blank until `main`
// has produced a same-version baseline). v2 = in-deployment trigger.
const BENCH_METHODOLOGY_VERSION = 2;

// Per-metric latency targets (ms) rendered as 🟢/🔴 marks in the PR comment.
// Provisional: now that the proxy leg is out of every window, these will be
// re-tightened once a few in-deployment baselines land.
const TTFS_TARGETS = { p75: 200, p90: 300, p99: 600 };
const SL_TARGETS = { p75: 50, p90: 60, p99: 125 };

// SO scenario: model a haiku-size LLM streaming tokens — ~100 tokens/sec, each
// token a 4-byte chunk, for 3 seconds (300 chunks). The writer paces itself so
// the write phase spans exactly `SO_CHUNK_COUNT * SO_INTERVAL_MS` ms; SO is the
// end-to-end write+consume time beyond that window (see runSoIteration). These
// derive `SO_NOMINAL_DURATION_MS`, the single value subtracted from the
// measured span, so the workflow's write span and the subtraction never drift.
const SO_CHUNK_RATE_PER_SEC = envInt('BENCH_SO_CHUNK_RATE', 100);
const SO_DURATION_SECONDS = envInt('BENCH_SO_DURATION_SECONDS', 3);
const SO_CHUNK_COUNT = SO_CHUNK_RATE_PER_SEC * SO_DURATION_SECONDS;
const SO_INTERVAL_MS = 1000 / SO_CHUNK_RATE_PER_SEC;
const SO_NOMINAL_DURATION_MS = SO_CHUNK_COUNT * SO_INTERVAL_MS;
// Provisional, like TTFS/SL above: re-tighten once in-deployment baselines land.
const SO_TARGETS = { p75: 250, p90: 500, p99: 1000 };

// Guard timeouts so a single stuck run fails fast instead of eating the job.
const RUN_TIMEOUT_MS = envInt('BENCH_RUN_TIMEOUT_MS', 120_000);
// Preflight guard: a trivial 1-step run must complete within this window
// before any scenario spends its attempt budget (see beforeAll below).
const PREFLIGHT_TIMEOUT_MS = envInt('BENCH_PREFLIGHT_TIMEOUT_MS', 180_000);
// An iteration can flake on transient network errors; grant each scenario a
// bounded fraction of spare (retry) attempts on top of its iteration count.
const MAX_FAILURE_RATIO = 0.2;
// When a scenario has produced zero successful iterations after this many
// attempts, the target is systematically broken (not flaking) — abort the
// scenario instead of burning the full attempt budget at RUN_TIMEOUT_MS per
// attempt.
const ZERO_SUCCESS_ABORT_ATTEMPTS = 3;

interface BenchStepTiming {
  start: number;
  end: number;
  /** 'queue-hop' if this was the first step body executed in its process
   * (cold start or a fresh dispatch after the prior invocation ended);
   * 'inline' for later steps in the same warm process. Set by the workflow
   * itself (see workflows/97_bench.ts) — ground truth, not inferred. */
  kind: 'inline' | 'queue-hop';
}

interface BenchStreamLatency {
  writtenAt: number;
  readAt: number;
}

interface BenchStreamOverhead {
  writtenAt: number;
  doneAt: number;
  received: number;
}

interface StreamIterationResult {
  runId: string;
  /** `steps[0].start - clientStart`, both deployment-side clocks. */
  ttfsMs: number;
}

interface SequentialIterationResult {
  runId: string;
  /** Datadog trace id for the `/api/bench` request that started this run, when
   * the deployment's route reports one (older deployments won't). */
  traceId?: string;
  /** STSO gaps preceding an 'inline' step (same warm process as the step
   * before it) — the framework's pure step-to-step overhead. */
  stsoInlineMs: number[];
  /** STSO gaps preceding a 'queue-hop' step (first step of a fresh process:
   * cold start or a redispatch via the queue after the prior invocation's
   * duration limit) — dispatch + reinit overhead, not step-body cost. */
  stsoQueueHopMs: number[];
  /** Whole-run workflow overhead, anchored on the in-deployment clientStart. */
  woMs: number;
}

interface SlIterationResult {
  runId: string;
  /** `readAt - writtenAt`, both deployment-side step-body clocks. */
  slMs: number;
}

interface SoIterationResult {
  runId: string;
  /** `(doneAt - writtenAt) - SO_NOMINAL_DURATION_MS`, deployment-side clocks. */
  soMs: number;
}

/** Mirrors BenchChunkRttResult in workflows/97_bench.ts: per-bucket RTT
 * summaries aggregated inside the reader step (buckets without samples are
 * absent). */
interface BenchChunkRttResult {
  received: number;
  all?: BenchRttSummary;
  byIndex: Partial<Record<string, BenchRttSummary>>;
  progress?: BenchRttMeanProfile;
  size?: BenchRttMeanProfile;
}

interface CrttIterationResult {
  runId: string;
  crtt: BenchChunkRttResult;
}

/** Response shape of the in-deployment `POST /api/bench` trigger route. */
interface BenchTriggerResponse {
  runId: string;
  /** Date.now() stamped in the route immediately before start(). */
  clientStart: number;
  /** Datadog trace id for this request, when the route reports one. */
  traceId?: string;
}

function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  label: string
): Promise<T> {
  let timer: NodeJS.Timeout;
  return Promise.race([
    promise.finally(() => clearTimeout(timer)),
    new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`Timed out after ${ms}ms: ${label}`)),
        ms
      );
      // Don't keep the process alive just for the guard.
      timer.unref?.();
    }),
  ]);
}

/**
 * Trigger a benchmark workflow via the in-deployment route so `clientStart` is
 * stamped by the deployment's clock (excluding the CI->ingress request path).
 * Returns the created run id and that anchor.
 */
async function triggerBenchRun(
  workflowFn: string,
  args: unknown[] = []
): Promise<BenchTriggerResponse> {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    ...(await getTrustedSourcesHeaders()),
  };
  const response = await fetch(`${deploymentUrl}/api/bench`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ workflowFn, args }),
  });
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(
      `bench trigger for ${workflowFn} failed: ${response.status} ${body.slice(0, 300)}`
    );
  }
  const data = (await response.json()) as Partial<BenchTriggerResponse>;
  if (typeof data.runId !== 'string' || typeof data.clientStart !== 'number') {
    throw new Error(
      `bench trigger for ${workflowFn} returned malformed body: ${JSON.stringify(data)?.slice(0, 200)}`
    );
  }
  return {
    runId: data.runId,
    clientStart: data.clientStart,
    // Optional: a deployment built before the route reported it simply logs
    // the run id without a trace link.
    traceId: typeof data.traceId === 'string' ? data.traceId : undefined,
  };
}

/** Poll a run's return value to completion (the handle polls internally). */
async function getReturnValue(runId: string): Promise<unknown> {
  const run = await getRun(runId);
  return run.returnValue;
}

function timingsFromReturnValue(
  value: unknown,
  runId: string
): BenchStepTiming[] {
  const steps = (value as { steps?: BenchStepTiming[] } | undefined)?.steps;
  if (!Array.isArray(steps) || steps.length === 0) {
    throw new Error(
      `Run ${runId} returned no step timings: ${JSON.stringify(value)?.slice(0, 200)}`
    );
  }
  for (const step of steps) {
    if (typeof step?.start !== 'number' || typeof step?.end !== 'number') {
      throw new Error(
        `Run ${runId} returned malformed step timing: ${JSON.stringify(step)}`
      );
    }
  }
  return steps;
}

/**
 * WO: total time outside of step bodies, from `anchorMs` (the in-deployment
 * clientStart) to the last step body's exit. Clamped at 0 to absorb small
 * intra-Vercel clock skew.
 */
function workflowOverheadMs(
  anchorMs: number,
  steps: BenchStepTiming[]
): number {
  const lastEnd = steps[steps.length - 1].end;
  const inStep = steps.reduce((sum, s) => sum + (s.end - s.start), 0);
  return Math.max(0, lastEnd - anchorMs - inStep);
}

async function runStreamIteration(
  workflowFn: string
): Promise<StreamIterationResult> {
  const { runId, clientStart } = await triggerBenchRun(workflowFn);
  try {
    const returnValue = await withTimeout(
      getReturnValue(runId),
      RUN_TIMEOUT_MS,
      `${workflowFn} returnValue (run ${runId})`
    );
    const steps = timingsFromReturnValue(returnValue, runId);
    return {
      runId,
      // Both timestamps are deployment-side; clamp to absorb tiny skew.
      ttfsMs: Math.max(0, steps[0].start - clientStart),
    };
  } catch (error) {
    (error as Error).message += ` (run ${runId})`;
    throw error;
  }
}

async function runSequentialIteration(
  stepCount: number
): Promise<SequentialIterationResult> {
  const { runId, clientStart, traceId } = await triggerBenchRun(
    'benchSequentialStepsWorkflow',
    [stepCount]
  );
  try {
    const returnValue = await withTimeout(
      getReturnValue(runId),
      RUN_TIMEOUT_MS + stepCount * 2_000,
      `benchSequentialStepsWorkflow returnValue (run ${runId})`
    );
    const steps = timingsFromReturnValue(returnValue, runId);
    if (steps.length !== stepCount) {
      throw new Error(
        `Run ${runId} returned ${steps.length} step timings, expected ${stepCount}`
      );
    }

    const stsoInlineMs: number[] = [];
    const stsoQueueHopMs: number[] = [];
    for (let i = 1; i < steps.length; i++) {
      const gap = steps[i].start - steps[i - 1].end;
      (steps[i].kind === 'queue-hop' ? stsoQueueHopMs : stsoInlineMs).push(gap);
    }

    return {
      runId,
      traceId,
      stsoInlineMs,
      stsoQueueHopMs,
      woMs: workflowOverheadMs(clientStart, steps),
    };
  } catch (error) {
    (error as Error).message += ` (run ${runId})`;
    throw error;
  }
}

async function runSlIteration(): Promise<SlIterationResult> {
  const { runId } = await triggerBenchRun('benchSlWorkflow');
  try {
    const returnValue = await withTimeout(
      getReturnValue(runId),
      RUN_TIMEOUT_MS,
      `benchSlWorkflow returnValue (run ${runId})`
    );
    const sl = (returnValue as { sl?: BenchStreamLatency } | undefined)?.sl;
    if (
      !sl ||
      typeof sl.writtenAt !== 'number' ||
      typeof sl.readAt !== 'number'
    ) {
      throw new Error(
        `Run ${runId} returned no stream-latency sample: ${JSON.stringify(returnValue)?.slice(0, 200)}`
      );
    }
    return { runId, slMs: Math.max(0, sl.readAt - sl.writtenAt) };
  } catch (error) {
    (error as Error).message += ` (run ${runId})`;
    throw error;
  }
}

async function runSoIteration(
  mode: 'text' | 'structured'
): Promise<SoIterationResult> {
  const { runId } = await triggerBenchRun('benchSoWorkflow', [
    SO_CHUNK_COUNT,
    SO_INTERVAL_MS,
    mode,
  ]);
  try {
    const returnValue = await withTimeout(
      getReturnValue(runId),
      // The writer streams for the whole generation window before the run can
      // complete, so extend the guard past the base run timeout by that window.
      RUN_TIMEOUT_MS + SO_NOMINAL_DURATION_MS,
      `benchSoWorkflow (${mode}) returnValue (run ${runId})`
    );
    const so = (returnValue as { so?: BenchStreamOverhead } | undefined)?.so;
    if (
      !so ||
      typeof so.writtenAt !== 'number' ||
      typeof so.doneAt !== 'number'
    ) {
      throw new Error(
        `Run ${runId} returned no stream-overhead sample: ${JSON.stringify(returnValue)?.slice(0, 200)}`
      );
    }
    if (so.received !== SO_CHUNK_COUNT) {
      throw new Error(
        `Run ${runId} consumed ${so.received} chunks, expected ${SO_CHUNK_COUNT}`
      );
    }
    // Both timestamps are deployment-side; subtract the modelled generation
    // window and clamp to absorb tiny intra-Vercel skew.
    return {
      runId,
      soMs: Math.max(0, so.doneAt - so.writtenAt - SO_NOMINAL_DURATION_MS),
    };
  } catch (error) {
    (error as Error).message += ` (run ${runId})`;
    throw error;
  }
}

async function runCrttIteration(
  variant: 'llm' | 'sweep'
): Promise<CrttIterationResult> {
  // Same chunk count and pacing as the SO scenarios, so the llm-shaped CRTT
  // numbers describe the exact same workload SO measures in aggregate.
  const { runId } = await triggerBenchRun('benchCrttWorkflow', [
    SO_CHUNK_COUNT,
    SO_INTERVAL_MS,
    variant,
  ]);
  try {
    const returnValue = await withTimeout(
      getReturnValue(runId),
      // The writer streams for the whole generation window before the run can
      // complete, so extend the guard past the base run timeout by that window.
      RUN_TIMEOUT_MS + SO_NOMINAL_DURATION_MS,
      `benchCrttWorkflow (${variant}) returnValue (run ${runId})`
    );
    const crtt = (returnValue as { crtt?: BenchChunkRttResult } | undefined)
      ?.crtt;
    if (!crtt || !crtt.all || typeof crtt.all.avg !== 'number') {
      throw new Error(
        `Run ${runId} returned no chunk-RTT summaries: ${JSON.stringify(returnValue)?.slice(0, 200)}`
      );
    }
    if (crtt.received !== SO_CHUNK_COUNT) {
      throw new Error(
        `Run ${runId} consumed ${crtt.received} chunks, expected ${SO_CHUNK_COUNT}`
      );
    }
    return { runId, crtt };
  } catch (error) {
    (error as Error).message += ` (run ${runId})`;
    throw error;
  }
}

/**
 * Runs recorded iterations (plus warmups) sequentially — concurrency would
 * contend on the same deployment and skew latencies. Failed iterations are
 * retried (each scenario gets `extraAttempts` spare attempts on top of the
 * requested iteration count), so a transient failure doesn't zero out or
 * shrink the sample set; the scenario only fails when the attempt budget
 * can't produce the full number of iterations.
 */
async function runScenario<T>(
  name: string,
  iterations: number,
  iteration: () => Promise<T>,
  {
    warmupIterations = WARMUP_ITERATIONS,
    extraAttempts = Math.ceil(iterations * MAX_FAILURE_RATIO),
  }: { warmupIterations?: number; extraAttempts?: number } = {}
): Promise<T[]> {
  for (let i = 0; i < warmupIterations; i++) {
    try {
      await iteration();
    } catch (error) {
      // Warmup failures are non-fatal but worth surfacing.
      console.warn(`[bench] ${name} warmup ${i + 1} failed:`, error);
    }
  }

  const results: T[] = [];
  const failures: Error[] = [];
  const maxAttempts = iterations + extraAttempts;
  let attempts = 0;
  while (results.length < iterations && attempts < maxAttempts) {
    attempts++;
    try {
      results.push(await iteration());
    } catch (error) {
      failures.push(error as Error);
      console.warn(
        `[bench] ${name} attempt ${attempts}/${maxAttempts} failed:`,
        error
      );
      if (results.length === 0 && attempts >= ZERO_SUCCESS_ABORT_ATTEMPTS) {
        throw new Error(
          `${name}: no successful iterations after ${attempts} attempts — target looks systematically broken, aborting scenario; last error: ${(error as Error).message}`
        );
      }
    }
  }

  console.log(
    `[bench] ${name}: ${results.length}/${iterations} iterations succeeded (${attempts} attempts)`
  );
  if (results.length < iterations) {
    throw new Error(
      `${name}: only ${results.length}/${iterations} iterations succeeded after ${attempts} attempts; last error: ${failures[failures.length - 1]?.message}`
    );
  }
  return results;
}

// ============================================================================
// Stats & output
// ============================================================================

interface MetricStats {
  /** Fastest (best) sample — the warm-start floor vs the cold-start tail. */
  best: number;
  /** Mean; kept in the JSON for reference but not shown in the PR comment. */
  avg: number;
  /** Median; only recorded for CRTT rows (the exit criteria track median and
   * average per-chunk RTT). Kept in the JSON, not shown in the PR comment. */
  p50?: number;
  p75: number;
  p90: number;
  p99: number;
  samples: number;
  /** Every sample (ms, ascending), not just the percentiles above: the PR
   * comment diffs the whole STSO distribution against `main`, and
   * percentiles alone hide *how many* samples moved and by how much. */
  raw: number[];
  /** Fixed-bin histogram of the samples, for rows whose raw samples never
   * reach this process (CRTT: aggregation happens in the reader step on the
   * deployment). Fixed shared edges make the PR comment's distribution diff
   * against `main` exact — the renderer only diffs matching-edge rows. */
  hist?: { edgesMs: number[]; counts: number[] };
  /** Drill-down rows (e.g. CRTT per-bucket splits): kept out of the PR
   * comment's main results table and rendered in a collapsed section. */
  detail?: boolean;
  /** Mean RTT per tenth of the stream (CRTT headline rows): the drift/trend
   * readout, rendered as a progress sparkline in the drill-down. */
  progressAvgMs?: number[];
  /** Mean RTT per log size bin (CRTT sweep headline row): the size→latency
   * curve, rendered as a size sparkline in the drill-down. Null entries are
   * bins the sweep left empty. */
  sizeAvgMs?: (number | null)[];
  /** Short group/bucket labels for drill-down rendering (CRTT: variant and
   * index/size bucket). */
  group?: string;
  bucket?: string;
}

interface MetricTargets {
  p75?: number;
  p90?: number;
  p99?: number;
}

function computeStats(samples: number[]): MetricStats {
  if (samples.length === 0) {
    throw new Error('Cannot compute stats over zero samples');
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const percentile = (q: number) =>
    sorted[
      Math.min(sorted.length - 1, Math.ceil((q / 100) * sorted.length) - 1)
    ];
  const round = (v: number) => Math.round(v * 10) / 10;
  return {
    best: round(sorted[0]),
    avg: round(sorted.reduce((sum, v) => sum + v, 0) / sorted.length),
    p75: round(percentile(75)),
    p90: round(percentile(90)),
    p99: round(percentile(99)),
    samples: sorted.length,
    raw: sorted,
  };
}

interface MetricRow extends MetricStats {
  /** Short metric id: ttfs | stso | wo | sl */
  metric: string;
  /** Short scenario label; explained via scenario descriptions in the output */
  scenario: string;
  unit: 'ms';
  /** Latency targets rendered as pass/fail marks in the PR comment */
  targets?: MetricTargets;
}

const metricRows: MetricRow[] = [];

function recordMetric(
  metric: string,
  scenario: string,
  samples: number[],
  targets?: MetricTargets
) {
  if (samples.length === 0) return;
  metricRows.push({
    metric,
    scenario,
    unit: 'ms',
    targets,
    ...computeStats(samples),
  });
}

/**
 * Records one CRTT row from per-iteration summaries. Unlike recordMetric
 * there are no raw samples in this process — the reader step aggregated them
 * on the deployment — so the row is the mergeRttSummaries merge: exact
 * count/best/avg/histogram, percentile-of-percentiles for p50-p99. `samples`
 * is the total chunk count across iterations. The merged fixed-bin histogram
 * rides along for the PR comment's sparkline drill-down (exact vs `main`,
 * where the percentiles are approximations). Rows with `detail` are not
 * rendered at all — they carry the per-index-bucket splits in the results
 * JSON (with baseline annotations) so a headline regression can be localized
 * from the artifacts. No targets yet (see the CRTT header note), so no 🔴
 * marks render.
 */
function recordCrttMetric(
  scenario: string,
  summaries: readonly (BenchRttSummary | undefined)[],
  {
    group,
    bucket,
    detail = false,
    progress,
    size,
  }: {
    group: string;
    bucket: string;
    detail?: boolean;
    progress?: BenchRttMeanProfile;
    size?: BenchRttMeanProfile;
  }
) {
  const merged = mergeRttSummaries(summaries);
  if (!merged) return;
  // Mean RTT per profile bin; sums/counts merge exactly across iterations,
  // so these avgs are exact like the histogram. Empty bins become null so
  // the renderer can show them as gaps rather than zeros.
  const profileAvgs = (profile?: BenchRttMeanProfile) =>
    profile?.totalMs.map((total, i) =>
      profile.counts[i] > 0
        ? Math.round((total / profile.counts[i]) * 10) / 10
        : null
    );
  const progressAvgMs = profileAvgs(progress)?.map((v) => v ?? 0);
  const sizeAvgMs = profileAvgs(size);
  metricRows.push({
    metric: 'crtt',
    scenario,
    unit: 'ms',
    best: merged.best,
    avg: merged.avg,
    p50: merged.p50,
    p75: merged.p75,
    p90: merged.p90,
    p99: merged.p99,
    samples: merged.count,
    raw: [],
    hist: { edgesMs: RTT_HIST_EDGES_MS, counts: merged.hist },
    detail: detail || undefined,
    group,
    bucket,
    progressAvgMs,
    sizeAvgMs,
  });
}

function getBackend(): string {
  if (process.env.WORKFLOW_BENCH_BACKEND) {
    return process.env.WORKFLOW_BENCH_BACKEND;
  }
  if (process.env.WORKFLOW_VERCEL_ENV) return 'vercel';
  if (process.env.WORKFLOW_TARGET_WORLD?.includes('postgres')) {
    return 'postgres';
  }
  return 'local';
}

// Short scenario labels for the results table; the descriptions are rendered
// as a legend at the bottom of the PR comment.
const SCENARIO_STEP = 'step';
const SCENARIO_TURBO_STREAM = 'stream';
const SCENARIO_HOOK_STREAM = 'hook + stream';
const SCENARIO_SEQUENTIAL = `${SEQUENTIAL_STEP_COUNT} steps`;
const SCENARIO_STREAM_LATENCY = 'stream latency';
// Two SO scenarios differing only in payload shape. The labels are distinct
// from the pre-existing 'stream overhead' baseline key, so the payload change
// doesn't diff against the old fixed-'aaaa' numbers — the SO deltas start blank
// and re-baseline on the next `main` run.
const SCENARIO_STREAM_OVERHEAD_TEXT = 'stream overhead (text)';
const SCENARIO_STREAM_OVERHEAD_STRUCTURED = 'stream overhead (structured)';
// CRTT scenario labels, doubling as the headline rows' scenario keys; the
// per-bucket detail rows are keyed `chunk RTT llm (<bucket>)` /
// `chunk RTT sweep (<bucket>)`. All new baseline keys, so nothing diffs
// against pre-existing SL/SO baselines (their scenarios and payloads are
// untouched) and the CRTT deltas stay blank until `main` produces them.
const SCENARIO_CHUNK_RTT_LLM = 'chunk RTT (llm)';
const SCENARIO_CHUNK_RTT_SWEEP = 'chunk RTT (size sweep)';
const SCENARIO_DESCRIPTIONS = [
  {
    name: SCENARIO_STEP,
    description:
      'one trivial no-op step, no stream; no hooks, so the run stays in turbo mode (in-process fast path)',
  },
  {
    name: SCENARIO_TURBO_STREAM,
    description:
      'one streaming step; no hooks, so the run stays in turbo mode (in-process fast path)',
  },
  {
    name: SCENARIO_HOOK_STREAM,
    description:
      'registers a hook before one step, which exits turbo mode (dispatch path)',
  },
  {
    name: SCENARIO_SEQUENTIAL,
    description: `${SEQUENTIAL_STEP_COUNT} trivial sequential steps; STSO is measured between consecutive steps in the given step ranges, and WO is the whole-run overhead outside step bodies`,
  },
  {
    name: SCENARIO_STREAM_LATENCY,
    description:
      'parallel reader/writer steps on a dedicated stream; SL is the in-deployment write->read propagation (readAt - writtenAt)',
  },
  {
    name: SCENARIO_STREAM_OVERHEAD_TEXT,
    description: `writer streams ${SO_CHUNK_COUNT} variable-length text token deltas paced at ${SO_CHUNK_RATE_PER_SEC}/s for ${SO_DURATION_SECONDS}s (a haiku-size LLM's token throughput) while a parallel reader drains the whole stream; SO is the end-to-end write+consume time beyond the ${SO_DURATION_SECONDS}s generation window (overhead/backpressure)`,
  },
  {
    name: SCENARIO_STREAM_OVERHEAD_STRUCTURED,
    description: `same workload as ${SCENARIO_STREAM_OVERHEAD_TEXT}, but each delta is an AI-SDK-style structured object ({ type: 'text-delta', id, text }) instead of a raw string, so the SO gap vs the text scenario is the added serialization cost`,
  },
  {
    name: SCENARIO_CHUNK_RTT_LLM,
    description: `writer streams the same paced ${SO_CHUNK_COUNT}-chunk LLM-shaped workload as the SO scenarios, but every delta embeds { seq, writtenAt }; the reader stamps each chunk's arrival and aggregates per-chunk write->read RTT on the deployment, split by chunk index (seq 0 = stream-open write, seq 1-20 = warmup, seq 21+ = steady state) plus a mean-RTT-per-tenth-of-stream progress profile that surfaces drift`,
  },
  {
    name: SCENARIO_CHUNK_RTT_SWEEP,
    description: `same pacing as ${SCENARIO_CHUNK_RTT_LLM}, but deltas are padded in rotation across seven log-spaced sizes (~160B to ~12KB serialized) so mean per-chunk RTT is profiled as a function of chunk size (the llm-shaped numbers stay pure of padding)`,
  },
];

// Datadog APM permalink for a trace id. The benchmark deployment exports its
// OTel spans to Datadog, and `/api/bench` returns the trace id of the request
// that started each run.
const DATADOG_TRACE_URL = 'https://app.datadoghq.com/apm/trace/';

/**
 * Datadog APM search for the spans tagged with a given `workflow.run.id`.
 *
 * The permalink above opens the *trigger's* trace, which under the default
 * `WORKFLOW_TRACE_MODE=linked` holds only `workflow.start` plus span links out
 * to the per-invocation trace roots — an entry point to the run rather than the
 * run itself. This search lands straight on the execution spans, which is where
 * an STSO investigation actually goes, so both are logged.
 *
 * Depends on `workflow.run.id` being an indexed span tag in the Datadog org. If
 * it isn't, this returns an empty search and the trace permalink stays the way
 * in; neither link is load-bearing for the benchmark itself.
 */
function datadogRunSearchUrl(runId: string): string {
  const query = encodeURIComponent(`@workflow.run.id:${runId}`);
  return `https://app.datadoghq.com/apm/traces?query=${query}`;
}

describe('workflow benchmarks', () => {
  // Preflight: prove the deployment executes workflows (and the trigger route
  // works) before any scenario spends its attempt budget. Without this, a
  // target that accepts run creation but never executes runs (e.g. queue not
  // delivering to the deployment) makes every iteration of every scenario wait
  // out RUN_TIMEOUT_MS, and the job dies at its time limit without a useful
  // error.
  beforeAll(async () => {
    const { runId } = await triggerBenchRun(
      'benchSequentialStepsWorkflow',
      [1]
    );
    try {
      const returnValue = await withTimeout(
        getReturnValue(runId),
        PREFLIGHT_TIMEOUT_MS,
        `preflight run (run ${runId})`
      );
      timingsFromReturnValue(returnValue, runId);
      console.log(`[bench] preflight ok (run ${runId})`);
    } catch (error) {
      throw new Error(
        `Benchmark preflight failed — the deployment accepted the run but did not execute it to completion; aborting all scenarios. ${(error as Error).message}`
      );
    }
  }, PREFLIGHT_TIMEOUT_MS + 60_000);

  test('scenario: 1 no-op step (turbo)', { timeout: 30 * 60_000 }, async () => {
    const results = await runScenario(SCENARIO_STEP, STREAM_ITERATIONS, () =>
      runStreamIteration('benchStepWorkflow')
    );
    recordMetric(
      'ttfs',
      SCENARIO_STEP,
      results.map((r) => r.ttfsMs),
      TTFS_TARGETS
    );
  });

  test(
    'scenario: 1 streaming step (turbo)',
    { timeout: 30 * 60_000 },
    async () => {
      const results = await runScenario(
        SCENARIO_TURBO_STREAM,
        STREAM_ITERATIONS,
        () => runStreamIteration('benchStreamWorkflow')
      );
      recordMetric(
        'ttfs',
        SCENARIO_TURBO_STREAM,
        results.map((r) => r.ttfsMs),
        TTFS_TARGETS
      );
    }
  );

  test(
    'scenario: hook + 1 step (non-turbo)',
    { timeout: 30 * 60_000 },
    async () => {
      const results = await runScenario(
        SCENARIO_HOOK_STREAM,
        STREAM_ITERATIONS,
        () => runStreamIteration('benchHookStreamWorkflow')
      );
      recordMetric(
        'ttfs',
        SCENARIO_HOOK_STREAM,
        results.map((r) => r.ttfsMs),
        TTFS_TARGETS
      );
    }
  );

  test('scenario: stream latency', { timeout: 30 * 60_000 }, async () => {
    const results = await runScenario(
      SCENARIO_STREAM_LATENCY,
      SL_ITERATIONS,
      () => runSlIteration()
    );
    recordMetric(
      'sl',
      SCENARIO_STREAM_LATENCY,
      results.map((r) => r.slMs),
      SL_TARGETS
    );
  });

  test(
    'scenario: stream overhead (text)',
    { timeout: 30 * 60_000 },
    async () => {
      const results = await runScenario(
        SCENARIO_STREAM_OVERHEAD_TEXT,
        SO_ITERATIONS,
        () => runSoIteration('text')
      );
      recordMetric(
        'so',
        SCENARIO_STREAM_OVERHEAD_TEXT,
        results.map((r) => r.soMs),
        SO_TARGETS
      );
    }
  );

  test(
    'scenario: stream overhead (structured)',
    { timeout: 30 * 60_000 },
    async () => {
      const results = await runScenario(
        SCENARIO_STREAM_OVERHEAD_STRUCTURED,
        SO_ITERATIONS,
        () => runSoIteration('structured')
      );
      recordMetric(
        'so',
        SCENARIO_STREAM_OVERHEAD_STRUCTURED,
        results.map((r) => r.soMs),
        SO_TARGETS
      );
    }
  );

  test('scenario: chunk RTT (llm)', { timeout: 30 * 60_000 }, async () => {
    const results = await runScenario(
      SCENARIO_CHUNK_RTT_LLM,
      CRTT_ITERATIONS,
      () => runCrttIteration('llm')
    );
    // The pooled row is the headline (per-chunk RTT averaged independent of
    // chunk size) and the only rendered llm row; the index-bucket rows split
    // it by position in the stream for the results artifacts (flat across
    // runs so far, so they are data for investigations, not rendered rows).
    recordCrttMetric(
      SCENARIO_CHUNK_RTT_LLM,
      results.map((r) => r.crtt.all),
      {
        group: 'llm',
        bucket: 'all',
        progress: mergeMeanProfiles(results.map((r) => r.crtt.progress)),
      }
    );
    for (const bucket of RTT_INDEX_BUCKETS) {
      recordCrttMetric(
        `chunk RTT llm (${bucket})`,
        results.map((r) => r.crtt.byIndex[bucket]),
        { group: 'llm', bucket, detail: true }
      );
    }
  });

  test(
    'scenario: chunk RTT (size sweep)',
    { timeout: 30 * 60_000 },
    async () => {
      const results = await runScenario(
        SCENARIO_CHUNK_RTT_SWEEP,
        CRTT_ITERATIONS,
        () => runCrttIteration('sweep')
      );
      recordCrttMetric(
        SCENARIO_CHUNK_RTT_SWEEP,
        results.map((r) => r.crtt.all),
        {
          group: 'sweep',
          bucket: 'all',
          progress: mergeMeanProfiles(results.map((r) => r.crtt.progress)),
          // The size→latency curve, only from the sweep variant: the
          // llm-shaped deltas all land in the smallest size bin, so a size
          // profile of them says nothing.
          size: mergeMeanProfiles(results.map((r) => r.crtt.size)),
        }
      );
    }
  );

  test('scenario: sequential steps', { timeout: 60 * 60_000 }, async () => {
    const results = await runScenario(
      SCENARIO_SEQUENTIAL,
      SEQUENTIAL_ITERATIONS,
      () => runSequentialIteration(SEQUENTIAL_STEP_COUNT),
      {
        // No warmup: STSO gaps are measured entirely on the deployment (the
        // other scenarios already warmed the client + world), and a warmup
        // run of this scenario would cost as much as a recorded one.
        warmupIterations: 0,
        // A long run occasionally fails outright (e.g. replay divergence
        // under a large event log); give the default single iteration two
        // spare attempts instead of failing the whole scenario.
        extraAttempts: Math.max(2, Math.ceil(SEQUENTIAL_ITERATIONS * 0.5)),
      }
    );
    // Name the runs behind the STSO histograms in this job's own log, right
    // where they were produced. When a bucket looks wrong the investigation
    // starts in APM, and this saves the usual hunt by deployment id + time
    // window. Logged rather than rendered into the PR comment so it is also
    // there for a local `pnpm bench` and for a run whose comment step never
    // gets to execute.
    for (const { runId, traceId } of results) {
      console.log(
        `[bench] ${SCENARIO_SEQUENTIAL} run ${runId}` +
          (traceId ? ` — trace ${DATADOG_TRACE_URL}${traceId}` : '') +
          ` — spans ${datadogRunSearchUrl(runId)}`
      );
    }
    // Report STSO split by whether the step that ends the gap was 'inline'
    // (same warm process as the step before it — pure framework overhead) or
    // a 'queue-hop' (first step of a fresh process — dispatch + reinit cost).
    // Ground truth from the workflow itself (see workflows/97_bench.ts), not
    // inferred from step index or trace timestamps. No targets yet: the two
    // populations are new, and the old per-index-window targets described a
    // different (index-bucketed) grouping.
    recordMetric(
      'stso',
      `${SCENARIO_SEQUENTIAL} (inline)`,
      results.flatMap((r) => r.stsoInlineMs)
    );
    recordMetric(
      'stso',
      `${SCENARIO_SEQUENTIAL} (queue-hop)`,
      results.flatMap((r) => r.stsoQueueHopMs)
    );
    // WO: whole-run overhead outside step bodies, anchored on the in-deployment
    // clientStart. Measured here rather than on the stream scenarios, where a
    // single step makes WO algebraically identical to TTFS.
    recordMetric(
      'wo',
      SCENARIO_SEQUENTIAL,
      results.map((r) => r.woMs)
    );
  });

  afterAll(() => {
    if (metricRows.length === 0) {
      console.warn('[bench] No metrics collected; skipping results file');
      return;
    }
    const appName = process.env.APP_NAME || 'unknown';
    const backend = getBackend();
    const outputPath = path.resolve(
      process.cwd(),
      process.env.BENCH_OUTPUT_FILE ??
        `bench-results-${appName}-${backend}.json`
    );
    const results = {
      version: 1,
      // Measurement-methodology version; baseline deltas only compare runs
      // with the same value (see annotateWithBaseline in the renderer).
      methodologyVersion: BENCH_METHODOLOGY_VERSION,
      app: appName,
      backend,
      generatedAt: new Date().toISOString(),
      commit: process.env.GITHUB_SHA || undefined,
      config: {
        streamIterations: STREAM_ITERATIONS,
        slIterations: SL_ITERATIONS,
        soIterations: SO_ITERATIONS,
        soChunkCount: SO_CHUNK_COUNT,
        soChunkRatePerSec: SO_CHUNK_RATE_PER_SEC,
        soDurationSeconds: SO_DURATION_SECONDS,
        crttIterations: CRTT_ITERATIONS,
        sequentialIterations: SEQUENTIAL_ITERATIONS,
        sequentialStepCount: SEQUENTIAL_STEP_COUNT,
        warmupIterations: WARMUP_ITERATIONS,
      },
      scenarios: SCENARIO_DESCRIPTIONS,
      metrics: metricRows,
    };
    fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
    console.log(`[bench] Results written to ${outputPath}`);
    console.table(
      metricRows.map(
        ({ metric, scenario, best, avg, p75, p90, p99, samples }) => ({
          metric,
          scenario,
          best,
          avg,
          p75,
          p90,
          p99,
          samples,
        })
      )
    );
  });
});
