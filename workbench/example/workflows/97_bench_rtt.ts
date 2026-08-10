// Pure bucketing + aggregation helpers for the chunk round-trip-time (CRTT)
// benchmark scenario. The workflow half lives in 97_bench.ts
// (benchCrttWorkflow) and the runner half in
// packages/core/e2e/benchmark.test.ts.
//
// This module is deliberately dependency-free so the same code runs in three
// places: the reader step aggregates per-chunk RTT samples on the deployment
// (keeping the workflow return value small — bucketed summaries, not hundreds
// of raw samples), the benchmark runner merges the per-iteration summaries
// into one row per bucket, and the unit tests
// (packages/core/src/bench-chunk-rtt-stats.test.ts) exercise both directly.

/**
 * Summary of one bucket's RTT samples (all values in ms, rounded to 0.1ms).
 * Computed inside the reader step per iteration (exact percentiles over that
 * iteration's samples), then merged across iterations by
 * {@link mergeRttSummaries}.
 */
export interface BenchRttSummary {
  /** Number of samples aggregated into this summary. */
  count: number;
  /** Fastest sample (min). */
  best: number;
  /** Mean — the exit criteria's headline "average per-chunk RTT". */
  avg: number;
  p50: number;
  p75: number;
  p90: number;
  p99: number;
  /** Fixed-bin histogram of the samples (see {@link RTT_HIST_EDGES_MS}):
   * `hist[i]` counts samples in `[edges[i-1], edges[i])`, with `hist[0]`
   * below the first edge and the last entry at/above the last edge. Because
   * the edges are a shared constant, histograms merge exactly — across
   * iterations and across benchmark runs — unlike the percentile fields. */
  hist: number[];
}

// Histogram bin edges (ms), a 1-2-5 log series. Log-scale bins keep
// resolution at both ends of the plausible range — a warm in-region
// write->read can be single-digit ms while a stalled delivery is over a
// second — and fixed shared edges are what make cross-run histogram diffs
// exact (adaptive widths, like the STSO section's, cannot be re-binned once
// the raw samples have been left behind on the deployment).
export const RTT_HIST_EDGES_MS = [
  1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000,
];

/** Buckets samples into the fixed {@link RTT_HIST_EDGES_MS} bins. Returns
 * `edges.length + 1` counts (last = at/above the final edge). */
export function histogramRttSamples(samples: number[]): number[] {
  const counts = new Array(RTT_HIST_EDGES_MS.length + 1).fill(0);
  for (const v of samples) {
    let bin = 0;
    while (bin < RTT_HIST_EDGES_MS.length && v >= RTT_HIST_EDGES_MS[bin]) {
      bin++;
    }
    counts[bin]++;
  }
  return counts;
}

// Chunk-index buckets. Each boundary is tied to a mechanism, not a progress
// range:
// - 'seq 0': the stream-open write (stream creation / cold write path). Also
//   a cross-check against the SL scenario, which times the same first-chunk
//   propagation.
// - 'seq 1-20': warmup — the first ~200ms at the modeled 100 chunks/s, where
//   connections, buffers, and flush cycles are still settling.
// - 'seq 21+': steady state, kept as ONE bucket so its large n gives stable
//   tail percentiles (splitting it further just compares noise floors of
//   unequal sample sizes — iteration-level stalls land in whichever range
//   they land in).
// Latency *drift* across the stream (cumulative log/buffer growth) is a
// trend, which fixed buckets detect badly; that is the progress profile's
// job (see {@link progressProfile}).
export const RTT_INDEX_BUCKETS = ['seq 0', 'seq 1-20', 'seq 21+'] as const;
export type RttIndexBucket = (typeof RTT_INDEX_BUCKETS)[number];

export function rttIndexBucket(seq: number): RttIndexBucket {
  if (seq <= 0) return 'seq 0';
  if (seq <= 20) return 'seq 1-20';
  return 'seq 21+';
}

// Number of equal fractions of the stream in the progress profile. Ten keeps
// the profile line compact while still localizing a drift or a slow phase.
export const RTT_PROGRESS_BINS = 10;

/** A binned mean-RTT profile: `totalMs[i]`/`counts[i]` is the mean RTT of
 * bin i. Used for both the stream-progress profile (bin = tenth of the
 * stream) and the chunk-size profile (bin = log size range). Sums and counts
 * merge exactly across iterations and runs. */
export interface BenchRttMeanProfile {
  counts: number[];
  totalMs: number[];
}

/** Builds the progress profile from per-seq RTT samples (`rttBySeq[seq]` =
 * that chunk's RTT; sparse entries are skipped defensively). Fraction-based
 * (not absolute seq), so profiles are comparable across chunk counts. The
 * trend this surfaces — does per-chunk RTT rise as the stream grows? — is
 * what fixed index buckets cannot answer without arbitrary boundaries. */
export function progressProfile(
  rttBySeq: readonly (number | undefined)[]
): BenchRttMeanProfile {
  const counts = new Array(RTT_PROGRESS_BINS).fill(0);
  const totalMs = new Array(RTT_PROGRESS_BINS).fill(0);
  const n = rttBySeq.length;
  for (let seq = 0; seq < n; seq++) {
    const rtt = rttBySeq[seq];
    if (typeof rtt !== 'number') continue;
    const bin = Math.min(
      RTT_PROGRESS_BINS - 1,
      Math.floor((seq * RTT_PROGRESS_BINS) / n)
    );
    counts[bin]++;
    totalMs[bin] += rtt;
  }
  return { counts, totalMs };
}

/** Merges mean profiles by summation — exact, like the histograms. */
export function mergeMeanProfiles(
  profiles: readonly (BenchRttMeanProfile | undefined)[]
): BenchRttMeanProfile | undefined {
  const present = profiles.filter((p): p is BenchRttMeanProfile => p != null);
  if (present.length === 0) return undefined;
  const bins = Math.max(...present.map((p) => p.counts.length));
  const counts = new Array(bins).fill(0);
  const totalMs = new Array(bins).fill(0);
  for (const p of present) {
    for (let i = 0; i < bins; i++) {
      counts[i] += p.counts[i] ?? 0;
      totalMs[i] += p.totalMs[i] ?? 0;
    }
  }
  return { counts, totalMs };
}

// Chunk-size profile bins (approximate serialized bytes, doubling edges).
// Bin i covers [edges[i-1], edges[i]), bin 0 everything below 256B, and the
// last bin everything at/above 8KB. The size-sweep scenario's pad rotation
// (see CRTT_SWEEP_PAD_LENGTHS in 97_bench.ts) puts one padded size in each
// bin, so the mean-RTT-per-bin profile is a size→latency curve: flat means
// chunk size doesn't matter, a knee localizes where it starts to.
export const RTT_SIZE_BIN_EDGES_BYTES = [256, 512, 1024, 2048, 4096, 8192];

/** Bin index into {@link RTT_SIZE_BIN_EDGES_BYTES} for a serialized size. */
export function rttSizeBin(serializedBytes: number): number {
  let bin = 0;
  while (
    bin < RTT_SIZE_BIN_EDGES_BYTES.length &&
    serializedBytes >= RTT_SIZE_BIN_EDGES_BYTES[bin]
  ) {
    bin++;
  }
  return bin;
}

/** Builds the chunk-size profile from (serialized bytes, RTT) samples. */
export function sizeProfile(
  samples: readonly { bytes: number; rttMs: number }[]
): BenchRttMeanProfile {
  const bins = RTT_SIZE_BIN_EDGES_BYTES.length + 1;
  const counts = new Array(bins).fill(0);
  const totalMs = new Array(bins).fill(0);
  for (const { bytes, rttMs } of samples) {
    const bin = rttSizeBin(bytes);
    counts[bin]++;
    totalMs[bin] += rttMs;
  }
  return { counts, totalMs };
}

// Same percentile convention as the benchmark runner's computeStats
// (nearest-rank via ceil), so a CRTT p90 means the same thing as an SO p90.
function percentile(sortedAscending: number[], q: number): number {
  return sortedAscending[
    Math.min(
      sortedAscending.length - 1,
      Math.ceil((q / 100) * sortedAscending.length) - 1
    )
  ];
}

const round = (v: number) => Math.round(v * 10) / 10;

/** Exact summary of one iteration's samples for a bucket; undefined when the
 * bucket received no samples (so the caller can just skip it). */
export function summarizeRttSamples(
  samples: number[]
): BenchRttSummary | undefined {
  if (samples.length === 0) return undefined;
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    count: sorted.length,
    best: round(sorted[0]),
    avg: round(sorted.reduce((sum, v) => sum + v, 0) / sorted.length),
    hist: histogramRttSamples(sorted),
    p50: round(percentile(sorted, 50)),
    p75: round(percentile(sorted, 75)),
    p90: round(percentile(sorted, 90)),
    p99: round(percentile(sorted, 99)),
  };
}

/**
 * Merges per-iteration bucket summaries into one summary for reporting.
 *
 * `count`, `best`, `avg` (count-weighted), and `hist` (elementwise sum over
 * the shared fixed bins) are exact. The percentiles are
 * percentile-of-percentiles — pQ over the iterations' pQ values — because the
 * raw samples never leave the reader step. That is exact at the ends (best;
 * p99 degenerates to max-of-max when an iteration's p99 is its max, which it
 * is at the per-bucket sample counts this bench produces) and an approximation
 * of the pooled percentile in between; good enough for trend tracking, which
 * is what these rows are for. The histogram is the exact view of the pooled
 * distribution.
 */
export function mergeRttSummaries(
  summaries: readonly (BenchRttSummary | undefined)[]
): BenchRttSummary | undefined {
  const present = summaries.filter((s): s is BenchRttSummary => s != null);
  if (present.length === 0) return undefined;
  const count = present.reduce((sum, s) => sum + s.count, 0);
  const mergedPercentile = (q: number, values: number[]) =>
    round(
      percentile(
        [...values].sort((a, b) => a - b),
        q
      )
    );
  const histLength = Math.max(...present.map((s) => s.hist?.length ?? 0));
  const hist = new Array(histLength).fill(0);
  for (const s of present) {
    (s.hist ?? []).forEach((c, i) => {
      hist[i] += c;
    });
  }
  return {
    count,
    best: round(Math.min(...present.map((s) => s.best))),
    avg: round(present.reduce((sum, s) => sum + s.avg * s.count, 0) / count),
    hist,
    p50: mergedPercentile(
      50,
      present.map((s) => s.p50)
    ),
    p75: mergedPercentile(
      75,
      present.map((s) => s.p75)
    ),
    p90: mergedPercentile(
      90,
      present.map((s) => s.p90)
    ),
    p99: mergedPercentile(
      99,
      present.map((s) => s.p99)
    ),
  };
}
