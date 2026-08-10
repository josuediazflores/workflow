/**
 * Unit tests for the chunk-RTT (CRTT) benchmark's pure bucketing/aggregation
 * helpers (workbench/example/workflows/97_bench_rtt.ts). The module is
 * dependency-free on purpose: the same code runs inside the benchmark's
 * reader step on the deployment (per-iteration aggregation) and in the
 * benchmark runner (cross-iteration merging), and this suite is the fast
 * check on both — the bench itself only runs against a deployment.
 */

import { describe, expect, test } from 'vitest';
import {
  type BenchRttSummary,
  histogramRttSamples,
  mergeProgressProfiles,
  mergeRttSummaries,
  progressProfile,
  RTT_HIST_EDGES_MS,
  RTT_INDEX_BUCKETS,
  RTT_PROGRESS_BINS,
  RTT_SIZE_BUCKETS,
  rttIndexBucket,
  rttSizeBucket,
  summarizeRttSamples,
} from '../../../workbench/example/workflows/97_bench_rtt';

/** Histogram with `count` in the bin holding `value` and zeros elsewhere. */
function histWith(value: number, count = 1): number[] {
  const hist = new Array(RTT_HIST_EDGES_MS.length + 1).fill(0);
  let bin = 0;
  while (bin < RTT_HIST_EDGES_MS.length && value >= RTT_HIST_EDGES_MS[bin]) {
    bin++;
  }
  hist[bin] = count;
  return hist;
}

describe('rttIndexBucket', () => {
  test('boundaries: stream-open write / warmup / steady state', () => {
    expect(rttIndexBucket(0)).toBe('seq 0');
    expect(rttIndexBucket(1)).toBe('seq 1-20');
    expect(rttIndexBucket(20)).toBe('seq 1-20');
    expect(rttIndexBucket(21)).toBe('seq 21+');
    expect(rttIndexBucket(299)).toBe('seq 21+');
  });

  test('every bucket is a declared bucket key', () => {
    for (let seq = 0; seq < 300; seq++) {
      expect(RTT_INDEX_BUCKETS).toContain(rttIndexBucket(seq));
    }
  });
});

describe('progressProfile', () => {
  test('bins by fraction of the stream, so profiles are chunk-count independent', () => {
    // 300 chunks: each tenth holds exactly 30.
    const rtts = Array.from({ length: 300 }, (_, seq) => seq);
    const profile = progressProfile(rtts);
    expect(profile.counts).toEqual(new Array(RTT_PROGRESS_BINS).fill(30));
    // First tenth: seq 0..29 (sum 435); last tenth: seq 270..299 (sum 8535).
    expect(profile.totalMs[0]).toBe(435);
    expect(profile.totalMs[RTT_PROGRESS_BINS - 1]).toBe(8535);

    // 20 chunks (fewer than would fill 10 bins evenly at other counts): still
    // 2 per tenth.
    const small = progressProfile(Array.from({ length: 20 }, () => 5));
    expect(small.counts).toEqual(new Array(RTT_PROGRESS_BINS).fill(2));
  });

  test('skips sparse entries defensively', () => {
    const rtts: (number | undefined)[] = new Array(100);
    rtts[0] = 7;
    rtts[99] = 9;
    const profile = progressProfile(rtts);
    expect(profile.counts.reduce((a, b) => a + b, 0)).toBe(2);
    expect(profile.totalMs[0]).toBe(7);
    expect(profile.totalMs[RTT_PROGRESS_BINS - 1]).toBe(9);
  });
});

describe('mergeProgressProfiles', () => {
  test('returns undefined with no profiles and sums exactly otherwise', () => {
    expect(mergeProgressProfiles([])).toBeUndefined();
    expect(mergeProgressProfiles([undefined])).toBeUndefined();
    const a = progressProfile(Array.from({ length: 10 }, () => 10));
    const b = progressProfile(Array.from({ length: 10 }, () => 30));
    const merged = mergeProgressProfiles([a, undefined, b]);
    expect(merged?.counts).toEqual(new Array(RTT_PROGRESS_BINS).fill(2));
    expect(merged?.totalMs).toEqual(new Array(RTT_PROGRESS_BINS).fill(40));
  });
});

describe('rttSizeBucket', () => {
  test('boundaries', () => {
    expect(rttSizeBucket(1)).toBe('<=256B');
    expect(rttSizeBucket(256)).toBe('<=256B');
    expect(rttSizeBucket(257)).toBe('256B-4KB');
    expect(rttSizeBucket(4096)).toBe('256B-4KB');
    expect(rttSizeBucket(4097)).toBe('>4KB');
  });

  test('the sweep pad sizes land in three distinct buckets', () => {
    // Approximate serialized sizes of the sweep variant's rotation
    // (~50B base chunk + pads of 64 / 1024 / 10240 chars).
    expect(rttSizeBucket(120)).toBe(RTT_SIZE_BUCKETS[0]);
    expect(rttSizeBucket(1080)).toBe(RTT_SIZE_BUCKETS[1]);
    expect(rttSizeBucket(10300)).toBe(RTT_SIZE_BUCKETS[2]);
  });
});

describe('histogramRttSamples', () => {
  test('bins are [prev edge, edge), first bin is <1ms, last is 5000+', () => {
    expect(histogramRttSamples([0, 0.5])[0]).toBe(2);
    // A sample exactly on an edge lands in the bin the edge opens.
    const atEdge = histogramRttSamples([1]);
    expect(atEdge[0]).toBe(0);
    expect(atEdge[1]).toBe(1);
    const overflow = histogramRttSamples([5000, 60000]);
    expect(overflow[RTT_HIST_EDGES_MS.length]).toBe(2);
  });

  test('counts sum to the sample count', () => {
    const samples = [0, 1, 3, 7, 59, 128, 438, 1229, 9999];
    const hist = histogramRttSamples(samples);
    expect(hist).toHaveLength(RTT_HIST_EDGES_MS.length + 1);
    expect(hist.reduce((a, b) => a + b, 0)).toBe(samples.length);
  });
});

describe('summarizeRttSamples', () => {
  test('returns undefined for an empty bucket', () => {
    expect(summarizeRttSamples([])).toBeUndefined();
  });

  test('single sample collapses every stat to that value', () => {
    expect(summarizeRttSamples([7])).toEqual({
      count: 1,
      best: 7,
      avg: 7,
      hist: histWith(7),
      p50: 7,
      p75: 7,
      p90: 7,
      p99: 7,
    });
  });

  test('percentiles use the runner convention (nearest-rank via ceil)', () => {
    // 1..100 shuffled: pQ must be exactly Q under nearest-rank.
    const samples = Array.from({ length: 100 }, (_, i) => i + 1).sort(
      () => 0.5 - Math.random()
    );
    expect(summarizeRttSamples(samples)).toEqual({
      count: 100,
      best: 1,
      avg: 50.5,
      hist: histogramRttSamples(samples),
      p50: 50,
      p75: 75,
      p90: 90,
      p99: 99,
    });
  });

  test('rounds to 0.1ms', () => {
    const summary = summarizeRttSamples([1, 2, 2.44]);
    expect(summary?.avg).toBe(1.8);
    expect(summary?.p99).toBe(2.4);
  });
});

describe('mergeRttSummaries', () => {
  const summary = (overrides: Partial<BenchRttSummary>): BenchRttSummary => ({
    count: 10,
    best: 1,
    avg: 5,
    hist: histWith(5, 10),
    p50: 5,
    p75: 6,
    p90: 8,
    p99: 9,
    ...overrides,
  });

  test('returns undefined when no iteration produced the bucket', () => {
    expect(mergeRttSummaries([])).toBeUndefined();
    expect(mergeRttSummaries([undefined, undefined])).toBeUndefined();
  });

  test('single summary passes through unchanged', () => {
    const s = summary({});
    expect(mergeRttSummaries([undefined, s])).toEqual(s);
  });

  test('count sums, best is the min, avg is count-weighted', () => {
    const merged = mergeRttSummaries([
      summary({ count: 10, best: 2, avg: 10 }),
      summary({ count: 30, best: 1, avg: 2 }),
    ]);
    expect(merged?.count).toBe(40);
    expect(merged?.best).toBe(1);
    expect(merged?.avg).toBe(4); // (10*10 + 2*30) / 40
  });

  test('histograms merge by elementwise summation (exact)', () => {
    const merged = mergeRttSummaries([
      summary({ count: 10, hist: histWith(5, 10) }),
      summary({ count: 30, hist: histWith(128, 30) }),
    ]);
    const expected = histWith(5, 10);
    const bin128 = histWith(128, 30);
    for (let i = 0; i < expected.length; i++) expected[i] += bin128[i];
    expect(merged?.hist).toEqual(expected);
    expect(merged?.hist.reduce((a, b) => a + b, 0)).toBe(40);
  });

  test('percentiles merge as percentile-of-percentiles', () => {
    const summaries = Array.from({ length: 10 }, (_, i) =>
      summary({ p50: i + 1, p90: (i + 1) * 10, p99: (i + 1) * 100 })
    );
    const merged = mergeRttSummaries(summaries);
    // p50 over the ten per-iteration p50s (1..10) = 5.
    expect(merged?.p50).toBe(5);
    // p90 over 10..100 = 90.
    expect(merged?.p90).toBe(90);
    // p99 over 100..1000 = max of maxes (exact at the tail).
    expect(merged?.p99).toBe(1000);
  });
});
