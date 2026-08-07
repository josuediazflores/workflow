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
  mergeRttSummaries,
  RTT_INDEX_BUCKETS,
  RTT_SIZE_BUCKETS,
  rttIndexBucket,
  rttSizeBucket,
  summarizeRttSamples,
} from '../../../workbench/example/workflows/97_bench_rtt';

describe('rttIndexBucket', () => {
  test('boundaries', () => {
    expect(rttIndexBucket(0)).toBe('seq 0');
    expect(rttIndexBucket(1)).toBe('seq 1-20');
    expect(rttIndexBucket(20)).toBe('seq 1-20');
    expect(rttIndexBucket(21)).toBe('seq 21-100');
    expect(rttIndexBucket(100)).toBe('seq 21-100');
    expect(rttIndexBucket(101)).toBe('seq 101+');
    expect(rttIndexBucket(299)).toBe('seq 101+');
  });

  test('every bucket is a declared bucket key', () => {
    for (let seq = 0; seq < 300; seq++) {
      expect(RTT_INDEX_BUCKETS).toContain(rttIndexBucket(seq));
    }
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

describe('summarizeRttSamples', () => {
  test('returns undefined for an empty bucket', () => {
    expect(summarizeRttSamples([])).toBeUndefined();
  });

  test('single sample collapses every stat to that value', () => {
    expect(summarizeRttSamples([7])).toEqual({
      count: 1,
      best: 7,
      avg: 7,
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
