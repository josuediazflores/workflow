import { afterEach, describe, expect, test, vi } from 'vitest';
import {
  classifyRebuild,
  createFileChangeScheduler,
  createSourceSnapshotFromSource,
  extractImportSignature,
  type SourceSnapshot,
  stripCommentsFromSource,
} from './watch-rebuild.js';

afterEach(() => {
  vi.useRealTimers();
});

describe('watch-rebuild scheduling', () => {
  test('merges changes until filesystem writes become quiet', async () => {
    vi.useFakeTimers();
    const rebuild = vi.fn(async () => {});
    const schedule = createFileChangeScheduler(rebuild);

    schedule({
      kind: 'changes',
      fileChanges: {
        addedFiles: [],
        modifiedFiles: ['/app/workflow.ts'],
        removedFiles: [],
      },
    });
    await vi.advanceTimersByTimeAsync(99);
    schedule({
      kind: 'changes',
      fileChanges: {
        addedFiles: ['/app/helper.ts'],
        modifiedFiles: [],
        removedFiles: [],
      },
    });
    await vi.advanceTimersByTimeAsync(99);

    expect(rebuild).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(rebuild).toHaveBeenCalledWith({
      kind: 'changes',
      fileChanges: {
        addedFiles: ['/app/helper.ts'],
        modifiedFiles: ['/app/workflow.ts'],
        removedFiles: [],
      },
    });
  });

  test('collapses full rebuild requests while a rebuild runs', async () => {
    vi.useFakeTimers();
    let finishFirstBuild!: () => void;
    const firstBuild = new Promise<void>((resolve) => {
      finishFirstBuild = resolve;
    });
    let reportFullBuild!: () => void;
    const fullBuild = new Promise<void>((resolve) => {
      reportFullBuild = resolve;
    });
    const requests: string[] = [];
    const schedule = createFileChangeScheduler(async (request) => {
      requests.push(request.kind);
      if (requests.length === 1) {
        await firstBuild;
      } else {
        reportFullBuild();
      }
    });

    schedule({
      kind: 'changes',
      fileChanges: {
        addedFiles: [],
        modifiedFiles: ['/app/workflow.ts'],
        removedFiles: [],
      },
    });
    await vi.advanceTimersByTimeAsync(100);

    schedule({ kind: 'full' });
    schedule({ kind: 'full' });
    await vi.advanceTimersByTimeAsync(100);
    finishFirstBuild();
    await fullBuild;

    expect(requests).toEqual(['changes', 'full']);
  });
});

const detectWorkflowPatterns = (source: string) => ({
  hasDirective:
    source.includes("'use workflow'") ||
    source.includes('"use workflow"') ||
    source.includes("'use step'") ||
    source.includes('"use step"'),
  hasSerde: /Symbol\.for\(['"]workflow-(?:serialize|deserialize)['"]\)/.test(
    source
  ),
});

describe('watch-rebuild source snapshots', () => {
  test('ignores imports inside line and block comments', () => {
    const source = stripCommentsFromSource(`
import * as active from './workflows/active';
// import * as commentedLine from './workflows/commented-line';
/*
import * as commentedBlock from './workflows/commented-block';
*/
`);

    expect(extractImportSignature(source)).toBe('./workflows/active');
  });

  test('does not treat regex literals as comments', () => {
    const source = stripCommentsFromSource(`
const commentStartChars = /[/*]/;
const protocol = /https?:\\/\\//;
import * as active from './workflows/active';
`);

    expect(extractImportSignature(source)).toBe('./workflows/active');
  });

  test('includes comments in the content hash', () => {
    const source = 'const value = /* @__PURE__ */ createValue();';
    const changedSource = 'const value = /* @__NOPE__ */ createValue();';

    expect(
      createSourceSnapshotFromSource(source, detectWorkflowPatterns).contentHash
    ).not.toBe(
      createSourceSnapshotFromSource(changedSource, detectWorkflowPatterns)
        .contentHash
    );
  });

  test('ignores workflow definitions inside comments', () => {
    const snapshot = createSourceSnapshotFromSource(
      `
// export async function commentedWorkflow() { 'use workflow'; }
/*
export async function commentedStep() { 'use step'; }
*/
export async function realWorkflow() {
  'use workflow';
}
`,
      detectWorkflowPatterns
    );

    expect(snapshot.definitionSignature).toBe('workflow:realWorkflow');
    expect(snapshot.hasDirective).toBe(true);
  });

  test('commenting out a registry import requires full rediscovery', async () => {
    const registryFile = '/app/_workflows.ts';
    const workflowFile = '/app/workflows/1_simple.ts';
    const pageFile = '/app/app/page.tsx';
    const initialRegistrySource = `import * as workflow_1_simple from './workflows/1_simple';

export const allWorkflows = {
  'workflows/1_simple.ts': workflow_1_simple,
} as const;
`;
    const sources = new Map<string, string>([
      [registryFile, initialRegistrySource],
    ]);
    const sourceSnapshots = new Map<string, SourceSnapshot>([
      [
        registryFile,
        createSourceSnapshotFromSource(
          initialRegistrySource,
          detectWorkflowPatterns
        ),
      ],
    ]);

    sources.set(
      registryFile,
      `// import * as workflow_1_simple from './workflows/1_simple';

export const allWorkflows = {
  'workflows/1_simple.ts': workflow_1_simple,
} as const;
`
    );

    await expect(
      classifyRebuild({
        discoveredEntries: {
          discoveredSteps: new Set(),
          discoveredWorkflows: new Set([workflowFile]),
          discoveredSerdeFiles: new Set(),
          discoveredFiles: new Set([pageFile, registryFile, workflowFile]),
        },
        fileChanges: {
          addedFiles: [],
          modifiedFiles: [registryFile],
          removedFiles: [],
        },
        inputFiles: [pageFile],
        parentHasChild: () => false,
        readSnapshot: async (file) =>
          createSourceSnapshotFromSource(
            sources.get(file) ?? '',
            detectWorkflowPatterns
          ),
        sourceSnapshots,
      })
    ).resolves.toEqual({ kind: 'full' });
  });

  test('modified registry import without previous snapshot requires full rediscovery', async () => {
    const registryFile = '/app/_workflows.ts';
    const stepFile = '/app/workflows/dev-test-step-change.ts';
    const registrySource = `import './workflows/dev-test-step-change';

export const allWorkflows = {} as const;
`;
    const sources = new Map<string, string>([[registryFile, registrySource]]);

    await expect(
      classifyRebuild({
        discoveredEntries: {
          discoveredSteps: new Set(),
          discoveredWorkflows: new Set(),
          discoveredSerdeFiles: new Set(),
          discoveredFiles: new Set([registryFile]),
        },
        fileChanges: {
          addedFiles: [stepFile],
          modifiedFiles: [registryFile],
          removedFiles: [],
        },
        inputFiles: [registryFile],
        parentHasChild: () => false,
        readSnapshot: async (file) =>
          createSourceSnapshotFromSource(
            sources.get(file) ?? '',
            detectWorkflowPatterns
          ),
        sourceSnapshots: new Map(),
      })
    ).resolves.toEqual({ kind: 'full' });
  });

  test('ignores stale add events for already snapshotted files', async () => {
    const stepFile = '/app/workflows/hmr-fuzz-step.ts';
    const pageFile = '/app/app/page.tsx';
    const stepSource = `export async function hmrFuzzStep() {
  'use step';
  return 'step-value';
}
`;
    const sourceSnapshots = new Map<string, SourceSnapshot>([
      [
        stepFile,
        createSourceSnapshotFromSource(stepSource, detectWorkflowPatterns),
      ],
    ]);

    const decision = await classifyRebuild({
      discoveredEntries: {
        discoveredSteps: new Set([stepFile]),
        discoveredWorkflows: new Set(),
        discoveredSerdeFiles: new Set(),
        discoveredFiles: new Set([pageFile, stepFile]),
      },
      fileChanges: {
        addedFiles: [stepFile],
        modifiedFiles: [],
        removedFiles: [],
      },
      inputFiles: [pageFile],
      parentHasChild: () => false,
      readSnapshot: async () =>
        createSourceSnapshotFromSource(stepSource, detectWorkflowPatterns),
      sourceSnapshots,
    });

    expect(decision).toEqual({ kind: 'ignored' });
  });

  test('rebuilds relevant added files without snapshots', async () => {
    const helperFile = '/app/workflows/helper.ts';

    await expect(
      classifyRebuild({
        discoveredEntries: {
          discoveredSteps: new Set(),
          discoveredWorkflows: new Set(),
          discoveredSerdeFiles: new Set(),
          discoveredFiles: new Set([helperFile]),
        },
        fileChanges: {
          addedFiles: [helperFile],
          modifiedFiles: [],
          removedFiles: [],
        },
        inputFiles: [],
        parentHasChild: () => false,
        readSnapshot: async () =>
          createSourceSnapshotFromSource(
            "export const value = 'helper';\n",
            detectWorkflowPatterns
          ),
        sourceSnapshots: new Map(),
      })
    ).resolves.toEqual({ kind: 'full' });
  });

  test('ignores byte-identical modified files', async () => {
    const workflowFile = '/app/workflows/example.ts';
    const source = `export async function example() {
  'use workflow';
}
`;
    const snapshot = createSourceSnapshotFromSource(
      source,
      detectWorkflowPatterns
    );

    await expect(
      classifyRebuild({
        discoveredEntries: {
          discoveredSteps: new Set(),
          discoveredWorkflows: new Set([workflowFile]),
          discoveredSerdeFiles: new Set(),
          discoveredFiles: new Set([workflowFile]),
        },
        fileChanges: {
          addedFiles: [],
          modifiedFiles: [workflowFile],
          removedFiles: [],
        },
        inputFiles: [workflowFile],
        parentHasChild: () => false,
        readSnapshot: async () => snapshot,
        sourceSnapshots: new Map([[workflowFile, snapshot]]),
      })
    ).resolves.toEqual({ kind: 'ignored' });
  });

  test('rebuilds changed stale adds used by workflows', async () => {
    const helperFile = '/app/workflows/helper.ts';
    const workflowFile = '/app/workflows/workflow.ts';
    const previousHelperSnapshot = createSourceSnapshotFromSource(
      "export const value = 'before';\n",
      detectWorkflowPatterns
    );
    const nextHelperSnapshot = createSourceSnapshotFromSource(
      "export const value = 'after';\n",
      detectWorkflowPatterns
    );
    const workflowSnapshot = createSourceSnapshotFromSource(
      `export async function workflow() {
  'use workflow';
}
`,
      detectWorkflowPatterns
    );

    await expect(
      classifyRebuild({
        discoveredEntries: {
          discoveredSteps: new Set(),
          discoveredWorkflows: new Set([workflowFile]),
          discoveredSerdeFiles: new Set(),
          discoveredFiles: new Set([helperFile, workflowFile]),
        },
        fileChanges: {
          addedFiles: [helperFile],
          modifiedFiles: [workflowFile],
          removedFiles: [],
        },
        inputFiles: [workflowFile],
        parentHasChild: (parent, child) =>
          parent === workflowFile && child === helperFile,
        readSnapshot: async (file) => {
          if (file === helperFile) {
            return nextHelperSnapshot;
          }
          if (file === workflowFile) {
            return workflowSnapshot;
          }
          throw new Error(`Unexpected file: ${file}`);
        },
        sourceSnapshots: new Map([
          [helperFile, previousHelperSnapshot],
          [workflowFile, workflowSnapshot],
        ]),
      })
    ).resolves.toEqual({
      kind: 'hot',
      refreshStepRegistrations: false,
      snapshots: new Map([[helperFile, nextHelperSnapshot]]),
    });
  });
});
