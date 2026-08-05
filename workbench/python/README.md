# Python workbench app

A Python implementation of the workflow app side, so `packages/core/e2e/e2e.test.ts`
has something other than JavaScript to run against. The test driver stays in
TypeScript — one source of truth for what the protocol is — and this app is the
second implementation it drives.

Built on [vercel-py](https://github.com/vercel/vercel-py), pinned by commit in
`pyproject.toml`. Bump that `rev` deliberately and re-run the suite when you do.

## Running it

```bash
cd workbench/python
uv sync
WORKFLOW_PUBLIC_MANIFEST=1 pnpm dev     # uvicorn on :3000
```

Then, from the repo root:

```bash
DEPLOYMENT_URL="http://localhost:3000" APP_NAME="python" \
  pnpm vitest run packages/core/e2e/e2e.test.ts
```

Both sides talk to the same `world-local` data directory
(`WORKFLOW_LOCAL_DATA_DIR`, defaulting to `.workflow-data` here) with
byte-compatible file formats, so the TypeScript CLI can inspect runs the Python
app produced:

```bash
cd workbench/python && node ./node_modules/workflow/bin/run.js inspect --json runs
```

Which fixtures the suite will run is declared in `e2e-conformance.json`. Anything
not listed there is skipped; anything listed that the app stops registering fails
the run rather than quietly skipping. Add a name only once its test passes.

That file has a second axis, `unsupported`, keyed by test name rather than
fixture name. It exists because "did you port this workflow" and "does your
runtime implement this protocol behaviour" are different questions, and one test
answers the second one about a fixture that *is* ported — see the entry there.
It ratchets in the direction that can rot: a name listed under `unsupported`
that matches no test in the suite is a hard failure, so a renamed test cannot
leave a stale exemption behind.

Current baseline: **8 passing, 129 skipped, of 137.**

## What is missing

This app is honest about being early. In rough order of how much it costs:

- **Most fixtures are simply not ported yet** — 66 tests across 52 fixtures.
  They are not blocked on one thing anymore: the largest blocks are hooks (19
  tests, where vercel-py's `BaseHook.wait()` has a different shape than the
  async-iterable hook the fixtures use), streams (11), `setAttributes` (9, no
  Python equivalent), and `FatalError` / `RetryableError` (7, not exported by
  `vercel.workflow.errors`).
- **A run whose `run_created` write failed never starts.** vercel-py's workflow
  handler reads the run row before replaying (`runtime.py:529`) and 500s when it
  is absent, where the TypeScript runtime bootstraps from `run_started` using the
  input carried in the queue message. This is the one test that fails rather than
  skips, so it is recorded under `unsupported`.
- **The `.well-known/workflow/v1` surface lives in `app.py`, not the SDK.**
  Deployed Python is driven by platform queue triggers on
  `/_py_workflows/<name>`, so vercel-py ships no manifest generator, no `/flow`
  route, and no concrete `HTTPRequest`. This app supplies all three. It does not
  translate any protocol — `LocalWorld.create_queue_handler` already reads
  exactly the headers `@workflow/world-local` sends — so the adapter is routing
  plus a manifest, and it belongs here because it implements a contract the SDK
  does not claim to serve.
- **Imports reach into `vercel._internal`.** `workflow_entrypoint`,
  `step_entrypoint`, and the `HTTPRequest` base are all private. There is no
  public equivalent.
- **No health check.** vercel-py defines `HealthCheckPayload` and nothing consumes
  it, so the three health-check tests are marked JS-only. `GET flow?__health`
  here answers `{"status": "ok"}` for port discovery only — it is not the
  `healthCheck()` protocol from `@workflow/core`.
- **No webhook route and no app-specific API routes**, so the webhook and
  direct-step-call tests are JS-only too.

## Conventions that look wrong and are not

- `workflows/99_e2e.py` starts with a digit, so it cannot be `import`ed by
  statement; `app.py` loads it with `importlib.import_module`. The name has to
  match the TypeScript fixture file because the suite looks fixtures up by path
  and matches manifest keys on the stem.
- The fixtures keep the TypeScript functions' **camelCase** names. That keeps the
  manifest emitter mechanical — `qualname -> workflow_id` straight out of the
  registry — instead of a name-mapping table that would rot. The camelCase names
  are the conformance contract.
