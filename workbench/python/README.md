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

## What is missing

This app is honest about being early. In rough order of how much it costs:

- **Positional run input is unsupported.** vercel-py enforces keyword-only
  parameters at decoration time, and `keyword_arguments()` raises
  `SerializationError` on a positional array. Every fixture whose test starts it
  with something like `[123]` is unportable until that changes — which is most of
  them, and the main reason `e2e-conformance.json` is two names long.
- **The `.well-known/workflow/v1` surface lives in `app.py`, not the SDK.**
  Deployed Python is driven by platform queue triggers on
  `/_py_workflows/<name>`, so vercel-py ships no manifest generator, no `/flow`
  route, and no concrete `HTTPRequest`. This app supplies all three. It does not
  translate any protocol — `LocalWorld.create_queue_handler` already reads
  exactly the headers `@workflow/world-local` sends — so the adapter is routing
  plus a manifest, and it belongs here because it implements a contract the SDK
  does not claim to serve.
- **`compat.py` patches the SDK at runtime.** vercel-py declares
  `BaseEvent.spec_version` as `Literal[1, 2]` and never widened it as the
  TypeScript spec version moved 3 → 4 → 5. Since `@workflow/world-local` writes
  the current version, every read of a TS-written log fails validation before the
  runtime sees an event. The field is validation-only — nothing reads it to decide
  how to decode — so the shim widens it to `int`. **A green run currently depends
  on this patch**; it should be deleted once vercel-py widens the field.
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
