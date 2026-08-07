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
uv sync --locked
WORKFLOW_PUBLIC_MANIFEST=1 pnpm dev     # uvicorn on :3000
```

`--locked` because a plain `uv sync` will quietly rewrite `uv.lock` if your
personal `~/.config/uv/uv.toml` sets anything that affects resolution — see the
note above `[tool.uv.sources]` in `pyproject.toml`.

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

## Running it against the Vercel world

Both sides are wired: the `workbench-python-workflow` project is rooted at
`workbench/python` and Git-connected, and the `e2e-vercel-prod` matrix carries a
`python` row (excluded from the `quickjs` VM axis, which is a JS-engine dimension
with no Python meaning). What makes the build work:

- `vercel.json` declares `pyproject.toml` as the build src. That is what puts
  `@vercel/python` in "declared-only" mode — without it the `[tool.vercel]`
  keys are ignored, because the builder only attaches workflows to recognised
  Python frameworks or to declared builds, and a bare ASGI app is neither.
- `[tool.vercel] entrypoint = "app:app"` builds the web function. On Vercel it
  serves exactly one useful route, `manifest.json`. Runs arrive over the queue,
  so the hand-written `POST /flow` adapter is dead code there — it is how the
  *local* world delivers, and only that.
- `[[tool.vercel.workflows]] entrypoint = "app:registry"` builds the workflow
  function. At build time the builder imports `app`, reads
  `vercel.queue.get_subscriptions()`, and writes one `queue/v2beta` trigger per
  subscription. You can run that introspection yourself, exactly as the builder
  does:

  ```bash
  VERCEL=1 VERCEL_REGION=iad1 VERCEL_DEPLOYMENT_ID=dpl_introspection \
    uv run python -c 'import importlib; importlib.import_module("app")
  from vercel.queue import get_subscriptions
  print([(s.topic, s.consumer_group) for s in get_subscriptions()])'
  # [('__wkf_workflow_*', 'default')]
  ```

  One topic, not two: since vercel-py #251 a step invocation rides the workflow
  topic as a `stepId` on the invoke payload, the way the TypeScript SDK does it.

  `default` is the point. It is the consumer group `createWorkflowQueueTrigger`
  writes for the TypeScript SDK on the same topics, which is what makes the
  platform deliver to a Python consumer at all. Getting there needed
  vercel/vercel#17236 (`@vercel/python` 6.54.0), which replaced a consumer name
  derived from the output path with the introspected one, and it requires the
  installed `vercel` package to be >= 0.8.0.

`.python-version` pins 3.14 so the deployed interpreter matches the local venv;
the builder would otherwise default to 3.12.

The project also needs a `VERCEL_WORKFLOW_SERVER_URL` env var scoped to
**Preview**, with the same value every other workbench project has. On a PR the
`e2e-vercel-prod` job points the *driver* at a branch workflow-server
(`tests.yml:484`); without the matching variable on the project the deployed app
keeps writing to production `vercel-workflow.com`, and the two sides end up on
different primary stores. vercel-py reads it in
`_internal/workflow/worlds/vercel.py`. Production runs need nothing: the secret
resolves to `''` on `main`, so both sides use `vercel-workflow.com`.

That variable is about the *app* reaching the right store. The *driver* reaching
it is a separate permission, and it is not set in this repo: the branch
workflow-server (`e2e.vercel-workflow.com`) sits behind deployment protection,
and a preview run's driver clears it with the workbench project's own identity.
So `workbench-python-workflow` has to be listed in **that** project's Trusted
Sources, alongside the JS workbench projects. Until it is, every driver write
fails with `v4 createEvent: response missing required x-wf-* headers` and a
`SyntaxError: Unexpected token '<'` — the HTML SSO page, not a workflow-server
response. Production runs are unaffected: the secret is `''` on `main`, so the
driver talks to `vercel-workflow.com`, which is not protected.

CI reaches *this* deployment past deployment protection through the project's
`trustedSources.oidcProviders` entry for `token.actions.githubusercontent.com`.
A `trustedSources.projects` entry would additionally let a locally pulled
`VERCEL_OIDC_TOKEN` in; the other workbench projects have one, this project does
not need it for CI.

Nothing about the *protocol* is expected to be the hard part; the divergences
that will bite are catalogued in vercel-py's queue notes — region routing,
JSON-only queue transport, no delivery cap, and a one-second floor on immediate
re-enqueues.

## Conformance baseline

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
- **Imports reach into `vercel._internal`.** Both `workflow_entrypoint` and the
  `HTTPRequest` base are private. There is no public equivalent.
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
