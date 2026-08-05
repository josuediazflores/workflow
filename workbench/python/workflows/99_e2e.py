"""Python ports of the fixtures in `workbench/example/workflows/99_e2e.ts`.

The TypeScript e2e suite (`packages/core/e2e/e2e.test.ts`) is the single source
of truth for cross-language conformance: the driver stays in TypeScript, and the
app side gets reimplemented per language. This file is the Python app side.

Two conventions that look wrong and are not:

- **The module is named `99_e2e.py`**, matching the TS fixture file. That is not
  importable with an `import` statement, so `app.py` loads it with
  `importlib.import_module`. The name has to match because the harness looks
  fixtures up by file path (`workflows/99_e2e.ts`) and matches manifest keys by
  suffix.

- **Functions keep the TS fixtures' camelCase names** instead of being
  snake_cased. This keeps the manifest emitter mechanical — it can publish
  `qualname -> workflow_id` straight from the registry, with no TS-name to
  Python-name table to maintain and let rot. The camelCase names *are* the
  conformance contract.

Ported fixtures are listed in `../e2e-conformance.json`; a fixture missing from
that list is skipped by the suite. Only add a name there once its test passes.
"""

import asyncio
import random

from vercel.workflow import Workflows

# `as_vercel_job=False` because `app.py` wires the queue entrypoints itself: the
# default constructor creates them and discards the HTTP handlers, and calling
# the entrypoints again to recover a reference would register a second
# subscriber on the same topic and consumer group.
app = Workflows(as_vercel_job=False)


##########################################################
# nullByteWorkflow — 99_e2e.ts:291
#
# A NUL byte surviving the step return is a real cross-language signal: it has
# to round-trip the devalue codec and whatever the world writes to disk without
# being treated as a string terminator.


@app.step
async def nullByteStep() -> str:
    return "null byte \0"


@app.workflow
async def nullByteWorkflow() -> str:
    return await nullByteStep()


##########################################################
# promiseAllWorkflow — 99_e2e.ts:44
#
# `asyncio.gather` is Python's `Promise.all`, and the interesting part is the
# same in both: three steps suspend in a single turn, so the orchestrator has to
# create three pending events before the replay yields.
#
# `randomDelay` takes its argument by keyword because vercel-py rejects
# positional parameters at decoration time. That restriction is on the Python
# call, not on the wire, so it costs nothing here — unlike fixtures whose *run
# input* is positional (`[123]`), which cannot be ported at all yet.


@app.step
async def randomDelay(*, v: str) -> str:
    await asyncio.sleep(random.random() * 3)
    return v.upper()


@app.workflow
async def promiseAllWorkflow() -> str:
    a, b, c = await asyncio.gather(
        randomDelay(v="a"),
        randomDelay(v="b"),
        randomDelay(v="c"),
    )
    return a + b + c
