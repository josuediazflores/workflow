"""Runtime patches that let the pinned vercel-py read an event log written by the
current TypeScript SDK.

Everything in here is an upstream gap, not a design choice. Each patch says which
one, and the whole module should shrink to nothing as vercel-py catches up. Import
it before anything touches the world.

    import compat  # noqa: F401  (import for side effects)
    compat.apply()
"""

from __future__ import annotations

import pydantic
from vercel._internal.workflow import world as w


def _widen_spec_version() -> None:
    """Accept any integer `specVersion` on events.

    vercel-py declares `BaseEvent.spec_version` as `Literal[1, 2]` ("1: legacy
    JSON, 2: devalue"), and never widened it as the TypeScript spec version moved
    3 -> 4 -> 5. `@workflow/world-local` writes `SPEC_VERSION_CURRENT`, currently
    5, with no way to ask for less, so every read of a TS-written log fails
    validation before the runtime sees a single event.

    Widening is safe because the field is validation-only: nothing in vercel-py
    reads `spec_version` to decide how to decode. (`worlds/local.py` only ever
    copies it from the run row onto events it writes, or hard-codes 2 for runs it
    starts itself.) So the `Literal` rejects logs it would otherwise handle fine
    -- which is exactly what the passing conformance run demonstrates.
    """
    models: list[type[pydantic.BaseModel]] = [w.BaseEvent]

    def collect(cls: type[pydantic.BaseModel]) -> None:
        for sub in cls.__subclasses__():
            models.append(sub)
            collect(sub)

    collect(w.BaseEvent)

    for model in models:
        field = model.model_fields.get("spec_version")
        if field is not None:
            field.annotation = int
        model.model_rebuild(force=True)

    # `read_json(f, w.EventAdaptor)` resolves the attribute at call time, so
    # rebuilding the discriminated union here is enough -- but it has to happen
    # after the members are rebuilt, or the adaptor caches the old schema.
    w.EventAdaptor = pydantic.TypeAdapter(w.Event)


def apply() -> None:
    _widen_spec_version()
