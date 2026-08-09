# The ctl engine

Every module here owns one responsibility and is named for it. There is no
`utils`, `common`, `helpers` or `misc` — a name that does not say what is inside
is what lets anything be put there, and this package exists because one such file
reached 14460 lines.

## Layering

Dependencies run **downward only**. A module may reference names owned by a
package below it and never the reverse. This is measured, not assumed: the module
graph is a DAG with **zero** strongly-connected components. A cycle is not
resolved with an import inside a function — the function that creates it moves.

```
commands/          entry points: what a run DOES
cli/               what a runner accepts on the command line
preflight/         what must hold before the first target runs
state/             run records, their meaning, and their publication
catalog/           what is declared: targets and workflows
guardrails/        rendered cfg checked against recorded baselines
execution/         the run's identity, and the provider seam
cfg/               finding, merging, rendering and staging cfg
run/               the vocabulary a run is described in
kernel/            no domain knowledge at all
```

## What each package owns

| Package | Owns |
|---|---|
| `kernel/` | YAML, paths, hashing, subprocess, logging, ids, git, scalars, errors. Nothing here knows what a target or a run is. |
| `run/` | The engine's vocabulary: `actions` (the five, as enums with declared facts), `addressing` (how a thing is named and where its record lives), `selectors` (which member of a cfg block applies), `policy` (what a ctl profile permits). |
| `cfg/` | `layout` (the few filenames the engine may know), `resources`, `merge`, `tooling`, `validate`, `overlays`, `tree`, `materialize`, `views`, `presets`. |
| `execution/` | `references` (the `${execution_context.*}` language), `run_context` (assembling the context a run is carried out under), `providers` + `adapters` (the provider seam). |
| `guardrails/` | `policies` (declarations and baselines) and `verify`. |
| `catalog/` | `targets` (what is declared and what may run it) and `workflow` (composing an ordered run). |
| `state/` | `run_store` (storage), `status` (what the records MEAN), `sync` (publication), `lifecycle` (the transitions). |
| `preflight/` | `reports` (build), `render` (draw), `gates` (decide). |
| `commands/` | `selection`, `pipeline`, `status`, `maintenance`. |
| `cli/` | `args`. Nothing in the engine calls it; entry points do. |

Status keeps storage vocabulary and operator vocabulary separate. Target state
uses internal result groups. Workflow rows expose the derived public effect
`mutative|non_mutative` and retain exact resolved `actions`. Maintenance is not a
workflow action: direct target maintenance results, maintenance-owned runs, and
durable maintenance manifests are projected only by `status --maintenance`.

## Rules this package is held to

Enforced by tests, not convention:

- **No consumer vocabulary.** Engine core may name no execution-param axis a
  consumer declares — no `landing_zone`, no `main_tag`, no environment name.
  `scripts/tests/test_engine_names_no_consumer_vocabulary.py` reads the axes from
  the consumer's ctl cfg, so declaring a new one extends the check.
- **No provider name.** Which providers exist is `ctl_providers.yaml`; the engine
  reads the declaration. `tests/test_provider_boundary.py` walks this package and
  has no exemptions.
- **No tool name.** No `terraform`, `tofu`, `tfstate`, `.tf` — a tool lives inside
  a step, behind the step contract. `tests/test_tool_boundary.py`, same walk.
- **No project history.** No `# Phase N`, no `_legacy_`, no `_moved_`. Code states
  the current state. `scripts/tests/test_code_states_current_state.py`.
- **No undefined or doubly-defined names.** ruff F821/F811 over the whole engine,
  which covers branches no test reaches.

## Related functions live in a class named for the responsibility

A module is a directory of responsibilities, not a pile of functions. Ten
validators are a `CfgValidator`; the class name is what tells a reader they are a
set. `cfg/validate.py` is the reference: it was six flat `validate_*` functions
and is now `CommitPinning` (an instance, because `if not requires_commits(...)`
was the first line of four of them) and `CfgTreeShape` (static, because those
checks read a tree and hold nothing).

Which form to use:

| Signal | Form |
|---|---|
| every function threads the same value, or re-derives the same answer first | an **instance** holding it |
| each reads its argument and holds nothing between calls | **static** methods |
| a cluster reads and writes shared module-level mutable state | a class, and that state becomes its attributes |
| they share nothing but a filename | **two** classes — one namespace over unrelated functions is the junk drawer again |

## Two things that bite

**A path is anchored on a name, never counted in `..` steps.** Use
`kernel.paths.ctl_utils_root()`. A counted path still resolves to something that
exists once a module moves deeper, so the failure surfaces far from its cause —
it did so three times here in one afternoon.

**A `StrEnum` is a drop-in for `str` everywhere except PyYAML.** `yaml.safe_dump`
dispatches on exact type and raises `RepresenterError` on a member, so an enum
works through equality, dict keys, f-strings and `json.dumps` and then fails the
moment it reaches a run record. `kernel/yaml_io` registers a representer for
`Enum`; keep dumping through it.

## Importing

Modules are imported as objects and their names reached through them:

```python
from engine.state import run_store as state_run_store

state_run_store.load_run_metadata(run_dir)
```

Not `from engine.state.run_store import load_run_metadata` — that binds at import
time, so a test patching `state_run_store.load_run_metadata` would not be seen by
a caller that had already bound it. Package `__init__.py` files are empty for the
same reason a facade is refused: a heavy `__init__` executes every intermediate
module on import and hides the real graph.

The orchestrator cannot import this package at module scope — the version it must
use is declared in cfg and fetched during bootstrap — so it receives an `Engine`
handle whose attributes are these modules, named `<package>_<module>`.
