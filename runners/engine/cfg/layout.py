"""The few names the engine is allowed to know about a cfg tree.

The engine derives cfg SHAPE from structure and metadata, never from hardcoded
filenames. This module is the exception list, and keeping it one small file is
what makes the rule checkable: anything the engine knows by name is here, and
everything else it discovers."""

from pathlib import Path
from engine.cfg import presets as cfg_presets

from engine.kernel import yaml_io as kernel_yaml_io

PLT_GUARDRAILS_FILENAME = "__guardrails__.yaml"


PLT_GUARDRAILS_DIRNAME = "__guardrails__"


CFG_SOURCE_KEYS = ("plt", "guardrails")


CFG_ROOT_META_FILENAME = "__cfg__.yaml"


def load_cfg_root_meta(cfg_root: Path) -> dict:
    path = cfg_root / CFG_ROOT_META_FILENAME
    if not path.is_file():
        return {}
    data = kernel_yaml_io.load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ {CFG_ROOT_META_FILENAME} must contain a mapping: {path}")
    return data


SCOPE_META_FILENAME = "__meta__.yaml"


SCOPE_COMPOSITION_FILENAME = "__scope_composition__.yaml"


# Declaration files configure composition and are never payload.
SCOPE_META_SKIP_FILENAMES = {
    SCOPE_META_FILENAME,
    PLT_GUARDRAILS_FILENAME,
    SCOPE_COMPOSITION_FILENAME,
    *cfg_presets.DECLARATION_FILENAMES,
}
