"""Reading and writing the YAML the engine treats as data.

The loader REFUSES duplicate keys, which stock PyYAML accepts silently by
letting the last one win. Cfg is merged from many files and a duplicate is
always a mistake, so it is an error here rather than a value someone finds
later.

The dumper is taught to write a `StrEnum` as its value. `StrEnum` is otherwise a
drop-in for `str` — equality, dict keys, f-strings and `json.dumps` all behave —
but PyYAML dispatches on EXACT type and raises `RepresenterError` on a subclass
it does not know. Without this, every enum the engine adopts would work
everywhere until the moment it reached a run record."""

from enum import Enum
from pathlib import Path

import yaml


class UniqueKeySafeLoader(yaml.SafeLoader):
    pass


def _represent_enum(dumper, value):
    return dumper.represent_data(value.value)


for _enum_type in (Enum,):
    yaml.SafeDumper.add_multi_representer(_enum_type, _represent_enum)


def _construct_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"duplicate YAML key: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


def load_cfg_yaml(path: str):
    with open(path, encoding="utf-8") as f:
        raw = f.read()

    if not raw.strip():
        return {}

    data = yaml.load(raw, Loader=UniqueKeySafeLoader)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"cfg file must contain a mapping: {path}")
    return data


def write_cfg_yaml(path: str, data: dict, *, header_comment: str | None = None) -> None:
    rendered = yaml.safe_dump(data, sort_keys=False, allow_unicode=True)
    with open(path, "w", encoding="utf-8") as f:
        if header_comment:
            f.write(header_comment)
        f.write(rendered)


def collect_top_level_sections(cfg_root: Path, key: str) -> list[tuple[Path, object]]:
    sections: list[tuple[Path, object]] = []
    for yf in sorted(cfg_root.rglob("*.yaml")):
        data = load_yaml(yf) or {}
        if not isinstance(data, dict):
            continue
        if key in data:
            sections.append((yf, data[key]))
    return sections


def load_yaml(path: Path):
    with open(path, encoding="utf-8") as f:
        raw = f.read()

    if not raw.strip():
        return {}

    data = yaml.load(raw, Loader=UniqueKeySafeLoader)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ YAML file must contain a mapping: {path}")
    return data


def load_optional_yaml_mapping(path: Path) -> dict:
    """

    load an optional YAML mapping, returning {} when the file is absent."""

    if not path.is_file():
        return {}
    data = load_yaml(path) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ YAML file must contain a mapping: {path}")
    return data


def write_yaml_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)
