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

_MERGE_TAG = "tag:yaml.org,2002:merge"


class UniqueKeySafeLoader(yaml.SafeLoader):
    """A safe loader that REFUSES a duplicate mapping key.

    PyYAML keeps the last one, so a cfg file declaring `targets:` twice loses
    the first block with no error at all.
    """

    def construct_mapping(self, node, deep=False):
        """Reject a repeated key, then construct as the safe loader would.

        Checked against the node's own keys BEFORE the base class flattens `<<`
        merges into them, so a key that legitimately overrides a merged one is
        not mistaken for a duplicate.
        """

        seen: set = set()
        for key_node, _ in node.value:
            # `<<` has no constructor until the base class flattens it away, and
            # a merged key the mapping also states explicitly is an override
            if key_node.tag == _MERGE_TAG:
                continue
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"duplicate key {key!r}",
                    key_node.start_mark,
                )
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def _represent_enum(dumper, value):
    return dumper.represent_data(value.value)


for _enum_type in (Enum,):
    yaml.SafeDumper.add_multi_representer(_enum_type, _represent_enum)


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


def load_yaml_text(text: str, *, label: str) -> dict:
    """Parse YAML held in memory. THE loader: everything else reaches it.

    Cfg is rewritten before it is parsed — alias and parameter binding — so the
    guard has to sit on text rather than on a path.
    """

    if not text.strip():
        return {}
    data = yaml.load(text, Loader=UniqueKeySafeLoader)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"❌ YAML must contain a mapping: {label}")
    return data


def load_yaml(path: Path | str) -> dict:
    """Parse a YAML file."""

    with open(path, encoding="utf-8") as f:
        return load_yaml_text(f.read(), label=str(path))


def write_yaml_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)
