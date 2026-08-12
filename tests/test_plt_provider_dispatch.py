"""Selected cfg units and source steps meet at one PLT-provider boundary."""

import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runners"))

from engine.cfg import materialize as cfg_materialize
from engine.plt import dispatch as plt_dispatch


class FakeProvider:
    name = "runtime"

    def __init__(self):
        self.materializations = []

    def materialize(self, **arguments):
        self.materializations.append(arguments)
        workspace = Path(arguments["workspace"])
        workspace.mkdir(parents=True)
        (workspace / "atmos.yaml").write_text("base_path: .\n")
        return {
            "resolved_cfg": [
                {
                    "target_path": "/env",
                    "values": {"main_tag": "test"},
                }
            ]
        }

    @staticmethod
    def execute(materialized, *, env=None):
        return None


class ProviderDispatchTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ctl_root = self.root / "ctl"
        self.ctl_root.mkdir()
        (self.ctl_root / "cfg_sources.yaml").write_text(
            "cfg_sources:\n  plt:\n    provider: runtime\n    repo_path: ../plt\n"
            "  guardrails:\n    repo_path: ../guardrails\n"
        )
        self.plt_root = self.root / "plt"
        scope = self.plt_root / "domains" / "env" / "dev"
        scope.mkdir(parents=True)
        (self.plt_root / "provider.yaml").write_text("provider_owned: true\n")
        (scope / "__meta__.yaml").write_text(
            yaml.safe_dump(
                {
                    "type": "scope",
                    "target_path": "/env",
                    "plt": {
                        "provider_cfg": {"entrypoint": "stack.yaml"},
                    },
                    "selectors": {
                        "contains": {"execution_context.target.domains": "env"},
                        "match": {"execution_context.params.env.type": "dev"},
                    },
                },
                sort_keys=False,
            )
        )
        (scope / "stack.yaml").write_text("provider_owned: true\n")
        self.provider = FakeProvider()
        self.registry = unittest.mock.Mock()
        self.registry.entries = {"runtime": {}}
        self.registry.adapter.return_value = self.provider
        patcher = unittest.mock.patch.object(
            plt_dispatch.plt_providers,
            "ProviderRegistry",
            return_value=self.registry,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.dispatch = plt_dispatch.ProviderDispatch(self.ctl_root, self.plt_root)
        self.context = {
            "execution_context.target.domains": ["env"],
            "execution_context.params.env.type": "dev",
        }

    def test_selection_does_not_load_provider_payload(self):
        selection = self.dispatch.select(
            {"plt_overlays": []},
            self.context,
            scope_params={"env.type": "dev"},
        )

        self.assertEqual("runtime", selection["provider"])
        self.assertEqual(
            {
                "type": "scope",
                "relative_path": "domains/env/dev",
                "target_path": "/env",
                "provider_cfg": {"entrypoint": "stack.yaml"},
            },
            selection["selected_units"][0],
        )

    def test_selector_free_shared_scope_is_selected_only_by_import(self):
        self.dispatch.provider = None
        shared = self.plt_root / "domains" / "shared"
        shared.mkdir()
        (shared / "__meta__.yaml").write_text(
            yaml.safe_dump(
                {
                    "type": "shared_scope",
                    "target_path": "/shared",
                    "plt": {"provider": "atlas"},
                },
                sort_keys=False,
            )
        )
        (shared / "common.yaml").write_text("common:\n  a: value-a\n")
        scope = self.plt_root / "domains" / "env" / "dev"
        (scope / "__imports__.yaml").write_text(
            'imports:\n  - from: /domains/shared\n    import: "*"\n'
        )
        meta = yaml.safe_load((scope / "__meta__.yaml").read_text())
        meta["plt"]["provider"] = "runtime"
        (scope / "__meta__.yaml").write_text(yaml.safe_dump(meta, sort_keys=False))
        self.registry.adapter.side_effect = lambda provider: {
            "atlas": unittest.mock.Mock(name="atlas"),
            "runtime": self.provider,
        }[provider]

        selection = self.dispatch.select(
            {"plt_overlays": []},
            self.context,
            scope_params={"env.type": "dev"},
        )

        self.assertEqual(["atlas", "runtime"], selection["providers"])
        shared_unit = next(
            unit for unit in selection["selected_units"] if unit["type"] == "shared_scope"
        )
        self.assertEqual("domains/shared", shared_unit["relative_path"])
        self.assertEqual([], shared_unit["imports"])

    def test_precheck_writes_provider_resolved_values_for_guardrails(self):
        source = self.root / "source"
        step = source / "atlas_ctl_adapter" / "steps" / "plan" / "component"
        step.mkdir(parents=True)
        (source / "atlas_ctl_adapter" / "manifest.yaml").write_text(
            "manifest:\n  plan:\n    plan/component:\n"
            "      path: atlas_ctl_adapter/steps/plan/component\n"
        )
        (source / "atlas_ctl_adapter" / "procedures.yaml").write_text(
            "procedures:\n  plan:\n    component:\n      steps: [plan/component]\n"
        )
        (step / "step.yaml").write_text(
            yaml.safe_dump(
                {
                    "id": "plan/component",
                    "providers": ["execution"],
                    "cfg_keys": {},
                    "runtime": {"image": "infra"},
                },
                sort_keys=False,
            )
        )

        rendered, selection = self.dispatch.prepare_target_view(
            "component",
            {"source": "seed", "procedure": "component", "plt_overlays": []},
            execution_context=self.context,
            target_cfg_dir=self.root / "view",
            scope_params={"env.type": "dev"},
        )

        self.assertEqual("runtime", selection["provider"])
        self.assertEqual(
            {"main_tag": "test"},
            yaml.safe_load(next((rendered / "env").glob("*.yaml")).read_text()),
        )
        self.assertTrue((self.root / "view" / "provider" / "atmos.yaml").is_file())
        materialization = self.provider.materializations[0]
        self.assertNotIn("source", materialization)
        self.assertNotIn("target_source_root", materialization)

    def test_provider_values_are_distributed_to_universal_step_input(self):
        pipeline_cfg = self.root / "pipeline.yaml"
        pipeline_cfg.write_text(
            yaml.safe_dump(
                {
                    "target_runs": {
                        "env/component": {
                            "plt_provider": {"provider": "runtime"},
                            "domains": ["env"],
                            "cfg_keys": {"env": ["main_tag"]},
                        }
                    }
                }
            )
        )
        views = self.root / "views"
        rendered = views / "env/component" / "rendered" / "env"
        rendered.mkdir(parents=True)
        (rendered / "000-values.yaml").write_text("main_tag: test\nignored: value\n")

        cfg_materialize.run_cfg_distribution(pipeline_cfg, views)

        self.assertEqual(
            {"main_tag": "test"},
            yaml.safe_load((views / "env/component" / "input" / "env.yaml").read_text()),
        )

    def test_scope_graph_passes_universal_values_between_providers(self):
        class Producer:
            def materialize(self, **arguments):
                Path(arguments["workspace"]).mkdir(parents=True)
                return {
                    "resolved_cfg": [
                        {
                            "target_path": "/shared",
                            "values": {"common": {"a": "value-a"}},
                        }
                    ]
                }

        class Consumer:
            def __init__(self):
                self.imported = None

            def materialize(self, **arguments):
                self.imported = arguments["imported_values"]
                Path(arguments["workspace"]).mkdir(parents=True)
                return {
                    "resolved_cfg": [
                        {
                            "target_path": "/env",
                            "values": {"result": self.imported["common"]["a"]},
                        }
                    ]
                }

        consumer = Consumer()
        self.registry.adapter.side_effect = lambda provider: {
            "atlas": Producer(),
            "runtime": consumer,
        }[provider]
        self.dispatch.provider = None
        selection = {
            "providers": ["atlas", "runtime"],
            "selected_units": [
                {
                    "type": "scope",
                    "relative_path": "domains/shared",
                    "target_path": "/shared",
                    "provider": "atlas",
                    "imports": [],
                },
                {
                    "type": "scope",
                    "relative_path": "domains/env/dev",
                    "target_path": "/env",
                    "provider": "runtime",
                    "imports": [
                        {
                            "from": "domains/shared",
                            "import": "*",
                            "as": None,
                        }
                    ],
                },
            ],
        }

        materialized = self.dispatch.materialize_scope(
            selection,
            execution_context=self.context,
            workspace=self.root / "graph",
        )

        self.assertEqual({"common": {"a": "value-a"}}, consumer.imported)
        self.assertEqual(
            {"result": "value-a"},
            materialized["resolved_cfg"][1]["values"],
        )

    def test_scope_graph_rejects_cycles_before_provider_execution(self):
        self.dispatch.provider = None
        selection = {
            "providers": ["runtime"],
            "selected_units": [
                {
                    "type": "scope",
                    "relative_path": "one",
                    "target_path": "/one",
                    "provider": "runtime",
                    "imports": [{"from": "two", "import": "*", "as": None}],
                },
                {
                    "type": "scope",
                    "relative_path": "two",
                    "target_path": "/two",
                    "provider": "runtime",
                    "imports": [{"from": "one", "import": "*", "as": None}],
                },
            ],
        }
        with self.assertRaisesRegex(RuntimeError, "scope import cycle"):
            self.dispatch.materialize_scope(
                selection,
                execution_context=self.context,
                workspace=self.root / "cycle",
            )

    def test_full_cfg_validation_uses_provider_dispatch(self):
        validator = (Path(__file__).resolve().parents[1] / "cfg" / "validate_cfg.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("plt_dispatch.ProviderDispatch", validator)
        self.assertIn("provider_dispatch.prepare_target_view", validator)


if __name__ == "__main__":
    unittest.main()
