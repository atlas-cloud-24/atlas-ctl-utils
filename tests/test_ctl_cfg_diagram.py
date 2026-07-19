import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "cfg"))

import generate_ctl_cfg_diagram as diagram  # noqa: E402


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def make_cfg(root: Path) -> Path:
    cfg = root / "ctl-cfg"
    write(
        cfg / "arbitrary" / "fan.yaml",
        "fan_outs:\n"
        "  deploy-all:\n"
        "    runs:\n"
        "    - workflow_key: deploy\n",
    )
    write(
        cfg / "not-workflows.yaml",
        "workflows:\n"
        "  deploy:\n"
        "    actions: [provision]\n"
        "    target_keys: [app]\n",
    )
    write(
        cfg / "anything.yaml",
        "targets:\n"
        "  app:\n"
        "    actions: [provision]\n"
        "    source_key: app-source\n"
        "    execution_identity_key: deploy-group\n"
    )
    write(
        cfg / "catalog-a.yaml",
        "target_sources:\n"
        "  app-source:\n"
        "    repo_url: https://example.test/app.git\n",
    )
    write(
        cfg / "catalog-b.yaml",
        "execution_identities:\n"
        "  deploy-group:\n"
        "    provider: aws\n"
        "    members:\n"
        "    - identity_key: deploy-dev\n"
        "  deploy-dev:\n"
        "    provider: aws\n"
        "    account_key: dev\n"
        "  sync-dev:\n"
        "    provider: aws\n"
        "    account_key: dev\n",
    )
    write(
        cfg / "catalog-c.yaml",
        "ctl_state_backends:\n"
        "  env:\n"
        "    provider: aws\n"
        "    backend_type: s3\n"
        "    bucket_name: example-env-ctl-state\n"
        "    execution_identity_keys:\n      sync: sync-${execution_context.params.account}\n",
    )
    return cfg


class CtlCfgDiagramTests(unittest.TestCase):
    def test_builds_four_separate_dependency_views(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp))

            def edge(source_kind, source_key, target_kind, target_key, label=""):
                source = diagram.node_id(source_kind, source_key)
                target = diagram.node_id(target_kind, target_key)
                return (
                    f'{source} -->|"{label}"| {target}'
                    if label
                    else f"{source} --> {target}"
                )

            general = diagram.build_diagram(
                cfg,
                action="provision",
                view="general",
            )
            self.assertIn("flowchart LR", general)
            self.assertIn('"fontSize":"28px"', general)
            self.assertIn('"defaultRenderer":"elk"', general)
            self.assertIn("Fan-outs", general)
            self.assertIn("Workflows", general)
            self.assertIn("Targets", general)
            self.assertNotIn("Sources", general)
            self.assertIn(
                edge("fanout", "deploy-all", "workflow", "provision:deploy", "workflow"),
                general,
            )
            self.assertIn(
                edge("workflow", "provision:deploy", "target", "provision:app"),
                general,
            )

            sources = diagram.build_diagram(
                cfg,
                action="provision",
                view="sources",
            )
            self.assertIn("Targets", sources)
            self.assertIn("Sources", sources)
            self.assertNotIn("Workflows", sources)
            self.assertIn(
                edge("target", "provision:app", "source", "app-source"),
                sources,
            )

            ctl_state = diagram.build_diagram(
                cfg,
                action="provision",
                view="ctl_state_buckets",
            )
            self.assertIn("Targets", ctl_state)
            self.assertIn("Ctl-state namespaces", ctl_state)
            self.assertNotIn("Execution identities", ctl_state)
            self.assertIn(
                edge("target", "provision:app", "backend", "env", "invocation-selected"),
                ctl_state,
            )
            self.assertNotIn("sync-dev", ctl_state)

            identities = diagram.build_diagram(
                cfg,
                action="provision",
                view="execution_identities",
            )
            self.assertIn("Targets", identities)
            self.assertIn("Execution identities", identities)
            self.assertNotIn("Ctl-state buckets", identities)
            self.assertIn(
                edge("target", "provision:app", "identity", "deploy-group"),
                identities,
            )
            self.assertIn(
                edge("identity", "deploy-group", "identity", "deploy-dev", "member"),
                identities,
            )
            self.assertNotIn("sync-dev", identities)

    def test_content_is_independent_of_cfg_filenames(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = make_cfg(root / "first")
            second = root / "second" / "ctl-cfg"
            for index, source in enumerate(sorted(first.rglob("*.yaml"))):
                write(second / f"renamed-{index}.yaml", source.read_text(encoding="utf-8"))

            self.assertEqual(
                diagram.build_diagram(
                    first,
                    action="provision",
                    view="general",
                ),
                diagram.build_diagram(
                    second,
                    action="provision",
                    view="general",
                ),
            )

    def test_dynamic_reference_links_every_matching_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp))
            write(
                cfg / "anything.yaml",
                "targets:\n"
                "  app:\n"
                "    actions: [provision]\n"
                "    source_key: app-source\n"
                "    execution_identity_key: runtime-${execution_context.params.account}\n"
                    )
            write(
                cfg / "extra.yaml",
                "execution_identities:\n"
                "  runtime-dev:\n"
                "    provider: aws\n"
                "    account_key: dev\n"
                "  runtime-prod:\n"
                "    provider: aws\n"
                "    account_key: prod\n",
            )
            rendered = diagram.build_diagram(
                cfg,
                action="provision",
                view="execution_identities",
            )

            target = diagram.node_id("target", "provision:app")
            self.assertIn(
                f"{target} --> {diagram.node_id('identity', 'runtime-dev')}",
                rendered,
            )
            self.assertIn(
                f"{target} --> {diagram.node_id('identity', 'runtime-prod')}",
                rendered,
            )

    def test_missing_reference_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp))
            (cfg / "catalog-a.yaml").write_text("target_sources: {}\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "references missing key 'app-source'"):
                diagram.build_diagram(
                    cfg,
                    action="provision",
                    view="sources",
                )

    def test_action_filter_keeps_only_selected_workflows_and_targets(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp))
            write(
                cfg / "plan.yaml",
                "workflows:\n  inspect:\n    actions: [plan]\n    target_keys: [inspect-app]\n"
                "targets:\n  inspect-app:\n    actions: [plan]\n    source_key: app-source\n"
                "    execution_identity_key: deploy-group\n",
            )
            rendered = diagram.build_diagram(
                cfg,
                action="plan",
                view="general",
            )

            self.assertIn("plan<br/>inspect", rendered)
            self.assertNotIn("provision<br/>deploy", rendered)
            self.assertNotIn("sync-prod", rendered)

    def test_action_filter_ignores_fan_outs_for_other_actions(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp))
            write(
                cfg / "destroy.yaml",
                "workflows:\n  remove:\n    actions: [destroy]\n    target_keys: [remove-app]\n"
                "targets:\n  remove-app:\n    actions: [destroy]\n    source_key: app-source\n"
                "    execution_identity_key: deploy-group\n"
                    )

            rendered = diagram.build_diagram(
                cfg,
                action="destroy",
                view="general",
            )

            self.assertIn("destroy<br/>remove", rendered)
            self.assertNotIn("deploy-all", rendered)

    def test_main_writes_only_per_action_outputs_under_diagrams(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = make_cfg(Path(tmp))
            argv = [
                "generate_ctl_cfg_diagram.py",
                "--ctl-cfg-root",
                str(cfg),
                "--mmd-only",
            ]

            with mock.patch.object(sys, "argv", argv):
                self.assertEqual(diagram.main(), 0)

            for view in diagram.DIAGRAM_VIEWS:
                self.assertTrue(
                    (
                        cfg
                        / "diagrams"
                        / "provision"
                        / view
                        / "diagram.mmd"
                    ).is_file()
                )
            self.assertFalse(
                (cfg / "diagrams" / "ctl-cfg-architecture-provision.mmd").exists()
            )
            self.assertFalse((cfg / "ctl-cfg-architecture.mmd").exists())

    def test_render_svg_invokes_mmdc_and_requires_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mmd_path = root / "diagram.mmd"
            svg_path = root / "diagram.svg"
            write(mmd_path, "flowchart LR\n  a --> b\n")

            def fake_run(command, **kwargs):
                self.assertIn("--input", command)
                self.assertIn("--output", command)
                output_path = Path(command[command.index("--output") + 1])
                write(output_path, "<svg></svg>\n")
                return subprocess.CompletedProcess(command, 0, "", "")

            with mock.patch.object(diagram.shutil, "which", return_value="/usr/bin/mmdc"), mock.patch.object(
                diagram.subprocess, "run", side_effect=fake_run
            ):
                diagram.render_svg(mmd_path, svg_path)

            self.assertTrue(svg_path.is_file())
            self.assertEqual(svg_path.read_text(encoding="utf-8"), "<svg></svg>\n")


if __name__ == "__main__":
    unittest.main()
