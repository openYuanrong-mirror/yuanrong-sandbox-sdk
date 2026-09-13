"""SDK wire contracts, including the scheduler's required-label truth table."""

import copy
import dataclasses
import unittest
from unittest.mock import patch

from yr_sandbox import LabelOperator, S3Config, Sandbox, ScheduleAffinity


def _create_body(**kwargs):
    with patch("yr_sandbox.sandbox_api.SandboxClient") as client:
        client.return_value.create_info.return_value = {"sandboxId": "default-test"}
        Sandbox(**kwargs)
        return client.return_value.create_info.call_args.args[0]


def _required_resource_matches(body, labels):
    # Mirror the public wire semantics of RequiredAffinityFilter: AND across
    # ordinary groups; OR across priority groups; AND within every group.
    # This is a contract oracle, not a live FunctionSystem integration test.
    groups = []
    for group in body.get("scheduleAffinities", []):
        promoted = (group["affinity"] == 0 and group["preferredPriority"]
                    and group["preferredAntiOtherLabels"])
        if group["kind"] == 0 and (group["affinity"] == 2 or promoted):
            groups.append(group)

    def matches(op):
        key = op["labelKey"]
        overlaps = bool(set(labels.get(key, ())) & set(op["labelValues"]))
        return {0: overlaps, 1: not overlaps, 2: key in labels, 3: key not in labels}[op["type"]]

    values = [all(matches(op) for op in group["labelOps"]) for group in groups]
    return any(values) if groups and groups[0]["preferredPriority"] else all(values)


class SchedulingTests(unittest.TestCase):
    def test_default_and_explicit_runtime_cover_all_rootfs_paths(self):
        for kwargs, runtime in [
            ({"cpu": 1000, "memory": 2048}, "runsc"),
            ({"runtime": " runc "}, "runc"),
            ({"runtime": "firecracker", "image": "example/image"}, "firecracker"),
            ({"runtime": "kata", "rootfs": S3Config("endpoint", "bucket", "object")}, "kata"),
            ({"runtime": "custom-runtime"}, "custom-runtime"),
        ]:
            with self.subTest(kwargs=kwargs):
                body = _create_body(**kwargs)
                self.assertEqual(body["rootfs"]["runtime"], runtime)
                self.assertNotIn("runtime", body)
                self.assertEqual(body["scheduleAffinities"], [{
                    "kind": 0, "affinity": 2,
                    "preferredPriority": False, "preferredAntiOtherLabels": False,
                    "labelOps": [{"type": 0, "labelKey": "sandbox.runtime", "labelValues": [runtime]}],
                }])

    def test_mixed_node_pool_and_no_compatible_node_contract(self):
        ecs = {"sandbox.runtime": ["runsc"]}
        baremetal = {"sandbox.runtime": ["runc", "firecracker"]}
        for runtime, expected in [("runsc", [True, False]), ("runc", [False, True]),
                                  ("firecracker", [False, True]), ("missing", [False, False])]:
            with self.subTest(runtime=runtime):
                body = _create_body(runtime=runtime)
                self.assertEqual([_required_resource_matches(body, node) for node in [ecs, baremetal]], expected)
                self.assertFalse(_required_resource_matches(body, {}))

    def test_required_runtime_node_and_user_labels_intersect(self):
        body = _create_body(node_id="ecs-a", schedule_affinities=[
            ScheduleAffinity([LabelOperator("zone", values=["a"])])
        ])
        node = {"sandbox.runtime": ["runsc"], "NODE_ID": ["ecs-a"], "zone": ["a"]}
        self.assertTrue(_required_resource_matches(body, node))
        for key, value in [("sandbox.runtime", ["runc"]), ("NODE_ID", ["ecs-b"]), ("zone", ["b"])]:
            with self.subTest(key=key):
                self.assertFalse(_required_resource_matches(body, {**node, key: value}))

    def test_explicit_runtime_affinity_remains_required(self):
        for explicit, expected in [("runsc", True), ("runc", False)]:
            body = _create_body(schedule_affinities=[
                ScheduleAffinity([LabelOperator("sandbox.runtime", values=[explicit])])
            ])
            self.assertEqual(_required_resource_matches(body, {"sandbox.runtime": ["runsc"]}), expected)
            self.assertFalse(_required_resource_matches(body, {"sandbox.runtime": ["runc"]}))

    def test_priority_alternatives_cannot_bypass_runtime_or_node(self):
        for mode, anti_other in [("required", False), ("preferred", True)]:
            with self.subTest(mode=mode):
                body = _create_body(node_id="ecs", schedule_affinities=[
                    ScheduleAffinity([LabelOperator("zone", values=[zone])], affinity=mode,
                                     preferred_priority=True, preferred_anti_other_labels=anti_other)
                    for zone in ("a", "b")
                ])
                self.assertEqual(len(body["scheduleAffinities"]), 2)
                for zone in ("a", "b"):
                    node = {"zone": [zone], "sandbox.runtime": ["runsc"], "NODE_ID": ["ecs"]}
                    self.assertTrue(_required_resource_matches(body, node))
                    self.assertFalse(_required_resource_matches(body, {**node, "sandbox.runtime": ["runc"]}))
                    self.assertFalse(_required_resource_matches(body, {**node, "NODE_ID": ["baremetal"]}))
                self.assertFalse(_required_resource_matches(body, {"zone": ["c"], "sandbox.runtime": ["runsc"], "NODE_ID": ["ecs"]}))

    def test_soft_instance_and_anti_affinities_preserve_wire_semantics(self):
        for kind in ("resource", "instance"):
            for mode in ("preferred", "preferred_anti", "required", "required_anti"):
                with self.subTest(kind=kind, mode=mode):
                    config = ScheduleAffinity([LabelOperator("zone", values=["a"])], kind=kind, affinity=mode)
                    body = _create_body(schedule_affinities=[config])
                    if (kind, mode) != ("resource", "required"):
                        self.assertEqual(body["scheduleAffinities"][0], config.to_dict())
                        self.assertEqual(len(body["scheduleAffinities"]), 2)

    def test_inconsistent_priority_for_same_selector_is_rejected_before_io(self):
        for mode, anti_other in [("required", False), ("preferred", True)]:
            groups = [
                ScheduleAffinity([LabelOperator("a", "exists")]),
                ScheduleAffinity([LabelOperator("b", "exists")], affinity=mode,
                                 preferred_priority=True, preferred_anti_other_labels=anti_other),
            ]
            with patch("yr_sandbox.sandbox_api.SandboxClient") as client:
                with self.assertRaisesRegex(ValueError, "preferred_priority"):
                    Sandbox(schedule_affinities=groups)
                client.assert_not_called()

    def test_configuration_is_immutable_and_reusable(self):
        values = ["a"]
        operators = [LabelOperator("zone", values=values)]
        config = ScheduleAffinity(operators)
        before = copy.deepcopy(config.to_dict())
        values.append("b")
        operators.clear()
        first = _create_body(runtime="runsc", schedule_affinities=[config])
        second = _create_body(runtime="runc", schedule_affinities=[config])
        first["scheduleAffinities"][0]["labelOps"][0]["labelValues"].append("changed")
        self.assertEqual(config.to_dict(), before)
        self.assertEqual(second["scheduleAffinities"][0]["labelOps"][0], before["labelOps"][0])
        with self.assertRaises(dataclasses.FrozenInstanceError):
            config.kind = "instance"

    def test_label_operator_wire_values_and_validation(self):
        for index, operator in enumerate(("in", "not_in", "exists", "not_exists")):
            values = ["a"] if index < 2 else []
            self.assertEqual(LabelOperator("key", operator, values).to_dict(),
                             {"type": index, "labelKey": "key", "labelValues": values})
        for args in [("",), (" ",), ("key", "bad"), ("key", "in"),
                     ("key", "not_in"), ("key", "exists", ["a"]),
                     ("key", "not_exists", ["a"])]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                LabelOperator(*args)
        for values in ("abc", None, [1], {"a": "b"}):
            with self.subTest(values=values), self.assertRaises(TypeError):
                LabelOperator("key", values=values)

    def test_invalid_affinities_and_runtime_fail_before_io(self):
        for value in ({}, "required", [None], [dict(kind=0)]):
            with patch("yr_sandbox.sandbox_api.SandboxClient") as client:
                with self.assertRaisesRegex(TypeError, "schedule_affinities"):
                    Sandbox(schedule_affinities=value)
                client.assert_not_called()
        for runtime in (None, "", " ", 1, True):
            with patch("yr_sandbox.sandbox_api.SandboxClient") as client:
                with self.assertRaisesRegex(ValueError, "runtime"):
                    Sandbox(runtime=runtime)
                client.assert_not_called()
        for kwargs in ({"kind": "node"}, {"affinity": "soft"}, {"preferred_priority": 1},
                       {"preferred_anti_other_labels": "false"}):
            with self.subTest(kwargs=kwargs), self.assertRaises((ValueError, TypeError)):
                ScheduleAffinity([LabelOperator("key", "exists")], **kwargs)
        for ops in ([], [None], "key", None):
            with self.subTest(ops=ops), self.assertRaises((ValueError, TypeError)):
                ScheduleAffinity(ops)

    def test_snapshot_create_preserves_runtime_inheritance_and_explicit_placement(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient") as client:
            client.return_value.create_info.return_value = {"sandboxId": "clone"}
            Sandbox.create("snapshot-runc")
            self.assertNotIn("scheduleAffinities", client.return_value.create_info.call_args.args[0])
        body = _create_body(snapshot_id="snapshot-runc", node_id="baremetal", schedule_affinities=[
            ScheduleAffinity([LabelOperator("sandbox.runtime", values=["runc"])])
        ])
        self.assertTrue(_required_resource_matches(body, {"sandbox.runtime": ["runc"], "NODE_ID": ["baremetal"]}))
        self.assertFalse(_required_resource_matches(body, {"sandbox.runtime": ["runsc"], "NODE_ID": ["baremetal"]}))
        self.assertNotIn("runsc", str(body["scheduleAffinities"]))
