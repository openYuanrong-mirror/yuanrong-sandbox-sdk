import inspect
import unittest
from dataclasses import fields

from yr_sandbox import (
    CommandInfo,
    Mount,
    NodeInfo,
    S3Config,
    SandboxInfo,
    resources,
)


class TypesTests(unittest.TestCase):
    def test_types_are_frozen(self):
        node = NodeInfo(
            id="node-a",
            status=0,
            capacity={"cpu": 1000.0},
            allocatable={"cpu": 800.0},
            labels={"arch": "arm64"},
        )
        self.assertTrue(type(node).__dataclass_params__.frozen)

    def test_akernel_value_type_fields_match(self):
        self.assertEqual(
            [item.name for item in fields(SandboxInfo)],
            ["id", "state", "cpu", "memory", "image"],
        )
        self.assertEqual(
            [item.name for item in fields(CommandInfo)],
            ["pid", "command", "running"],
        )
        self.assertEqual(
            [item.name for item in fields(NodeInfo)],
            ["id", "status", "capacity", "allocatable", "labels"],
        )

    def test_s3config_to_dict_and_redaction(self):
        cfg = S3Config(
            endpoint="https://s3.example",
            bucket="img",
            object="base",
            access_key="AK",
            secret_key="SK",
        )
        payload = cfg.to_dict()
        self.assertEqual(payload["object"], "base")
        self.assertIn("secretKey", payload)
        self.assertEqual(
            "SK",
            payload["secretKey"],
            "secret key must be carried in payload, but hidden from repr",
        )
        self.assertNotIn("SK", repr(cfg))

    def test_mount_is_immutable(self):
        m = Mount(target="/mnt/data", image_url="file://local")
        self.assertTrue(type(m).__dataclass_params__.frozen)

    def test_mount_rejects_relative_target(self):
        with self.assertRaisesRegex(ValueError, "absolute"):
            Mount(target="mnt/data", image_url="file://local")

    def test_mount_rejects_empty_image_url(self):
        with self.assertRaisesRegex(ValueError, "non-empty"):
            Mount(target="/mnt/data", image_url="")

    def test_resources_uses_process_configuration(self):
        self.assertEqual(list(inspect.signature(resources).parameters), [])


if __name__ == "__main__":
    unittest.main()
