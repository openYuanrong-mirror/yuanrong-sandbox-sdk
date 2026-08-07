import asyncio
import inspect
import unittest
from unittest.mock import patch

from yr_sandbox import (
    PortForwarding,
    S3Config,
    Sandbox,
)
from yr_sandbox.commands import Commands
from yr_sandbox.shell import Shells


class _FakeClient:
    created = []

    def __init__(self):
        self.calls = []
        self.closed = False

    def create_info(self, body):
        type(self).created.append(dict(body))
        return {"sandboxId": "sandbox-1", "status": "running"}

    def invoke(self, sandbox_id, action, args, **_kwargs):
        self.calls.append((sandbox_id, action, args))
        if action == "process.exec":
            return {"stdout": "", "stderr": "", "exit_code": 0}
        if action == "process.list":
            return {
                "processes": [
                    {"pid": 7, "cmd": "sleep 1", "running": True},
                    {"pid": 8, "cmd": "true", "running": False},
                ]
            }
        if action in ("shell.create", "shell.close"):
            return {}
        if action == "shell.run":
            return {}
        if action == "shell.poll":
            return {"status": "done", "stdout": "", "stderr": "", "exit_code": 0}
        raise AssertionError(action)

    def instance_info(self, sandbox_id):
        return {
            "id": sandbox_id,
            "status": "running",
            "required_cpu": 1000,
            "required_mem": 4096,
            "image": "ubuntu:22.04",
        }

    def delete(self, _sandbox_id):
        pass

    def close(self):
        self.closed = True


class SDKContractTests(unittest.TestCase):
    def setUp(self):
        _FakeClient.created.clear()

    def test_sandbox_runtime_and_default_cwd_are_applied_without_frontend_cwd_field(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox(
                image="ubuntu:22.04",
                runtime="kata",
                cwd="/workspace",
                detached=True,
            )
            sandbox.commands.run("pwd", timeout=10)

        body = _FakeClient.created[-1]
        self.assertEqual(body["runtime"], "kata")
        self.assertNotIn("runtime", body["rootfs"])
        self.assertEqual(body["rootfs"]["imageurl"], "ubuntu:22.04")
        self.assertNotIn("image", body)
        self.assertNotIn("cwd", body)
        self.assertNotIn("cwdMode", body)
        self.assertEqual(
            sandbox._client.calls[-1][2]["cwd"],
            "/workspace",
        )

    def test_sandbox_forwards_runtime_without_owning_runtime_registry(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                runtime="gvisor-next",
                detached=True,
            )

        body = _FakeClient.created[-1]
        self.assertEqual(body["runtime"], "gvisor-next")
        self.assertNotIn("runtime", body["rootfs"])

    def test_node_id_is_encoded_as_frontend_affinity_semantics(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                node_id="node-a",
                detached=True,
            )

        body = _FakeClient.created[-1]
        self.assertNotIn("nodeId", body)
        self.assertEqual(
            body["scheduleAffinities"],
            [
                {
                    "kind": 0,
                    "affinity": 2,
                    "labelOps": [
                        {
                            "type": 0,
                            "labelKey": "NODE_ID",
                            "labelValues": ["node-a"],
                        }
                    ],
                }
            ],
        )

    def test_xpu_is_validated_and_forwarded_without_normalization(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                xpu="GPU:L20:2",
                detached=True,
            )

        self.assertEqual(_FakeClient.created[-1]["xpu"], "GPU:L20:2")

    def test_xpu_without_model_is_forwarded_without_normalization(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                xpu="GPU::2",
                detached=True,
            )

        self.assertEqual(_FakeClient.created[-1]["xpu"], "GPU::2")

    def test_xpu_defaults_to_no_request(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(image="ubuntu:22.04", detached=True)

        self.assertNotIn("xpu", _FakeClient.created[-1])
        self.assertIsNone(inspect.signature(Sandbox).parameters["xpu"].default)

    def test_invalid_xpu_is_rejected_before_create(self):
        invalid_cases = [
            (1, TypeError, "string or None"),
            ("", ValueError, "exactly three fields"),
            ("gpu:l20", ValueError, "exactly three fields"),
            ("gpu:l20:1:0", ValueError, "exactly three fields"),
            (":l20:1", ValueError, "exactly three fields"),
            ("gpu:l20:", ValueError, "exactly three fields"),
            ("npu:910b:1", ValueError, "unsupported xpu type"),
            ("gpu:l20:0", ValueError, "positive integer"),
            ("gpu:l20:-1", ValueError, "positive integer"),
            ("gpu:l20:1.5", ValueError, "positive integer"),
            ("gpu:l20:1,gpu:h100:1", ValueError, "exactly three fields"),
            (" gpu:l20:1", ValueError, "whitespace"),
        ]
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for xpu, error_type, message in invalid_cases:
                with self.subTest(xpu=xpu):
                    with self.assertRaisesRegex(error_type, message):
                        Sandbox(image="ubuntu:22.04", xpu=xpu)
        self.assertEqual(_FakeClient.created, [])

    def test_storage_is_validated_and_forwarded(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                storage_mb=153600,
                detached=True,
            )

        body = _FakeClient.created[-1]
        self.assertEqual(body["storageMb"], 153600)

    def test_storage_default_is_omitted(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(image="ubuntu:22.04", detached=True)

        body = _FakeClient.created[-1]
        self.assertNotIn("storageMb", body)
        signature = inspect.signature(Sandbox).parameters
        self.assertIsNone(signature["storage_mb"].default)

    def test_invalid_storage_is_rejected_before_create(self):
        invalid_cases = [
            (True, TypeError),
            ("1024", TypeError),
            (0, ValueError),
            (-1, ValueError),
        ]
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for storage_mb, error_type in invalid_cases:
                with self.subTest(storage_mb=storage_mb):
                    with self.assertRaises(error_type):
                        Sandbox(image="ubuntu:22.04", storage_mb=storage_mb)
        self.assertEqual(_FakeClient.created, [])

    def test_commands_list_reads_rrt_running_field(self):
        client = _FakeClient()
        processes = Commands(client, "sandbox-1").list()
        self.assertEqual(processes[0].command, "sleep 1")
        self.assertTrue(processes[0].running)
        self.assertFalse(processes[1].running)

    def test_commands_list_accepts_legacy_status_field(self):
        class _LegacyClient(_FakeClient):
            def invoke(self, sandbox_id, action, args, **kwargs):
                if action == "process.list":
                    return {
                        "processes": [
                            {"pid": 7, "cmd": "sleep 1", "status": "running"},
                            {"pid": 8, "cmd": "true", "status": "done"},
                        ]
                    }
                return super().invoke(sandbox_id, action, args, **kwargs)

        processes = Commands(_LegacyClient(), "sandbox-1").list()
        self.assertTrue(processes[0].running)
        self.assertFalse(processes[1].running)

    def test_shell_uses_sandbox_default_cwd(self):
        client = _FakeClient()
        shells = Shells(client, "sandbox-1", default_cwd="/workspace")
        asyncio.run(shells.create())
        shell_run = next(call for call in client.calls if call[1] == "shell.run")
        self.assertIn("cd /workspace", shell_run[2]["command"])

    def test_get_info_uses_frontend_instance_summary(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox(image="ubuntu:22.04", detached=True)
            info = sandbox.get_info()
            self.assertEqual(info.id, "sandbox-1")
            self.assertEqual(info.state, "running")
            self.assertTrue(sandbox.is_running())

    def test_sandbox_without_explicit_rootfs_preserves_cluster_default(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(detached=True)

        body = _FakeClient.created[-1]
        self.assertNotIn("image", body)
        self.assertNotIn("rootfs", body)

    def test_runtime_override_without_explicit_rootfs_preserves_cluster_default(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(runtime="kata", detached=True)

        body = _FakeClient.created[-1]
        self.assertEqual(body["runtime"], "kata")
        self.assertNotIn("image", body)
        self.assertNotIn("rootfs", body)

    def test_s3_rootfs_uses_top_level_runtime_only(self):
        rootfs = S3Config(
            endpoint="https://s3.example.com",
            bucket="rootfs",
            object="runtime.img",
        )
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(rootfs=rootfs, runtime="kata", detached=True)

        body = _FakeClient.created[-1]
        self.assertEqual(body["runtime"], "kata")
        self.assertEqual(body["rootfs"]["type"], "s3")
        self.assertNotIn("runtime", body["rootfs"])

    def test_image_and_rootfs_are_mutually_exclusive(self):
        rootfs = S3Config(
            endpoint="https://s3.example.com",
            bucket="rootfs",
            object="rootfs.img",
        )
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "mutually exclusive"):
                Sandbox(
                    image="ubuntu:24.04",
                    rootfs=rootfs,
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_schedule_timeout_minus_one_is_rejected_before_create(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "positive integer"):
                Sandbox(
                    image="ubuntu:22.04",
                    schedule_timeout=-1,
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_sandbox_constructor_keeps_backend_reverse_tunnel_boundary(self):
        signature = inspect.signature(Sandbox)
        self.assertEqual(signature.parameters["schedule_timeout"].default, 30)
        self.assertNotIn("reverse_tunnel", signature.parameters)
        self.assertIn("upstream", signature.parameters)
        self.assertIn("proxy_port", signature.parameters)
        self.assertIn("tunnel_connect_timeout", signature.parameters)

    def test_duplicate_forwarded_ports_are_rejected_before_create(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "duplicate"):
                Sandbox(
                    image="ubuntu:22.04",
                    port_forwardings=[8080, 8080],
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_port_forwarding_descriptors_remain_compatible(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                port_forwardings=[PortForwarding(8080), 9090],
                detached=True,
            )

        self.assertEqual(_FakeClient.created[-1]["ports"], ["8080", "9090"])

    def test_duplicate_forwarded_ports_across_descriptor_and_integer_are_rejected(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "duplicate"):
                Sandbox(
                    image="ubuntu:22.04",
                    port_forwardings=[PortForwarding(8080), 8080],
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_reverse_tunnel_upstream_is_validated_before_create(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "upstream"):
                Sandbox(
                    image="ubuntu:22.04",
                    upstream="",
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_core_arguments_are_validated_before_create(self):
        invalid_cases = [
            ({"image": ""}, ValueError, "image"),
            ({"image": "ubuntu", "rootfs": "bad"}, TypeError, "S3Config"),
            ({"image": "ubuntu", "env": {"A": 1}}, TypeError, "env"),
            ({"image": "ubuntu", "name": ""}, ValueError, "name"),
            ({"image": "ubuntu", "cwd": 1}, TypeError, "cwd"),
            ({"image": "ubuntu", "mounts": ["bad"]}, TypeError, "Mount"),
            ({"image": "ubuntu", "detached": 1}, TypeError, "detached"),
            ({"image": "ubuntu", "node_id": 1}, TypeError, "node_id"),
        ]
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for kwargs, error_type, message in invalid_cases:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaisesRegex(error_type, message):
                        Sandbox(**kwargs)
        self.assertEqual(_FakeClient.created, [])

    def test_reverse_tunnel_ports_cannot_conflict_with_forwarded_ports(self):
        with patch("yr_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "conflict"):
                Sandbox(
                    image="ubuntu:22.04",
                    port_forwardings=[8766],
                    upstream="127.0.0.1:9000",
                )
        self.assertEqual(_FakeClient.created, [])


if __name__ == "__main__":
    unittest.main()
