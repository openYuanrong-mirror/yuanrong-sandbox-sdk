import base64
import dataclasses
import json
import unittest
from unittest.mock import patch

import yr_sandbox
from yr_sandbox import Sandbox
from yr_sandbox._transport import SandboxClient, SandboxError


class ReusableSnapshotTests(unittest.TestCase):
    def test_snapshot_info_is_frozen_public_value(self):
        info = yr_sandbox.SnapshotInfo(snapshot_id="snap-1", names=("base",))
        self.assertEqual(info.snapshot_id, "snap-1")
        self.assertEqual(info.names, ("base",))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            info.snapshot_id = "changed"

    def test_create_snapshot_returns_snapshot_info(self):
        sandbox = object.__new__(Sandbox)
        sandbox._sid = "default-source"
        sandbox._closed = False

        class Client:
            def create_snapshot(self, sandbox_id, name=None):
                self.request = (sandbox_id, name)
                return {"snapshotId": "snap-1", "names": ["base"]}

        sandbox._client = Client()
        result = sandbox.create_snapshot(name="base")
        self.assertEqual(result, yr_sandbox.SnapshotInfo("snap-1", ("base",)))
        self.assertEqual(sandbox._client.request, ("default-source", "base"))

    def test_create_from_snapshot_adds_only_snapshot_id(self):
        captured = {}

        class Client:
            def __init__(self):
                pass

            def create_info(self, body):
                captured.update(body)
                return {"sandboxId": "default-clone"}

            def close(self):
                pass

        snapshot = yr_sandbox.SnapshotInfo("snap-ready", ("base",))
        with patch("yr_sandbox.sandbox_api.SandboxClient", Client):
            clone = Sandbox.create(snapshot, name="clone")
        self.assertEqual(clone.id, "default-clone")
        self.assertEqual(captured["snapshotId"], "snap-ready")

    def test_snapshot_get_list_delete_delegate_to_transport(self):
        class Client:
            def get_snapshot(self, snapshot_id):
                return {"snapshotId": snapshot_id, "names": ["one"]}

            def list_snapshots(self, name=None, page_token=None, page_size=None):
                self.page = (name, page_token, page_size)
                return {
                    "items": [{"snapshotId": "snap-1", "names": []}],
                    "nextPageToken": "next",
                }

            def delete_snapshot(self, snapshot_id):
                self.deleted = snapshot_id

        client = Client()
        self.assertEqual(
            Sandbox._get_snapshot(client, "snap-1"),
            yr_sandbox.SnapshotInfo("snap-1", ("one",)),
        )
        infos, token = Sandbox._list_snapshots(
            client,
            name="base",
            page_token="p",
            page_size=10,
        )
        self.assertEqual(infos, [yr_sandbox.SnapshotInfo("snap-1", ())])
        self.assertEqual(token, "next")
        self.assertEqual(client.page, ("base", "p", 10))
        Sandbox._delete_snapshot(client, "snap-1")
        self.assertEqual(client.deleted, "snap-1")

    def test_snapshot_resource_arguments_are_validated_before_transport(self):
        with self.assertRaisesRegex(ValueError, "snapshot_id"):
            Sandbox.get_snapshot(" ")
        with self.assertRaisesRegex(ValueError, "snapshot_id"):
            Sandbox.delete_snapshot("")
        with self.assertRaisesRegex(ValueError, "page_size"):
            Sandbox.list_snapshots(page_size=0)
        with self.assertRaisesRegex(ValueError, "name"):
            Sandbox.list_snapshots(name=" ")

    def test_transport_uses_snapshot_routes_and_stable_request_id(self):
        class Response:
            def __init__(self, payload, status_code=200):
                self.status_code = status_code
                self.text = "gateway unavailable" if status_code >= 400 else ""
                encoded = base64.b64encode(json.dumps(payload).encode()).decode()
                self._payload = {"code": 200, "data": encoded}

            def json(self):
                return self._payload

        class HTTP:
            def __init__(self):
                self.calls = []

            def post(self, url, **kwargs):
                self.calls.append(("POST", url, kwargs))
                request_id = kwargs["headers"]["X-YR-Request-ID"]
                return Response({"snapshotId": f"snap-{request_id[-8:]}", "names": []})

            def get(self, url, **kwargs):
                self.calls.append(("GET", url, kwargs))
                if url.endswith("/snapshots"):
                    return Response({"items": [], "nextPageToken": ""})
                return Response({"snapshotId": "snap-1", "names": []})

            def delete(self, url, **kwargs):
                self.calls.append(("DELETE", url, kwargs))
                return Response({"status": "deleted"})

        client = object.__new__(SandboxClient)
        client._base = "https://frontend/api/sandbox/v1"
        client._http = HTTP()

        created = client.create_snapshot("default-source", name="base")
        client.get_snapshot("snap-1")
        client.list_snapshots(name="base", page_token="next", page_size=10)
        client.delete_snapshot("snap-1")

        self.assertTrue(created["snapshotId"].startswith("snap-"))
        self.assertEqual(
            [call[0] for call in client._http.calls],
            ["POST", "GET", "GET", "DELETE"],
        )
        self.assertEqual(
            client._http.calls[0][1],
            "https://frontend/api/sandbox/v1/sandboxes/default-source/snapshots",
        )
        self.assertEqual(client._http.calls[0][2]["json"], {"name": "base"})
        self.assertEqual(
            client._http.calls[2][2]["params"],
            {"name": "base", "pageToken": "next", "pageSize": 10},
        )
        self.assertTrue(
            client._http.calls[3][2]["headers"]["X-YR-Request-ID"].startswith(
                "delete-snapshot-"
            )
        )

    def test_transport_accepts_snapshot_id_distinct_from_request_id(self):
        class Response:
            status_code = 200
            text = ""

            def json(self):
                payload = {"snapshotId": "snap-deterministic-id", "names": ["base"]}
                encoded = base64.b64encode(json.dumps(payload).encode()).decode()
                return {"code": 200, "data": encoded}

        class HTTP:
            def post(self, url, **kwargs):
                del url, kwargs
                return Response()

        client = object.__new__(SandboxClient)
        client._base = "https://frontend/api/sandbox/v1"
        client._http = HTTP()

        self.assertEqual(
            client.create_snapshot("default-source", name="base")["snapshotId"],
            "snap-deterministic-id",
        )

    def test_create_snapshot_does_not_blindly_retry_an_uncertain_gateway_result(self):
        class Response:
            status_code = 503
            text = "gateway unavailable"

        class HTTP:
            def __init__(self):
                self.calls = []

            def post(self, url, **kwargs):
                self.calls.append((url, kwargs))
                return Response()

        client = object.__new__(SandboxClient)
        client._base = "https://frontend/api/sandbox/v1"
        client._http = HTTP()

        with patch("yr_sandbox._transport.time.sleep") as sleep:
            with self.assertRaisesRegex(SandboxError, "HTTP 503"):
                client.create_snapshot("default-source", name="base")

        self.assertEqual(len(client._http.calls), 1)
        sleep.assert_not_called()
