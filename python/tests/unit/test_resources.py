import json
import unittest
from unittest.mock import patch

import httpx
from yr_sandbox._resources import resources
from yr_sandbox._transport import SandboxClient


class _ResourceClient:
    closed = False

    def resources(self):
        return {
            "resource": {
                "fragment": {
                    "node-1": {
                        "id": "node-1",
                        "status": 0,
                        "capacity": {
                            "resources": {
                                "CPU": {"scalar": {"value": 4000}},
                                "Memory": {"scalar": {"value": 8192}},
                            }
                        },
                        "allocatable": {
                            "resources": {
                                "CPU": {"scalar": {"value": 3000}},
                                "Memory": {"scalar": {"value": 4096}},
                            }
                        },
                        "nodeLabels": {
                            "ARCH": {"items": {"arm64": 1}},
                        },
                    }
                }
            }
        }

    def close(self):
        type(self).closed = True


class ResourceTests(unittest.TestCase):
    def test_resources_maps_existing_scheduler_json_response(self):
        _ResourceClient.closed = False
        with patch("yr_sandbox._resources.SandboxClient", _ResourceClient):
            nodes = resources()
        self.assertTrue(_ResourceClient.closed)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, "node-1")
        self.assertEqual(nodes[0].status, 0)
        self.assertEqual(nodes[0].capacity["CPU"], 4000.0)
        self.assertEqual(nodes[0].allocatable["Memory"], 4096.0)
        self.assertEqual(nodes[0].labels["ARCH"], "arm64")

    def test_client_uses_existing_global_scheduler_json_route(self):
        seen = {}

        def handle(request):
            seen["path"] = request.url.path
            seen["type"] = request.headers.get("Type")
            return httpx.Response(
                200,
                content=json.dumps({"resource": {"fragment": {}}}),
                headers={"Content-Type": "application/json"},
            )

        client = SandboxClient.__new__(SandboxClient)
        client._origin = "http://frontend"
        client._http = httpx.Client(transport=httpx.MockTransport(handle))
        try:
            payload = client.resources()
        finally:
            client.close()

        self.assertEqual(seen["path"], "/global-scheduler/resources")
        self.assertEqual(seen["type"], "json")
        self.assertIn("resource", payload)


if __name__ == "__main__":
    unittest.main()
