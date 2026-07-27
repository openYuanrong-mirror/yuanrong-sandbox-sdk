import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from yr_sandbox._pty_transport import _build_pty_uri
from yr_sandbox.pty import Pty, _use_tls


class PtyTests(unittest.TestCase):
    def test_uri_preserves_command_protocol(self):
        uri = _build_pty_uri(
            server="frontend.example:443",
            use_tls=True,
            instance_id="sandbox/1",
            token="secret",
            command=["/bin/bash", "-lc", "echo hello"],
            rows=24,
            cols=80,
        )
        parsed = urlparse(uri)
        query = parse_qs(parsed.query)
        self.assertEqual(parsed.scheme, "wss")
        self.assertEqual(parsed.path, "/terminal/ws")
        self.assertEqual(query["protocol"], ["sandbox.pty.v1"])
        self.assertEqual(query["command"], ["/bin/bash", "-lc", "echo hello"])

    def test_pty_uses_yr_process_configuration(self):
        pty = Pty("sandbox-1")
        self.assertEqual(pty._instance_id, "sandbox-1")

    def test_explicit_gateway_defaults_to_plain_websocket(self):
        with patch.dict(
            "os.environ",
            {"YR_GATEWAY_ADDRESS": "gateway:8080"},
            clear=True,
        ):
            self.assertFalse(_use_tls())


if __name__ == "__main__":
    unittest.main()
