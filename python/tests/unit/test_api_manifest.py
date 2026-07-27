import json
import unittest
from pathlib import Path

import yr_sandbox


class ApiManifestTests(unittest.TestCase):
    def test_public_api_manifest_symbols_match(self):
        manifest_path = (
            Path(__file__).resolve().parents[3]
            / "testdata/akernel_compat/api-manifest.json"
        )
        with manifest_path.open("r", encoding="utf-8") as fp:
            manifest = json.load(fp)

        manifest_exports = set(manifest["exports"])
        runtime_symbols = set(yr_sandbox.__all__)
        self.assertEqual(manifest_exports, runtime_symbols)

    def test_backend_owned_reverse_tunnel_type_is_not_public(self):
        self.assertNotIn("HttpReverseTunnel", yr_sandbox.__all__)
        self.assertFalse(hasattr(yr_sandbox, "HttpReverseTunnel"))


if __name__ == "__main__":
    unittest.main()
