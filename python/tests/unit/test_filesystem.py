import unittest

from yr_sandbox.filesystem import Filesystem


class _Client:
    def __init__(self):
        self.calls = 0

    def download_bytes_direct(self, *_args, **_kwargs):
        self.calls += 1
        return b"hello"


class FilesystemTests(unittest.TestCase):
    def test_read_rejects_unknown_format_before_network(self):
        client = _Client()
        filesystem = Filesystem(client, "sandbox-1")
        with self.assertRaisesRegex(ValueError, "text.*bytes"):
            filesystem.read("/tmp/data", format="json")
        self.assertEqual(client.calls, 0)

    def test_read_preserves_text_and_bytes_boundaries(self):
        client = _Client()
        filesystem = Filesystem(client, "sandbox-1")
        self.assertEqual(filesystem.read("/tmp/data"), "hello")
        self.assertEqual(filesystem.read("/tmp/data", format="bytes"), b"hello")


if __name__ == "__main__":
    unittest.main()
