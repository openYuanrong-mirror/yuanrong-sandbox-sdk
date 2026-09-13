import unittest

import yr_sandbox


class PublicApiTests(unittest.TestCase):
    def test_backend_owned_reverse_tunnel_type_is_not_public(self):
        self.assertNotIn("HttpReverseTunnel", yr_sandbox.__all__)
        self.assertFalse(hasattr(yr_sandbox, "HttpReverseTunnel"))


if __name__ == "__main__":
    unittest.main()
