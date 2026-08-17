"""Wire-level tests for the streaming reverse-tunnel protocol."""

import struct
import unittest

from yr_sandbox.tunnel_protocol import (
    BINARY_MAGIC,
    BinaryEnvelope,
    BinaryKind,
    ProtocolError,
    hello_frame,
)

REQUEST_ID = "00112233-4455-6677-8899-aabbccddeeff"


class BinaryEnvelopeTests(unittest.TestCase):
    def test_request_chunk_has_stable_cross_language_layout(self):
        encoded = BinaryEnvelope(
            request_id=REQUEST_ID,
            kind=BinaryKind.HTTP_REQUEST_DATA,
            payload=b"payload",
        ).encode()

        self.assertEqual(encoded[:2], BINARY_MAGIC)
        self.assertEqual(encoded[2:5], bytes([1, 1, 16]))
        self.assertEqual(encoded[5:21].hex(), REQUEST_ID.replace("-", ""))
        self.assertEqual(encoded[21], 0)
        self.assertEqual(struct.unpack("!I", encoded[22:26])[0], 7)
        self.assertEqual(encoded[26:], b"payload")
        self.assertEqual(BinaryEnvelope.decode(encoded).payload, b"payload")

    def test_response_chunk_round_trips_end_flag(self):
        envelope = BinaryEnvelope(
            request_id=REQUEST_ID,
            kind=BinaryKind.HTTP_RESPONSE_DATA,
            payload=b"last",
            end_of_body=True,
        )
        self.assertEqual(BinaryEnvelope.decode(envelope.encode()), envelope)

    def test_rejects_malformed_and_oversized_envelopes(self):
        valid = BinaryEnvelope(
            request_id=REQUEST_ID,
            kind=BinaryKind.HTTP_REQUEST_DATA,
            payload=b"abcd",
        ).encode()
        cases = [
            valid[:25],
            b"NO" + valid[2:],
            valid[:3] + b"\xff" + valid[4:],
            valid[:4] + b"\x0f" + valid[5:],
            valid[:-1],
        ]
        for raw in cases:
            with self.subTest(raw=raw[:26].hex()):
                with self.assertRaises(ProtocolError):
                    BinaryEnvelope.decode(raw)
        with self.assertRaises(ProtocolError):
            BinaryEnvelope.decode(valid, max_payload=3)
        with self.assertRaises(ProtocolError):
            BinaryEnvelope(
                request_id=REQUEST_ID,
                kind=BinaryKind.HTTP_REQUEST_DATA,
                payload=b"abcd",
            ).encode(max_payload=3)


class HelloFrameTests(unittest.TestCase):
    def test_default_hello_advertises_v2_limits(self):
        self.assertEqual(
            hello_frame(),
            {
                "type": "hello",
                "protocol_version": 2,
                "max_stream_chunk": 65536,
                "max_inflight": 16,
                "stream_window_frames": 16,
                "max_body_size": 67108864,
            },
        )

    def test_hello_can_explicitly_advertise_v1_for_rollout_fallback(self):
        self.assertEqual(hello_frame(protocol_version=1)["protocol_version"], 1)


if __name__ == "__main__":
    unittest.main()
