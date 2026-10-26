import gzip
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.download_full_dataset import download_source


class FakeResponse(io.BytesIO):
    def __init__(self, body, status, headers):
        super().__init__(body)
        self.status = status
        self.headers = headers

    def getcode(self):
        return self.status


def truncating_urlopen(source_bytes, cut_at, ranges):
    """Serve source_bytes, ending the first response early at cut_at."""

    def fake_urlopen(request, timeout=None):
        range_header = request.get_header("Range")
        ranges.append(range_header)
        total = len(source_bytes)
        if range_header is None:
            return FakeResponse(
                source_bytes[:cut_at], 200, {"Content-Length": str(total)}
            )
        start = int(range_header.removeprefix("bytes=").rstrip("-"))
        return FakeResponse(
            source_bytes[start:],
            206,
            {"Content-Range": f"bytes {start}-{total - 1}/{total}"},
        )

    return fake_urlopen


class FullDatasetDownloadTests(unittest.TestCase):
    def test_download_is_chunked_complete_and_repeatable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_path = root / "source.jsonl"
            source_bytes = (
                b'{"id":1,"text":"alpha"}\n'
                b'{"id":2,"text":"beta"}\n'
                b'{"id":3,"text":"gamma"}\n'
                b'{"id":4,"text":"delta"}\n'
            )
            source_path.write_bytes(source_bytes)
            output = root / "output"
            source = {
                "name": "reviews",
                "url": source_path.as_uri(),
            }

            first = download_source(source, output, chunk_bytes=30)
            second = download_source(source, output, chunk_bytes=30)

            self.assertTrue(first["completed"])
            self.assertEqual(first["line_count"], 4)
            self.assertGreater(len(first["parts"]), 1)
            self.assertEqual(first, second)

            restored = b"".join(
                gzip.open(path, "rb").read()
                for path in sorted((output / "reviews").glob("part-*.jsonl.gz"))
            )
            self.assertEqual(restored, source_bytes)

    def test_truncated_stream_never_finalizes_a_partial_line(self):
        source_bytes = (
            b'{"id":1,"text":"alpha"}\n'
            b'{"id":2,"text":"beta"}\n'
            b'{"id":3,"text":"gamma"}\n'
            b'{"id":4,"text":"delta"}'
        )
        second_line_start = source_bytes.index(b'{"id":2')
        cut_at = second_line_start + 10
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            source = {"name": "reviews", "url": "https://example.test/x.jsonl"}
            ranges = []
            fake = truncating_urlopen(source_bytes, cut_at, ranges)

            with mock.patch("scripts.download_full_dataset.urlopen", fake):
                with self.assertRaisesRegex(RuntimeError, "mid-line"):
                    download_source(source, output, chunk_bytes=1024)

                source_dir = output / "reviews"
                parts = sorted(source_dir.glob("part-*.jsonl.gz"))
                self.assertEqual(len(parts), 1)
                self.assertEqual(
                    gzip.open(parts[0], "rb").read(),
                    source_bytes[:second_line_start],
                )
                self.assertEqual(list(source_dir.glob("*.tmp")), [])

                state = download_source(source, output, chunk_bytes=1024)

            self.assertEqual(ranges, [None, f"bytes={second_line_start}-"])
            self.assertTrue(state["completed"])
            self.assertEqual(state["line_count"], 4)
            self.assertEqual(state["source_bytes"], len(source_bytes))
            self.assertEqual([part["line_count"] for part in state["parts"]], [1, 3])
            restored = b"".join(
                gzip.open(path, "rb").read()
                for path in sorted(source_dir.glob("part-*.jsonl.gz"))
            )
            self.assertEqual(restored, source_bytes)
            for raw in restored.splitlines():
                self.assertTrue(raw.startswith(b"{") and raw.endswith(b"}"))


if __name__ == "__main__":
    unittest.main()
