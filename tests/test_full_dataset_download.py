import gzip
import tempfile
import unittest
from pathlib import Path

from scripts.download_full_dataset import download_source


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


if __name__ == "__main__":
    unittest.main()
