import os
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT = Path(__file__).resolve().parent.parent
RUNNER = PROJECT / "scripts" / "run_batch_pipeline.sh"


class BatchRunnerTests(unittest.TestCase):
    def make_fake_submit(self, directory):
        script = directory / "fake-spark-submit.sh"
        script.write_text(
            """#!/bin/sh
count=0
if [ -f "$FAKE_COUNT_FILE" ]; then
  count=$(cat "$FAKE_COUNT_FILE")
fi
count=$((count + 1))
printf '%s' "$count" > "$FAKE_COUNT_FILE"
echo "$1" >> "$FAKE_INVOCATION_FILE"
case "$FAKE_MODE" in
  fail_once)
    if [ "$count" -eq 1 ]; then exit 17; fi
    ;;
  always_fail)
    exit 9
    ;;
esac
""",
            encoding="utf-8",
        )
        script.chmod(0o755)
        return script

    def run_pipeline(self, mode, *stages, max_attempts="2"):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        fake_submit = self.make_fake_submit(directory)
        count_file = directory / "count.txt"
        invocation_file = directory / "invocations.txt"
        environment = os.environ.copy()
        environment.update({
            "SPARK_SUBMIT_BIN": str(fake_submit),
            "PIPELINE_MAX_ATTEMPTS": max_attempts,
            "PIPELINE_RETRY_DELAY_SECONDS": "0",
            "FAKE_MODE": mode,
            "FAKE_COUNT_FILE": str(count_file),
            "FAKE_INVOCATION_FILE": str(invocation_file),
        })
        result = subprocess.run(
            ["/bin/sh", str(RUNNER), *stages],
            capture_output=True,
            check=False,
            env=environment,
            text=True,
        )
        invocations = (
            invocation_file.read_text(encoding="utf-8").splitlines()
            if invocation_file.exists()
            else []
        )
        return result, invocations

    def test_retries_current_stage_then_continues(self):
        result, invocations = self.run_pipeline(
            "fail_once",
            "bronze",
            "silver",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            [Path(path).name for path in invocations],
            [
                "load_product_reviews_bronze.py",
                "load_product_reviews_bronze.py",
                "build_product_reviews_silver.py",
            ],
        )
        self.assertIn("retrying in 0s", result.stderr)

    def test_stops_before_downstream_stage_when_retries_are_exhausted(self):
        result, invocations = self.run_pipeline(
            "always_fail",
            "bronze",
            "silver",
        )
        self.assertEqual(result.returncode, 9)
        self.assertEqual(len(invocations), 2)
        self.assertTrue(all("bronze" in path for path in invocations))
        self.assertIn("Failed bronze after 2 attempt(s)", result.stderr)

    def test_rejects_invalid_attempt_configuration(self):
        result, invocations = self.run_pipeline(
            "success",
            "bronze",
            max_attempts="0",
        )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(invocations, [])
        self.assertIn("must be at least 1", result.stderr)


if __name__ == "__main__":
    unittest.main()