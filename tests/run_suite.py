"""Run the project test suite through the container's Spark runtime."""

import sys
import unittest


def main() -> int:
    suite = unittest.defaultTestLoader.discover(
        "/opt/project/tests", pattern="test_*.py"
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
