import pytest
"""Put app/src on sys.path so the app modules import without a database."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(autouse=True)
def _no_shared_reads_between_tests():
    """The app shares monitoring reads for a few seconds (main._shared); one test's answers must not
    reach the next."""
    import sys
    main = sys.modules.get("main")
    if main is not None:
        main._shared_reads.clear()
    yield
    if sys.modules.get("main") is not None:
        sys.modules["main"]._shared_reads.clear()
