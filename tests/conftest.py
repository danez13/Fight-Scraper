import pytest
import os
import glob

@pytest.fixture(autouse=True)
def cleanup_temp_files():
    yield
    # After each test, close and remove temp files
    for tmp_file in glob.glob("./*_progress_*.csv"):
        try:
            # If file is still open somewhere, closing it might help
            os.remove(tmp_file)
        except PermissionError:
            # Try to close file handles forcibly if possible or just skip
            pass
        except Exception:
            pass
