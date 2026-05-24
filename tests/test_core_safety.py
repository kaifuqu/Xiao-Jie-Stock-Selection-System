import os
import tempfile
import unittest
from unittest import mock

from core.file_utils import atomic_json_update, is_safe_pickle_path


class TestCoreSafety(unittest.TestCase):
    def test_safe_pickle_path_only_allows_data_dir(self):
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
        inside = os.path.join(base, "sample.pkl")
        outside = os.path.abspath(os.path.join(tempfile.gettempdir(), "sample.pkl"))
        self.assertTrue(is_safe_pickle_path(inside))
        self.assertFalse(is_safe_pickle_path(outside))

    def test_atomic_json_update_recovers_stale_lock(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "x.json")
            lock_path = path + ".lock"
            with open(lock_path, "w", encoding="utf-8") as f:
                f.write("1")

            times = iter([1000, 2000, 2001, 2002, 2003, 2004, 2005])

            def fake_time():
                return next(times)

            with mock.patch("core.file_utils.time.time", side_effect=fake_time), \
                 mock.patch("core.file_utils.os.path.getmtime", return_value=0), \
                 mock.patch("core.file_utils.shutil.copy2"):
                def updater(data):
                    data["ok"] = True
                atomic_json_update(path, updater, timeout=1)
            self.assertTrue(os.path.isfile(path))


if __name__ == "__main__":
    unittest.main()
