import unittest
import tempfile

import memhive


class BasicsTest(unittest.TestCase):

    def test_sync_basic_run(self):

        def worker(sub):
            with open(sub['file'], 'w') as f:
                f.write('hello!')

        with tempfile.NamedTemporaryFile() as tmp:
            with memhive.MemHive() as m:

                m['file'] = tmp.name
                m.add_worker(main=worker)

            with open(tmp.name, 'r') as f:
                self.assertEqual(f.read(), 'hello!')

    def test_sync_main_cannot_die_earlier_than_sub(self):
        def worker(sub):
            import time
            time.sleep(0.1)

        with memhive.MemHive() as m:
            m.add_worker(main=worker)

    def test_sync_sub_errors_out(self):
        def worker(sub):
            1/0

        with memhive.MemHive() as m:
            m.add_worker(main=worker)
