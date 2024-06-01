import io
import unittest
import tempfile
import traceback

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

        try:
            with memhive.MemHive() as m:
                m.add_worker(main=worker)
        except memhive.MemhiveGroupError as ex:
            file = io.StringIO()
            traceback.print_exception(ex, file=file)
            render = file.getvalue()

            self.assertIn(
                'subinterpreter workers crashed',
                render
            )
            self.assertIn(
                '__subinterpreter__.ZeroDivisionError: division by zero',
                render
            )
            self.assertIn(
                'unhandled exception during the "main()" worker call',
                render
            )
        else:
            self.fail('exception was not propagated')
