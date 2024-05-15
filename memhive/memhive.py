import marshal
import os
import sys
import textwrap
import threading

import _xxsubinterpreters as subint

import memhive.core as core


class MemHive:

    def __init__(self):
        self._mem = core.MemHive()
        self._workers = []
        self._closed = False
        self._wait_add_worker = os.pipe()

    def _ensure_active(self):
        if self._closed:
            raise RuntimeError('MemHive is closed')

    def __getitem__(self, key):
        self._ensure_active()
        return self._mem[key]

    def __contains__(self, key):
        self._ensure_active()
        return key in self._mem

    def __setitem__(self, key, val):
        self._ensure_active()
        self._mem[key] = val

    def add_worker(self, *, setup=None, main=None):
        def runner(code):
            sub = subint.create(isolated=True)
            try:
                subint.run_string(sub, code)
            except Exception as ex:
                # XXX: serialize exceptions properly
                print('Unhandled error in a subinterpreter', type(ex), ex)
                raise
            finally:
                subint.destroy(sub)

        self._ensure_active()
        code = self._make_code(setup=setup, main=main)
        worker_thread = threading.Thread(target=runner, args=(code,))
        worker_thread.start()
        os.read(self._wait_add_worker[0], 1)
        self._workers.append(worker_thread)

    def broadcast(self, message):
        self._ensure_active()
        self._mem.broadcast(message)

    def push(self, message):
        self._ensure_active()
        self._mem.push(message)

    def listen(self):
        self._ensure_active()
        return self._mem.listen()

    def __enter__(self):
        self._ensure_active()
        return self

    def __exit__(self, *e):
        self.close()

    def process_refs(self):
        self._mem.process_refs()

    def close(self):
        if self._closed:
            return
        self._closed = True

        self._mem.close_subs_queue()
        for w in self._workers:
            w.join()
        self._workers.clear()
        self._mem.close()

    def _make_code(self, *, setup, main):
        hive_id = repr(id(self._mem))
        sys_path = repr(sys.path)

        has_setup = repr(False)
        if setup:
            has_setup = repr(True)
            setup_name = repr(setup.__name__)
            setup_code = repr(marshal.dumps(setup.__code__))
        else:
            setup_name = repr(None)
            setup_code = repr(None)

        main_name = repr(main.__name__)
        main_code = repr(marshal.dumps(main.__code__))
        main_defs = repr(marshal.dumps(main.__defaults__))

        return textwrap.dedent(f'''\
            import marshal as __marshal
            import os as __os
            import sys as __sys
            import types as __types

            import memhive.core as __core

            if hasattr(__core, 'enable_object_tracking'):
                __core.enable_object_tracking()

            if {has_setup}:
                __setup = __types.FunctionType(
                    __marshal.loads({setup_code}), globals(), {setup_name}
                )

            __main = __types.FunctionType(
                __marshal.loads({main_code}),
                globals(),
                {main_name},
                __marshal.loads({main_defs})
            )

            __sys.path = {sys_path}
            __sub = __core.MemHiveSub({hive_id})
            try:
                if {has_setup}:
                    __setup(__sub)
                __os.write({self._wait_add_worker[1]}, b'1')
                __main(__sub)
            except __core.ClosedQueueError:
                pass
            except Exception as ex:
                # import traceback
                # traceback.print_exception(ex)
                raise
            finally:
                __sub.close()
                del __sub
        ''')
