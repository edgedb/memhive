import itertools
import marshal
import sys
import textwrap
import threading

import _xxsubinterpreters as subint

import memhive.core as core


class CoreMemHive(core.MemHive):

    def __init__(self):
        super().__init__()

        self._workers = []

        # Using `itertools.count()` here as opposed to `count += 1`,
        # as the former is thread-safe. We also don't want sub ids
        # to start at 0 (that's reserved for the main interpreter
        # and I'd like to avoid any possible confusion)
        # and we don't want them to appear to be mapped to real
        # subinterpreter IDs (they can be out of sync anyway),
        # hence a random offset.
        self._sub_counter = itertools.count(42).__next__


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

        sub_id = self._sub_counter()
        code = self._make_code(setup=setup, main=main, sub_id=sub_id)
        worker_thread = threading.Thread(target=runner, args=(code,))
        worker_thread.start()
        self._workers.append(worker_thread)
        return sub_id

    def _make_code(self, *, setup, main, sub_id):
        hive_id = repr(id(self))
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
            import traceback as __traceback
            import types as __types

            import memhive.core as __core

            if hasattr(__core, 'enable_object_tracking'):
                __core.enable_object_tracking()

            __sys.path = {sys_path}

            try:
                __sub = __core.MemHiveSub({hive_id}, {sub_id})
            except BaseException as ex:
                nex = RuntimeError('COULD NOT INSTANTIATE SUB')
                nex.__cause__ = ex
                __traceback.print_exception(nex, file=__sys.stderr)
                raise nex

            try:
                if {has_setup}:
                    __setup = __types.FunctionType(
                        __marshal.loads({setup_code}), globals(), {setup_name}
                    )
            except Exception as ex:
                __sub.report_error(
                    'RuntimeError',
                    'failed to unserialize the "setup" worker function',
                    ex
                )
                __sub.close()
                __sub = None

            if __sub is not None:
                try:
                    __main = __types.FunctionType(
                        __marshal.loads({main_code}),
                        globals(),
                        {main_name},
                        __marshal.loads({main_defs})
                    )
                except Exception as ex:
                    __sub.report_error(
                        'RuntimeError',
                        'failed to unserialize the "main" worker function',
                        ex
                    )
                    __sub.close()
                    __sub == None

            if __sub is not None and {has_setup}:
                try:
                    __setup(__sub)
                except Exception as ex:
                    __sub.report_error(
                        'RuntimeError',
                        'unhandled error during the "setup" worker call',
                        ex
                    )
                    __sub.close()
                    __sub = None

            if __sub is not None:
                __sub.report_start()
                try:
                    __main(__sub)
                except Exception as ex:
                    __sub.report_error(
                        'RuntimeError',
                        'unhandled error during the "main" worker call',
                        ex
                    )
                except __core.ClosedQueueError:
                    __sub.report_close() # XXX do this properly
                else:
                    __sub.report_close()
                finally:
                    __sub.close()
                    del __sub
        ''')

    def join_worker_threads(self):
        for w in self._workers:
            w.join()
        self._workers.clear()


class MemHive:

    def __init__(self):
        self._mem = CoreMemHive()
        self._closed = False

    def _ensure_active(self):
        if self._closed:
            raise RuntimeError('MemHive is closed')

    def add_worker(self, *, setup=None, main=None):
        self._ensure_active()
        self._mem.add_worker(setup=setup, main=main)

    def __getitem__(self, key):
        self._ensure_active()
        return self._mem[key]

    def __contains__(self, key):
        self._ensure_active()
        return key in self._mem

    def __setitem__(self, key, val):
        self._ensure_active()
        self._mem[key] = val

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
        self._mem.join_worker_threads()
        self._mem.close()
