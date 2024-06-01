import builtins
import dataclasses
import itertools
import marshal
import sys
import textwrap
import threading

if sys.version_info[:2] >= (3, 13):
    Py_3_13 = True
    import _interpreters as subint
else:
    Py_3_13 = False
    import _xxsubinterpreters as subint

from . import core
from . import errors


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


    def add_worker(self, *, main=None):
        def runner(code):
            if Py_3_13:
                sub = subint.create('isolated')
            else:
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
        code = self._make_code(main=main, sub_id=sub_id)
        worker_thread = threading.Thread(target=runner, args=(code,))
        worker_thread.start()
        self._workers.append(worker_thread)
        return sub_id

    def _make_code(self, *, main, sub_id):
        hive_id = repr(id(self))
        sys_path = repr(sys.path)

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
                        'failed to unserialize the "main()" worker function',
                        ex
                    )
                    __sub.close()
                    __sub = None

            if __sub is not None:
                __sub.report_start()
                try:
                    __main(__sub)
                except __core.ClosedQueueError:
                    __sub.report_close() # XXX do this properly
                except Exception as ex:
                    __sub.report_error(
                        'RuntimeError',
                        'unhandled exception during the "main()" worker call',
                        ex
                    )
                except BaseException as ex:
                    __sub.report_error(
                        'RuntimeError',
                        'unhandled base exception during the "main()"'
                        'worker call',
                        ex
                    )
                    raise
                else:
                    __sub.report_close()
                finally:
                    __sub.close()
                    __sub = None

        ''')

    def join_worker_threads(self):
        for w in self._workers:
            w.join()
        self._workers.clear()


@dataclasses.dataclass
class WorkerStatus:

    wid: int
    error: BaseException | None = None
    ready: threading.Event = dataclasses.field(
        default_factory=threading.Event)
    completed: threading.Event = dataclasses.field(
        default_factory=threading.Event)


class MemHive:

    def __init__(self):
        self._mem = CoreMemHive()
        self._inside = False
        self._closed = False

        self._workers = {}
        self._health_listener = None
        self._health_listener_started = threading.Event()

    def _ensure_active(self):
        if self._closed:
            raise RuntimeError('MemHive is closed')
        if not self._inside:
            raise RuntimeError("MemHive hasn't entered its context")

    def add_worker(self, *, main=None):
        self._ensure_active()
        wid = self._mem.add_worker(main=main)
        self._workers[wid] = WorkerStatus(wid=wid)
        self._workers[wid].ready.wait()

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
        self._inside = True
        self._ensure_active()

        self._health_listener = threading.Thread(
            target=self._listen_for_health_updates)
        self._health_listener.start()
        self._health_listener_started.wait()

        return self

    def __exit__(self, *e):
        self._inside = False
        self.close()

    def _listen_for_health_updates(self):
        self._health_listener_started.set()

        while True:
            try:
                msg = self._mem.listen_subs_health()
            except core.ClosedQueueError:
                return

            match msg:
                case ("ERROR", wid, err_name, err_msg, exc):
                    err_type = getattr(builtins, err_name)
                    new_err = err_type(err_msg)
                    new_err.__cause__ = exc
                    self._workers[wid].error = new_err
                    self._workers[wid].completed.set()

                case ("CLOSE", wid):
                    self._workers[wid].completed.set()

                case ("START", wid):
                    self._workers[wid].ready.set()

                case _:
                    raise RuntimeError(f'unknown message {_}')

    def process_refs(self):
        self._mem.process_refs()

    def close(self):
        if self._closed:
            return

        try:
            ers = []
            for wrk in self._workers.values():
                wrk.completed.wait()
                if wrk.error is not None:
                    ers.append(wrk.error)

            if ers:
                raise errors.MemhiveGroupError(
                    'one or many subinterpreter workers crashed with an error',
                    ers
                )

        finally:
            self._closed = True

            self._mem.close_subs_health_queue()
            self._health_listener.join()
            self._health_listener = None

            self._mem.close_subs_queue()
            self._mem.join_worker_threads()
            self._mem.close()
