import marshal
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

    def add_worker(self, func):
        # XXX: add_worker is racy and returns *before* the MemHiveSub
        # object is constructed. That shouldn't be the case.
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
        code = _make_code(self, func)
        worker_thread = threading.Thread(target=runner, args=(code,))
        worker_thread.start()
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

    def close(self):
        if self._closed:
            return
        self._closed = True

        self._mem.close_subs_queue()
        for w in self._workers:
            w.join()
        self._workers.clear()
        self._mem.close()


def _make_code(memhive, func):
    hive_id = repr(id(memhive._mem))
    sys_path = repr(sys.path)
    func_name = repr(func.__name__)
    func_code = repr(marshal.dumps(func.__code__))

    return textwrap.dedent(f'''\
        import marshal as __marshal
        import sys as __sys
        import types as __types

        import memhive.core as __core

        __func = __types.FunctionType(
            __marshal.loads({func_code}), globals(), {func_name}
        )

        __sys.path = {sys_path}
        __sub = None
        try:
            __sub = __core.MemHiveSub({hive_id})
            __func(__sub)
        except __core.ClosedQueueError:
            pass
        finally:
            if __sub:
                __sub.close()
                del __sub
    ''')
