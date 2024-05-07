try:
    import stackless
except ImportError:
    pass
else:
    # The only reason for this is us abusing Py_TPFLAGS reserved
    # for Stackless. That's most likely a temporary limitation.
    raise RuntimeError('memhive is not compatible with Stackless.')


import sys
import textwrap
import threading
import marshal
import _xxsubinterpreters as subint

from ._memhive import _MemHive
from ._memhive import Map
from ._memhive import _MemQueue
from ._memhive import ClosedQueueError


class _Mem(_MemHive):
    pass


class MemHive:

    def __init__(self):
        self._ctx = None

    def __enter__(self):
        if self._ctx is not None:
            raise RuntimeError
        self._ctx = MemHiveContext(self)
        return self._ctx

    def __exit__(self, *e):
        self._ctx._join()


class MemHiveContext:

    def __init__(self, hive):
        self._hive = hive
        self._mem = _Mem()

    def __getitem__(self, key):
        return self._mem[key]

    def __setitem__(self, key, val):
        self._mem[key] = val

    def run(self, code: str):
        def runner(code):
            code = textwrap.dedent('''\
            import sys
            sys.path = ''' + repr(sys.path) + '''

            from memhive._memhive import _MemHiveProxy
            mem = _MemHiveProxy(''' + repr(id(self._mem)) + ''')
            \n''') + code

            sub = subint.create(isolated=True)
            try:
                subint.run_string(sub, code)
            except Exception as ex:
                print('!!-1', type(ex), '|', ex)
                raise
            finally:
                subint.destroy(sub)

        thread = threading.Thread(target=runner, args=(code,))
        try:
            thread.start()
        except Exception as ex:
            print('!!-2', type(ex), '|', ex)
            raise
        finally:
            thread.join()

    def _join(self):
        pass


class Executor:
    def __init__(self, *, nworkers: int=4):
        self._mem = _MemHive()
        self._nworkers = nworkers
        self._workers = []
        for _ in range(self._nworkers):
            self._make_sub()

    def _make_sub(self):
        def runner():
            code = textwrap.dedent('''\
            import sys
            import marshal, types
            import threading
            sys.path = ''' + repr(sys.path) + '''

            from memhive._memhive import ClosedQueueError
            from memhive._memhive import _MemHiveProxy
            mem = _MemHiveProxy(''' + repr(id(self._mem)) + ''')

            bin = []
            try:
                while True:
                    p = mem.get_proxied()
                    idx, func_name, func_code, args = p

                    func_code = marshal.loads(func_code)
                    func = types.FunctionType(func_code, globals(), func_name)

                    ret = (idx, func(*args))
                    bin.append(ret) # xxx
                    mem.put_borrowed(ret)
                    bin.append(p) # xxx
            except ClosedQueueError:
                pass
            finally:
                # xxx
                bin.clear()
                import gc
                gc.collect()
                gc.collect()
                gc.collect()
            \n''')

            sub = subint.create(isolated=True)
            try:
                subint.run_string(sub, code)
            except Exception as ex:
                print('Unhandled error in a subinterpreter', type(ex), ex)
                raise
            finally:
                subint.destroy(sub)

        thread = threading.Thread(target=runner)
        thread.start()
        self._workers.append(thread)

    def map(self, argss, func):
        payloads = []

        res = {}
        for i, args in enumerate(argss):
            p = (
                i,
                func.__name__,
                marshal.dumps(func.__code__),
                args
            )
            res[i] = None
            self._mem.put_borrowed(p)
            payloads.append(p)

        try:
            for _ in range(len(argss)):
                ret = self._mem.get_proxied()
                res[ret[0]] = ret[1]

            return list(res.values())

        except KeyboardInterrupt:
            try:
                self.close()
            finally:
                raise

    def close(self):
        self._mem.close_subs_intake()
        for t in self._workers:
            t.join()
        self._workers = []
