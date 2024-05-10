try:
    import stackless
except ImportError:
    pass
else:
    # The only reason for this is us abusing Py_TPFLAGS reserved
    # for Stackless. That's most likely a temporary limitation.
    raise RuntimeError('memhive is not compatible with Stackless.')


import random
import sys
import textwrap
import time
import threading
import marshal
import _xxsubinterpreters as subint

from ._memhive import _MemHive
from ._memhive import Map
from ._memhive import _MemQueue
from ._memhive import ClosedQueueError


class Executor:
    def __init__(self, *, nworkers: int=8):
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
            import time
            import threading
            import random
            sys.path = ''' + repr(sys.path) + '''

            import memhive._memhive as chive
            mem = chive._MemHiveSub(''' + repr(id(self._mem)) + ''')

            is_debug = hasattr(chive, 'enable_object_tracking')

            try:
                while True:
                    if is_debug:
                        chive.enable_object_tracking()
                    mem.do_refs()

                    p = mem.get()

                    idx, func_name, func_code, args = p

                    func_code = marshal.loads(func_code)
                    func = types.FunctionType(func_code, globals(), func_name)

                    ret = (idx, func(*args))
                    mem.put(ret)

                    if is_debug:
                        chive.disable_object_tracking()

            except chive.ClosedQueueError:
                pass
            finally:
                mem.do_refs()
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

        self._mem.do_refs()

        res = {}
        for i, args in enumerate(argss):
            p = (
                i,
                func.__name__,
                marshal.dumps(func.__code__),
                args
            )
            res[i] = None
            self._mem.put(p)
            payloads.append(p)

        try:
            for _ in range(len(argss)):
                ret = self._mem.get()
                res[ret[0]] = ret[1]

            return list(res.values())

        except KeyboardInterrupt:
            try:
                self.close()
            finally:
                raise

        finally:
            self._mem.do_refs()

    def close(self):
        self._mem.do_refs()
        self._mem.close_subs_intake()
        for t in self._workers:
            t.join()
        self._workers = []
