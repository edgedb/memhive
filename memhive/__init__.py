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
from ._memhive import ClosedQueueError

import memhive._memhive as chive


class Executor:
    def __init__(self, *, nworkers: int=8):
        self._mem = _MemHive()
        self._nworkers = nworkers
        self._workers = []
        self.is_debug = hasattr(chive, 'enable_object_tracking')
        for _ in range(self._nworkers):
            self._make_sub()
        # if self.is_debug:
        #     chive.enable_object_tracking()


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

            STOP = 0
            def dd():
                while not STOP:
                    mem.do_refs()
                    time.sleep(0.01)
            tt = threading.Thread(target=dd)
            tt.start()

            if is_debug:
                chive.enable_object_tracking()

            try:
                while True:
                    mem.do_refs()

                    p = mem.get()
                    mem.do_refs()

                    idx, func_name, func_code, args = p

                    func_code = marshal.loads(func_code)
                    func = types.FunctionType(func_code, globals(), func_name)

                    ret = (idx, func(*args))
                    mem.push(ret)

                    # if is_debug:
                    #     chive.disable_object_tracking()

            except chive.ClosedQueueError:
                pass
            finally:
                STOP = 1
                tt.join()
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

    def put(self, argss, func):
        self._mem.do_refs()

        self.STOP = 0
        def dd():
            while not self.STOP:
                self._mem.do_refs()
                time.sleep(0.01)
        self.tt = threading.Thread(target=dd)
        self.tt.start()


        res = {}
        for i, args in enumerate(argss):
            p = (
                i,
                func.__name__,
                marshal.dumps(func.__code__),
                args
            )
            res[i] = None
            print(f">>>>>>>>>>>>>> tuple args 0x{id(p):x}")
            self._mem.push(p)
            self._mem.do_refs()
            del p

        return res

    def get(self, res):
        try:
            for _ in range(len(res)):
                ret = self._mem.get()
                res[ret[0]] = ret[1]

            return list(res.values())

        except KeyboardInterrupt:
            try:
                self.close()
            finally:
                raise

        finally:
            self.STOP = 1
            self.tt.join()
            self._mem.do_refs()

    def close(self):
        self._mem.do_refs()
        self._mem.close_subs_intake()
        for t in self._workers:
            t.join()
        self._workers = []
