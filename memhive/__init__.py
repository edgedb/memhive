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
import _xxsubinterpreters as subint

from ._memhive import _MemHive
from ._memhive import Map


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
