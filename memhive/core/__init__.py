from ._core import Map
from ._core import MemHive, MemHiveSub
from ._core import ClosedQueueError

try:
    from ._core import enable_object_tracking, disable_object_tracking
except ImportError:
    pass

try:
    import stackless
except ImportError:
    pass
else:
    # The only reason for this is us abusing Py_TPFLAGS reserved
    # for Stackless. That's a temporary limitation; there's a better
    # solution.
    raise RuntimeError('memhive is not compatible with Stackless.')
