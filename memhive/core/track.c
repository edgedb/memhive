#include "debug.h"
#include "track.h"
#include "module.h"

#include "Python.h"

#ifdef DEBUG
int
MemHive_DebugIsLocalObject(module_state *state, PyObject *obj)
{
    if (!IS_TRACKABLE(state, obj)) {
        // really we have no idea in this case, so just say yes.
        return 1;
    }

    if (!IS_TRACKING(state)) {
        // tracking is disabled, hence nothing to do here.
        return 1;
    }

    PyObject *id = PyLong_FromVoidPtr(obj);
    assert(id != NULL);
    int is_local = PySet_Contains(state->debug_objects_ids, id);
    Py_DecRef(id);
    assert(is_local >= 0);
    return is_local;
}
#endif
