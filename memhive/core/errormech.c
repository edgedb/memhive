#include "errormech.h"
#include "debug.h"
#include "module.h"
#include "utils.h"

#include "Python.h"
#include "frameobject.h"

/* This file implements error marshaling from one subinterpreter to another.
   =========================================================================

It exposes two functions:

* `MemHive_DumpError` to capture the essential information
  about exception `err` into an immutable object, comprised of tuples
  and immutable scalar types. This transformation preserves information about:

  - Exception type name (fully qualified)
  - Exception message (by calling `str(err)`)
  - __cause__ and __context__
  - Tracebacks with filanames, funcnames, and line numbers
  - For ExceptionGroup - the nested exception.

  E.g., here's an example output:

    (("[Errno 2] No such file or directory: 'alksdjhsijfhdskjhfsdkjhf'",
      'FileNotFoundError',
      None,
      (('/Users/yury/dev/memhive/t13.py', 'zap', 13),),
      None,
      None),
     ('division by zero',
      'ZeroDivisionError',
      None,
      (('/Users/yury/dev/memhive/t13.py', 'zap', 8),),
      None,
      None),
     ('unexpected?',
      'ExceptionGroup',
      (0, 1),
      (('/Users/yury/dev/memhive/t13.py', 'zap', 15),
       ('/Users/yury/dev/memhive/t13.py', 'bar', 21)),
      None,
      0),
     ("unsupported operand type(s) for +: 'int' and 'str'",
      'TypeError',
      None,
      (('/Users/yury/dev/memhive/t13.py', 'bar', 23),
       ('/Users/yury/dev/memhive/t13.py', 'foo', 27),
       ('/Users/yury/dev/memhive/t13.py', '<module>', 30)),
      None,
      2))

* `MemHive_RestoreError` to re-build exception from its serialized from.
  Here's what the above data unpacks into:

    Traceback (most recent call last):
      File "/Users/yury/dev/memhive/t13.py", line 13, in zap
        open('alksdjhsijfhdskjhfsdkjhf', 'r')
    __subinterpreter__.FileNotFoundError: [Errno 2] No such file or directory:
    'alksdjhsijfhdskjhfsdkjhf'

    During handling of the above exception, another exception occurred:

      + Exception Group Traceback (most recent call last):
      |   File "/Users/yury/dev/memhive/t13.py", line 21, in bar
      |     zap()
      |   File "/Users/yury/dev/memhive/t13.py", line 15, in zap
      |     raise ExceptionGroup('unexpected?', [ex, err])
      | __subinterpreter__.ExceptionGroup: unexpected? (2 sub-exceptions)
      +-+---------------- 1 ----------------
        | Traceback (most recent call last):
        |   File "/Users/yury/dev/memhive/t13.py", line 13, in zap
        |     open('alksdjhsijfhdskjhfsdkjhf', 'r')
        | __subinterpreter__.FileNotFoundError: [Errno 2] No such file or
        | directory: 'alksdjhsijfhdskjhfsdkjhf'
        +---------------- 2 ----------------
        | Traceback (most recent call last):
        |   File "/Users/yury/dev/memhive/t13.py", line 8, in zap
        |     1/0
        | __subinterpreter__.ZeroDivisionError: division by zero
        +------------------------------------

    During handling of the above exception, another exception occurred:

    Traceback (most recent call last):
      File "/Users/yury/dev/memhive/t13.py", line 45, in <module>
        raise cc.restore_error(d)
      File "/Users/yury/dev/memhive/t13.py", line 30, in <module>
        foo()
      File "/Users/yury/dev/memhive/t13.py", line 27, in foo
        bar()
      File "/Users/yury/dev/memhive/t13.py", line 23, in bar
        1 + '1'
     __subinterpreter__.TypeError: unsupported operand type(s) for +: 'int'
     and 'str'

Notes:

* We do not preserve actual exception types. The restored exception tree
  consists of instances of dynamically generated subclasses of Exception.
  Only type name is preserved.

  This is because we think that the following is an anti-pattern
  (in pseudo-code):

    try:
        subinterpreter.run('collection[key]')
    except KeyErorr:
        ...

  While enabling this style is theoretically possible, in practice it would
  have mediocre performance. It's more reasonable to instead raise a
  "RemoteError" with the reflected exception set via `__cause__`. This
  simplifies debugging and encourages to build better APIs.

* We use the `PyFrame_New` API which isn't documented as public CPython API.
  Cython uses it too, so we should be fine.

* Dynamically generated error types and frame/code objects are cached in
  the module state. See the "state->exc_*" set of fields.

* Serializing errors into immutable data is important, as it allows us to
  implement the same approach to sharing error data that we use for sharing
  our immutable collections.
*/


#define ERR_NFIELDS                 6
#define ERR_GET_MSG(tup)            ((RemoteObject *)PyTuple_GET_ITEM(tup, 0))
#define ERR_SET_MSG(tup, o)         PyTuple_SET_ITEM(tup, 0, o)
#define ERR_GET_NAME(tup)           ((RemoteObject *)PyTuple_GET_ITEM(tup, 1))
#define ERR_SET_NAME(tup, o)        PyTuple_SET_ITEM(tup, 1, o)
#define ERR_GET_GROUP_EXCS(tup)     ((RemoteObject *)PyTuple_GET_ITEM(tup, 2))
#define ERR_SET_GROUP_EXCS(tup, o)  PyTuple_SET_ITEM(tup, 2, o)
#define ERR_GET_TB(tup)             ((RemoteObject *)PyTuple_GET_ITEM(tup, 3))
#define ERR_SET_TB(tup, o)          PyTuple_SET_ITEM(tup, 3, o)
#define ERR_GET_CAUSE(tup)          ((RemoteObject *)PyTuple_GET_ITEM(tup, 4))
#define ERR_SET_CAUSE(tup, o)       PyTuple_SET_ITEM(tup, 4, o)
#define ERR_GET_CONTEXT(tup)        ((RemoteObject *)PyTuple_GET_ITEM(tup, 5))
#define ERR_SET_CONTEXT(tup, o)     PyTuple_SET_ITEM(tup, 5, o)

#define TB_NFIELDS                  3
#define TB_GET_FILENAME(tup)        ((RemoteObject *)PyTuple_GET_ITEM(tup, 0))
#define TB_SET_FILENAME(tup, o)     PyTuple_SET_ITEM(tup, 0, o)
#define TB_GET_FUNCNAME(tup)        ((RemoteObject *)PyTuple_GET_ITEM(tup, 1))
#define TB_SET_FUNCNAME(tup, o)     PyTuple_SET_ITEM(tup, 1, o)
#define TB_GET_LINENO(tup)          ((RemoteObject *)PyTuple_GET_ITEM(tup, 2))
#define TB_SET_LINENO(tup, o)       PyTuple_SET_ITEM(tup, 2, o)


static PyObject *
new_none_tuple(ssize_t size)
{
    PyObject *tup = PyTuple_New(size);
    if (tup == NULL) {
        return NULL;
    }
    for (ssize_t i = 0; i < size; i++) {
        PyTuple_SET_ITEM(tup, i, Py_None);
    }
    return tup;
}


static PyObject *
list_to_tuple(PyObject *lst)
{
    assert(PyList_CheckExact(lst));
    ssize_t size = Py_SIZE(lst);
    PyObject *tup = PyTuple_New(size);
    if (tup == NULL) {
        return NULL;
    }
    for (ssize_t i = 0; i < size; i++) {
        PyObject *el = PyList_GET_ITEM(lst, i);
        PyTuple_SET_ITEM(tup, i, el);
        Py_INCREF(el);
    }
    return tup;
}


static int
reflect_tb(PyObject *tb_list, PyTracebackObject *tb)
{
    if (tb == NULL) {
        return 0;
    }

    int ret_code = 0;
    PyObject *ser = NULL;
    PyCodeObject *code = NULL;

    assert(PyTraceBack_Check(tb));

    if (reflect_tb(tb_list, tb->tb_next) < 0) {
        goto err;
    }

    code = PyFrame_GetCode(tb->tb_frame); /* newref */

    ser = new_none_tuple(TB_NFIELDS);
    if (ser == NULL) {
        goto err;
    }

    PyObject *fn = code->co_filename;
    TB_SET_FILENAME(ser, fn);
    Py_INCREF(fn);

    fn = code->co_name;
    TB_SET_FUNCNAME(ser, fn);
    Py_INCREF(fn);

    ssize_t lineno = (ssize_t)tb->tb_lineno;
    if (lineno == -1) {
        lineno = PyCode_Addr2Line(code, tb->tb_lasti);
        if (lineno < 0) {
            goto err;
        }
    }

    Py_CLEAR(code);

    PyObject *ln = PyLong_FromSsize_t(lineno);
    if (ln == NULL) {
        goto err;
    }
    TB_SET_LINENO(ser, ln);

    if (PyList_Append(tb_list, ser) < 0) {
        goto err;
    }

    goto done;

err:
    ret_code = -1;
done:
    Py_XDECREF(ser);
    Py_XDECREF(code);
    return ret_code;
}


static ssize_t
reflect_error(PyObject *err, PyObject *memo, PyObject *ret)
{
    int is_group = _PyBaseExceptionGroup_Check(err);

    PyObject *pos = PyDict_GetItemWithError(memo, err); /* borrow */
    if (pos == NULL) {
        if (PyErr_Occurred()) {
            return -1;
        }
    } else {
        return PyLong_AsSsize_t(pos);
    }

    PyObject *tb_list = NULL;

    PyObject *reflected_error = new_none_tuple(ERR_NFIELDS);
    if (reflected_error == NULL) {
        return -1;
    }

    PyObject *name = PyUnicode_FromString(Py_TYPE(err)->tp_name);
    if (name == NULL) {
        goto err;
    }
    ERR_SET_NAME(reflected_error, name);

    PyObject *msg = NULL;
    if (is_group) {
        PyObject *group = ((PyBaseExceptionGroupObject *)err)->excs;
        if (group != NULL)  {
            if (!PyTuple_CheckExact(group)) {
                PyErr_Format(PyExc_TypeError,
                             "expected a tuple for group->excs");
                goto err;
            }

            PyObject *reflected_group = PyTuple_New(Py_SIZE(group));
            if (reflected_group == NULL) {
                goto err;
            }

            for (ssize_t i = 0; i < Py_SIZE(group); i++) {
                PyObject *groupped_error = PyTuple_GetItem(group, i);
                assert(groupped_error != NULL);
                ssize_t er = reflect_error(groupped_error, memo, ret);
                if (er < 0) {
                    goto err;
                }

                PyObject *eri = PyLong_FromSsize_t(er);
                if (eri == NULL) {
                    Py_DECREF(reflected_group);
                    goto err;
                }
                PyTuple_SET_ITEM(reflected_group, i, eri);
            }

            ERR_SET_GROUP_EXCS(reflected_error, reflected_group);
        }

        msg = ((PyBaseExceptionGroupObject *)err)->msg;
        Py_INCREF(msg);
        ERR_SET_MSG(reflected_error, msg);
    } else {
        msg = PyObject_Str(err);
        if (msg == NULL) {
            PyObject *by_err = PyErr_GetRaisedException(); /* clears error */
            assert(by_err != NULL);
            // TODO: add information about the exception into the message
            msg = PyUnicode_FromFormat(
                "ERROR WHILE CALLING __str__ ON AN EXCEPTION IN " \
                "SUB INTERPRETER: %S", by_err
            );
            Py_CLEAR(by_err);
            if (msg == NULL) {
                goto err;
            }
        }
        ERR_SET_MSG(reflected_error, msg);
    }

    tb_list = PyList_New(0);
    if (tb_list == NULL) {
        goto err;
    }

    PyTracebackObject* tb =
        (PyTracebackObject*)PyException_GetTraceback(err);  /* newref */
    if (reflect_tb(tb_list, tb) < 0)
    {
        Py_CLEAR(tb);
        goto err;
    }
    Py_CLEAR(tb);

    PyObject *tb_tuple = list_to_tuple(tb_list);
    Py_CLEAR(tb_list);
    if (tb_tuple == NULL) {
        goto err;
    }
    ERR_SET_TB(reflected_error, tb_tuple);

    PyObject *cause = PyException_GetCause(err);
    if (cause != Py_None && cause != NULL) {
        if (cause == err) {
            // do nothing
            Py_DECREF(cause);
        } else {
            ssize_t cp = reflect_error(cause, memo, ret);
            Py_DECREF(cause);
            if (cp < 0) {
                goto err;
            }
            PyObject *cpp = PyLong_FromSsize_t(cp);
            if (cpp < 0) {
                goto err;
            }
            ERR_SET_CAUSE(reflected_error, cpp);
        }
    }

    PyObject *context = PyException_GetContext(err);
    if (context != Py_None && context != NULL) {
        if (context == err) {
            // do nothing
            Py_DECREF(context);
        } else {
            ssize_t cp = reflect_error(context, memo, ret);
            Py_DECREF(context);
            if (cp < 0) {
                goto err;
            }
            PyObject *cpp = PyLong_FromSsize_t(cp);
            if (cpp < 0) {
                goto err;
            }
            ERR_SET_CONTEXT(reflected_error, cpp);
        }
    }

    if (PyList_Append(ret, reflected_error) < 0) {
        goto err;
    }
    Py_DECREF(reflected_error);

    ssize_t p = Py_SIZE(ret) - 1;
    PyObject *pp = PyLong_FromSsize_t(p);
    if (pp == NULL) {
        goto err;
    }
    int r = PyDict_SetItem(memo, err, pp);
    Py_DECREF(pp);
    if (r < 0) {
        goto err;
    }

    return p;

err:
    Py_XDECREF(reflected_error);
    Py_XDECREF(tb_list);
    return -1;
}


PyObject *
MemHive_DumpError(PyObject *err)
{
    if (!PyExceptionInstance_Check(err)) {
        PyErr_Format(PyExc_ValueError, "expected an exception instance");
        return NULL;
    }

    PyObject *memo = NULL;
    PyObject *ret = NULL;

    memo = PyDict_New();
    if (memo == NULL) {
        goto err;
    }

    ret = PyList_New(0);
    if (ret == NULL) {
        goto err;
    }

    if (reflect_error(err, memo, ret) < 0) {
        goto err;
    }

    Py_DECREF(memo);

    PyObject *ret_tup = list_to_tuple(ret);
    Py_DECREF(ret);
    if (ret_tup == NULL) {
        return NULL;
    }
    return ret_tup;

err:
    Py_XDECREF(memo);
    Py_XDECREF(ret);
    return NULL;
}


////////////////////////////////////////////////////////////////////////////////


static PyFrameObject *
make_frame(module_state *state, RemoteObject *filename, RemoteObject *funcname)
{
    char c_cache_key[2048];

    const char *c_funcname = PyUnicode_AsUTF8((PyObject*)funcname);
    if (c_funcname == NULL) {
        return NULL;
    }

    const char *c_filename = PyUnicode_AsUTF8((PyObject*)filename);
    if (c_filename == NULL) {
        return NULL;
    }

    if (snprintf(c_cache_key, sizeof(c_cache_key),
                 "%s:%s", c_funcname, c_filename) < 0)
    {
        PyErr_Format(PyExc_ValueError,
                     "failed to compute frame cache key");
        return NULL;
    }

    PyObject *cache_key = PyUnicode_FromString(c_cache_key);
    if (cache_key == NULL) {
        return NULL;
    }

    PyFrameObject *frame = (PyFrameObject *)PyDict_GetItem(
        state->exc_frames_cache, cache_key);
    if (frame != NULL) {
        Py_INCREF(frame);
        goto done;
    }

    PyCodeObject *code = PyCode_NewEmpty(c_filename, c_funcname, 0);
    if (code == NULL) {
        goto err;
    }

    // Without this, the built-in traceback printer injects empty lines.
    code->co_firstlineno = -1;

    // Using a semi-private API here. Although Cython is doing the same
    // and seems to be OK.
    frame = PyFrame_New(
        PyThreadState_Get(),
        code,
        state->exc_empty_dict, // reuse one dict as nothing should mutate it
        NULL
    );
    Py_CLEAR(code);
    if (frame == NULL) {
        goto err;
    }

    if (PyDict_SetItem(state->exc_frames_cache,
                       cache_key, (PyObject*)frame) < 0)
    {
        goto err;
    }

done:
    Py_DECREF(cache_key);
    return frame;

err:
    Py_DECREF(cache_key);
    Py_XDECREF(code);
    Py_XDECREF(frame);
    return NULL;
}


static PyTracebackObject *
make_traceback(module_state *state,
               RemoteObject *filename, RemoteObject *funcname,
               RemoteObject *lineno)
{
    int c_lineno = (int)PyLong_AsLong((PyObject *)lineno);
    if (c_lineno < 0) {
        if (PyErr_Occurred() != NULL) {
            return NULL;
        }
        PyErr_Format(PyExc_ValueError, "lineno cannot be negative");
        return NULL;
    }

    PyFrameObject *frame = make_frame(state, filename, funcname);
    if (frame == NULL) {
        return NULL;
    }

    PyTracebackObject *tb = PyObject_GC_New(
        PyTracebackObject, &PyTraceBack_Type);
    if (tb == NULL) {
        Py_DECREF(frame);
        return NULL;
    }

    tb->tb_frame = frame;
    tb->tb_next = NULL;
    tb->tb_lineno = c_lineno;

    // Needed for traceback.py to use `tb_lineno` and not try to
    // fetch ranges from the code object.
    tb->tb_lasti = -1;

    PyObject_GC_Track(tb);
    return tb;
}


static PyObject *
make_error_type(module_state *state, RemoteObject *name, int is_group)
{
    const char *c_name = PyUnicode_AsUTF8((PyObject*)name);
    if (c_name == NULL) {
        return NULL;
    }

    char c_qual_name[1024];
    if (snprintf(c_qual_name, sizeof(c_qual_name),
                 "__subinterpreter__.%s", c_name) < 0)
    {
        PyErr_Format(PyExc_ValueError,
                     "could not prepend module name to error's __name__");
        return NULL;
    }

    char c_cache_key[1024];
    if (snprintf(c_cache_key, sizeof(c_cache_key),
                 "%s|%d", c_qual_name, is_group) < 0)
    {
        PyErr_Format(PyExc_ValueError,
                     "could not prepend module name to error's __name__");
        return NULL;
    }

    PyObject *type = PyDict_GetItemString(state->exc_types_cache, c_cache_key);
    if (type != NULL) {
        Py_INCREF(type);
        return type;
    }

    PyObject *bases = NULL;
    if (is_group) {
        bases = PyTuple_Pack(
            2, PyExc_BaseExceptionGroup, PyExc_Exception);
        if (bases == NULL) {
            return NULL;
        }
    }

    type = PyErr_NewException(c_qual_name, bases, NULL);
    Py_XDECREF(bases);
    if (type == NULL) {
        return NULL;
    }

    if (PyDict_SetItemString(state->exc_types_cache, c_cache_key, type) < 0) {
        Py_DECREF(type);
        return NULL;
    }

    return type;
}

static PyObject *
fetch_reflected_error(RemoteObject *index, PyObject *memo)
{
    ssize_t p = PyLong_AsSsize_t((PyObject*)index);
    if (p < 0) {
        if (PyErr_Occurred() != NULL) {
            return NULL;
        }
        PyErr_Format(PyExc_RuntimeError, "negative error index");
        return NULL;
    }
    if (p >= Py_SIZE(memo)) {
        PyErr_Format(PyExc_RuntimeError, "out of bound error index");
        return NULL;
    }

    PyObject *err = PyTuple_GET_ITEM(memo, p);
    if (err == NULL) {
        PyErr_Format(PyExc_RuntimeError,
                     "attempting to index unreflected error");
        return NULL;
    }

    Py_INCREF(err);
    return err;
}


static int
restore_error(module_state *state,
              RemoteObject *errors_desc, ssize_t index, PyObject *memo)
{
    assert(PyTuple_CheckExact(errors_desc));
    assert(PyTuple_CheckExact(memo));

    PyObject *msg = NULL;
    PyObject *err_cls = NULL;
    PyObject *err = NULL;

    RemoteObject *error_desc = (RemoteObject *)PyTuple_GET_ITEM(
        (PyObject*)errors_desc, index);
    assert(PyTuple_CheckExact(error_desc));

    RemoteObject *name = ERR_GET_NAME((PyObject*)error_desc);

    RemoteObject *_msg = ERR_GET_MSG((PyObject*)error_desc);
    assert((PyObject*)_msg != Py_None);
    msg = MemHive_CopyString((PyObject*)_msg);
    if (msg == NULL) {
        goto error;
    }
    _msg = NULL;

    RemoteObject *tbs = ERR_GET_TB((PyObject*)error_desc);

    RemoteObject *group_excs = ERR_GET_GROUP_EXCS((PyObject*)error_desc);
    int is_group = (PyObject*)group_excs != Py_None;

    err_cls = make_error_type(state, name, is_group);
    if (err_cls == NULL) {
        goto error;
    }

    if (is_group) {
        PyObject *reflected_excs = PyTuple_New(Py_SIZE(group_excs));
        if (reflected_excs == NULL) {
            goto error;
        }

        for (ssize_t i = 0; i < Py_SIZE(group_excs); i++) {
            PyObject *exc = fetch_reflected_error(
                (RemoteObject *)PyTuple_GET_ITEM(group_excs, i),
                memo
            );
            if (exc == NULL) {
                Py_CLEAR(reflected_excs);
                goto error;
            }
            PyTuple_SET_ITEM(reflected_excs, i, exc);
        }

        PyObject *args = PyTuple_Pack(2, msg, reflected_excs);
        Py_CLEAR(msg);
        Py_CLEAR(reflected_excs);
        if (args == NULL) {
            goto error;
        }

        err = PyObject_CallObject(err_cls, args);
        Py_CLEAR(args);
        Py_CLEAR(err_cls);
        if (err == NULL) {
            goto error;
        }
    } else {
        err = PyObject_CallOneArg(err_cls, msg);
        Py_CLEAR(msg);
        Py_CLEAR(err_cls);
        if (err == NULL) {
            return -1;
        }
    }

    PyTracebackObject *tb = NULL;
    for (ssize_t i = 0; i < Py_SIZE(tbs); i++) {
        RemoteObject *tl = (RemoteObject *)PyTuple_GET_ITEM((PyObject*)tbs, i);
        assert(tl != NULL);
        assert(PyTuple_CheckExact(tl));

        PyTracebackObject *tb_next = make_traceback(
            state,
            (RemoteObject *)TB_GET_FILENAME((PyObject*)tl),
            (RemoteObject *)TB_GET_FUNCNAME((PyObject*)tl),
            (RemoteObject *)TB_GET_LINENO((PyObject*)tl)
        );

        if (tb_next == NULL) {
            Py_CLEAR(tb);
            goto error;
        }

        tb_next->tb_next = tb;
        tb = tb_next;
    }

    if (tb != NULL) {
        int r = PyException_SetTraceback(err, (PyObject *)tb);
        Py_CLEAR(tb);
        if (r < 0) {
            goto error;
        }
    }

    RemoteObject *cause_index = ERR_GET_CAUSE((PyObject*)error_desc);
    if ((PyObject*)cause_index != Py_None) {
        PyObject *cause_error = fetch_reflected_error(cause_index, memo);
        if (cause_error == NULL) {
            goto error;
        }
        PyException_SetCause(err, cause_error);
    }

    RemoteObject *context_index = ERR_GET_CONTEXT((PyObject*)error_desc);
    if ((PyObject*)context_index != Py_None) {
        PyObject *context_error = fetch_reflected_error(context_index, memo);
        if (context_error == NULL) {
            goto error;
        }
        PyException_SetContext(err, context_error);
    }

    PyTuple_SET_ITEM(memo, index, err);
    return 0;

error:
    Py_XDECREF(msg);
    Py_XDECREF(err_cls);
    Py_XDECREF(err);
    return -1;
}


PyObject *
MemHive_RestoreError(module_state *state, RemoteObject *errors_desc)
{
    if (!PyTuple_CheckExact(errors_desc)) {
        PyErr_Format(PyExc_ValueError, "expected a tuple");
        return NULL;
    }

    PyObject *memo = NULL;
    ssize_t size = Py_SIZE(errors_desc);

    memo = PyTuple_New(size);
    if (memo == NULL) {
        goto err;
    }

    for (ssize_t i = 0; i < size; i++) {
        if (restore_error(state, errors_desc, i, memo) < 0) {
            goto err;
        }
    }

    PyObject *err = PyTuple_GET_ITEM(memo, size - 1);
    assert(err != NULL);
    Py_INCREF(err);
    Py_DECREF(memo);
    return err;

err:
    Py_XDECREF(memo);
    return NULL;
}
