#include <stddef.h> /* For offsetof */
#include <stdarg.h>

#include "pythoncapi_compat.h"
#include "map.h"
#include "memhive.h"
#include "module.h"
#include "debug.h"
#include "track.h"

#include "structmember.h"

/*
This file provides an implemention of an immutable mapping using the
Hash Array Mapped Trie (or HAMT) datastructure.

This design allows to have:

1. Efficient copy: immutable mappings can be copied by reference,
   making it an O(1) operation.

2. Efficient mutations: due to structural sharing, only a portion of
   the trie needs to be copied when the collection is mutated.  The
   cost of set/delete operations is O(log N).

3. Efficient lookups: O(log N).

(where N is number of key/value items in the immutable mapping.)


HAMT
====

The core idea of HAMT is that the shape of the trie is encoded into the
hashes of keys.

Say we want to store a K/V pair in our mapping.  First, we calculate the
hash of K, let's say it's 19830128, or in binary:

    0b1001011101001010101110000 = 19830128

Now let's partition this bit representation of the hash into blocks of
5 bits each:

    0b00_00000_10010_11101_00101_01011_10000 = 19830128
          (6)   (5)   (4)   (3)   (2)   (1)

Each block of 5 bits represents a number between 0 and 31.  So if we have
a tree that consists of nodes, each of which is an array of 32 pointers,
those 5-bit blocks will encode a position on a single tree level.

For example, storing the key K with hash 19830128, results in the following
tree structure:

                     (array of 32 pointers)
                     +---+ -- +----+----+----+ -- +----+
  root node          | 0 | .. | 15 | 16 | 17 | .. | 31 |   0b10000 = 16 (1)
  (level 1)          +---+ -- +----+----+----+ -- +----+
                                      |
                     +---+ -- +----+----+----+ -- +----+
  a 2nd level node   | 0 | .. | 10 | 11 | 12 | .. | 31 |   0b01011 = 11 (2)
                     +---+ -- +----+----+----+ -- +----+
                                      |
                     +---+ -- +----+----+----+ -- +----+
  a 3rd level node   | 0 | .. | 04 | 05 | 06 | .. | 31 |   0b00101 = 5  (3)
                     +---+ -- +----+----+----+ -- +----+
                                      |
                     +---+ -- +----+----+----+----+
  a 4th level node   | 0 | .. | 04 | 29 | 30 | 31 |        0b11101 = 29 (4)
                     +---+ -- +----+----+----+----+
                                      |
                     +---+ -- +----+----+----+ -- +----+
  a 5th level node   | 0 | .. | 17 | 18 | 19 | .. | 31 |   0b10010 = 18 (5)
                     +---+ -- +----+----+----+ -- +----+
                                      |
                       +--------------+
                       |
                     +---+ -- +----+----+----+ -- +----+
  a 6th level node   | 0 | .. | 15 | 16 | 17 | .. | 31 |   0b00000 = 0  (6)
                     +---+ -- +----+----+----+ -- +----+
                       |
                       V -- our value (or collision)

To rehash: for a K/V pair, the hash of K encodes where in the tree V will
be stored.

To optimize memory footprint and handle hash collisions, our implementation
uses three different types of nodes:

 * A Bitmap node;
 * An Array node;
 * A Collision node.

Because we implement an immutable dictionary, our nodes are also
immutable.  Therefore, when we need to modify a node, we copy it, and
do that modification to the copy.


Array Nodes
-----------

These nodes are very simple.  Essentially they are arrays of 32 pointers
we used to illustrate the high-level idea in the previous section.

We use Array nodes only when we need to store more than 16 pointers
in a single node.

Array nodes do not store key objects or value objects.  They are used
only as an indirection level - their pointers point to other nodes in
the tree.


Bitmap Node
-----------

Allocating a new 32-pointers array for every node of our tree would be
very expensive.  Unless we store millions of keys, most of tree nodes would
be very sparse.

When we have less than 16 elements in a node, we don't want to use the
Array node, that would mean that we waste a lot of memory.  Instead,
we can use bitmap compression and can have just as many pointers
as we need!

Bitmap nodes consist of two fields:

1. An array of pointers.  If a Bitmap node holds N elements, the
   array will be of N pointers.

2. A 32bit integer -- a bitmap field.  If an N-th bit is set in the
   bitmap, it means that the node has an N-th element.

For example, say we need to store a 3 elements sparse array:

   +---+  --  +---+  --  +----+  --  +----+
   | 0 |  ..  | 4 |  ..  | 11 |  ..  | 17 |
   +---+  --  +---+  --  +----+  --  +----+
                |          |           |
                o1         o2          o3

We allocate a three-pointer Bitmap node.  Its bitmap field will be
then set to:

   0b_00100_00010_00000_10000 == (1 << 17) | (1 << 11) | (1 << 4)

To check if our Bitmap node has an I-th element we can do:

   bitmap & (1 << I)


And here's a formula to calculate a position in our pointer array
which would correspond to an I-th element:

   popcount(bitmap & ((1 << I) - 1))


Let's break it down:

 * `popcount` is a function that returns a number of bits set to 1;

 * `((1 << I) - 1)` is a mask to filter the bitmask to contain bits
   set to the *right* of our bit.


So for our 17, 11, and 4 indexes:

 * bitmap & ((1 << 17) - 1) == 0b100000010000 => 2 bits are set => index is 2.

 * bitmap & ((1 << 11) - 1) == 0b10000 => 1 bit is set => index is 1.

 * bitmap & ((1 << 4) - 1) == 0b0 => 0 bits are set => index is 0.


To conclude: Bitmap nodes are just like Array nodes -- they can store
a number of pointers, but use bitmap compression to eliminate unused
pointers.


Bitmap nodes have two pointers for each item:

  +----+----+----+----+  --  +----+----+
  | k1 | v1 | k2 | v2 |  ..  | kN | vN |
  +----+----+----+----+  --  +----+----+

When kI == NULL, vI points to another tree level.

When kI != NULL, the actual key object is stored in kI, and its
value is stored in vI.


Collision Nodes
---------------

Collision nodes are simple arrays of pointers -- two pointers per
key/value.  When there's a hash collision, say for k1/v1 and k2/v2
we have `hash(k1)==hash(k2)`.  Then our collision node will be:

  +----+----+----+----+
  | k1 | v1 | k2 | v2 |
  +----+----+----+----+


Tree Structure
--------------

All nodes are PyObjects.

The `MapObject` object has a pointer to the root node (h_root),
and has a length field (h_count).

High-level functions accept a MapObject object and dispatch to
lower-level functions depending on what kind of node h_root points to.


Operations
==========

There are three fundamental operations on an immutable dictionary:

1. "o.assoc(k, v)" will return a new immutable dictionary, that will be
   a copy of "o", but with the "k/v" item set.

   Functions in this file:

        map_node_assoc, map_node_bitmap_assoc,
        map_node_array_assoc, map_node_collision_assoc

   `map_node_assoc` function accepts a node object, and calls
   other functions depending on its actual type.

2. "o.find(k)" will lookup key "k" in "o".

   Functions:

        map_node_find, map_node_bitmap_find,
        map_node_array_find, map_node_collision_find

3. "o.without(k)" will return a new immutable dictionary, that will be
   a copy of "o", buth without the "k" key.

   Functions:

        map_node_without, map_node_bitmap_without,
        map_node_array_without, map_node_collision_without


Further Reading
===============

1. http://blog.higher-order.net/2009/09/08/understanding-clojures-persistenthashmap-deftwice.html

2. http://blog.higher-order.net/2010/08/16/assoc-and-clojures-persistenthashmap-part-ii.html

3. Clojure's PersistentHashMap implementation:
   https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/PersistentHashMap.java
*/


#define TYPENAME_MAP "memhive.Map"
#define TYPENAME_MAPMUT "memhive.MapMutation"

#define TYPENAME_ARRAY_NODE "memhive.core.ArrayNode"
#define TYPENAME_BITMAP_NODE "memhive.core.BitmapNode"
#define TYPENAME_COLLISION_NODE "memhive.core.CollisionNode"

// IS_XXX_NODE_SLOW methods can be called on any PyObject, no matter
// if it belongs to our subinterpreter or another one.
#define IS_MAP_SLOW(state, o)                                                 \
    (o != NULL && strcmp(Py_TYPE(o)->tp_name, TYPENAME_MAP) == 0)
#define IS_ARRAY_NODE_SLOW(state, o)                                          \
    (o != NULL && strcmp(Py_TYPE(o)->tp_name, TYPENAME_ARRAY_NODE) == 0)
#define IS_BITMAP_NODE_SLOW(state, o)                                         \
    (o != NULL && strcmp(Py_TYPE(o)->tp_name, TYPENAME_BITMAP_NODE) == 0)
#define IS_COLLISION_NODE_SLOW(state, o)                                      \
    (o != NULL && strcmp(Py_TYPE(o)->tp_name, TYPENAME_COLLISION_NODE) == 0)
#define IS_NODE_SLOW(state, o)                                                \
    (IS_BITMAP_NODE_SLOW(state, o)                                            \
     || IS_ARRAY_NODE_SLOW(state, o)                                          \
     || IS_COLLISION_NODE_SLOW(state, o))


#define IS_ARRAY_NODE(state, node)                                            \
    (((MapNode*)(node))->node_kind == N_ARRAY)

#define IS_BITMAP_NODE(state, node)                                           \
    (((MapNode*)(node))->node_kind == N_BITMAP)

#define IS_COLLISION_NODE(state, node)                                        \
    (((MapNode*)(node))->node_kind == N_COLLISION)

#define IS_NODE(state, node)                                                  \
    (IS_ARRAY_NODE(state, node) ||                                            \
        IS_BITMAP_NODE(state, node) || IS_COLLISION_NODE(state, node))


#define IS_NODE_LOCAL(state, node)                                            \
    (((MapNode *)(node))->interpreter_id == (state)->interpreter_id)

#define MAYBE_VISIT_NODE(state, obj)                                          \
    if (obj != NULL) {                                                        \
        assert(IS_NODE_SLOW(state, obj));                                     \
        if (IS_NODE_LOCAL(state, obj)) {                                      \
            Py_VISIT(obj);                                                    \
        }                                                                     \
    }

#define MAYBE_VISIT(state, obj)                                               \
    if (obj != NULL) {                                                        \
        assert(!IS_NODE_SLOW(state, obj));                                    \
        Py_VISIT(obj);                                                        \
    }

#define NODE_INCREF(state, node)                                              \
    do {                                                                      \
        MapNode *_n = (MapNode*)(node);                                       \
        module_state *_s = state;                                             \
        assert(IS_NODE_SLOW(state, _n));                                      \
        if (IS_NODE_LOCAL(_s, _n)) {                                          \
            Py_IncRef((PyObject*)_n);                                         \
        } else {                                                              \
            if (_s->sub != NULL) {                                            \
                MemHiveSub *_sub = (MemHiveSub*)_s->sub;                      \
                if (MemHive_RefQueue_Inc(_sub->main_refs,                     \
                                         (RemoteObject*)_n)) {                \
                    Py_FatalError("Failed to remotely incref node");          \
                }                                                             \
            } else {                                                          \
                Py_FatalError("destructing HAMT node after the sub is gone"); \
            }                                                                 \
        }                                                                     \
    } while(0);

#define NODE_DECREF(state, node)                                              \
    do {                                                                      \
        MapNode *_n = (MapNode*)(node);                                       \
        module_state *_s = state;                                             \
        assert(IS_NODE_SLOW(state, _n));                                      \
        if (_n->interpreter_id == _s->interpreter_id) {                       \
            Py_DecRef((PyObject*)_n);                                         \
        } else {                                                              \
            if (_s->sub != NULL) {                                            \
                MemHiveSub *_sub = (MemHiveSub*)_s->sub;                      \
                if (MemHive_RefQueue_Dec(_sub->main_refs,                     \
                                         (RemoteObject*)_n)) {                \
                    Py_FatalError("Failed to remotely incref node");          \
                }                                                             \
            } else {                                                          \
                Py_FatalError("Unreachable node condition");                  \
            }                                                                 \
        }                                                                     \
    } while(0);

#define NODE_XINCREF(state, node)                                             \
    if (node != NULL) {                                                       \
        NODE_INCREF(state, node);                                             \
    }

#define NODE_XDECREF(state, node)                                             \
    if (node != NULL) {                                                       \
        NODE_DECREF(state, node);                                             \
    }

#define NODE_SETREF(state, dst, src)                                          \
    do {                                                                      \
        assert(src == NULL || IS_NODE_SLOW(state, src));                      \
        NODE_DECREF(state, dst);                                              \
        dst = src;                                                            \
    } while(0)

#define NODE_XSETREF(state, dst, src)                                         \
    do {                                                                      \
        assert(src == NULL || IS_NODE_SLOW(state, src));                      \
        NODE_XDECREF(state, dst);                                             \
        dst = src;                                                            \
    } while(0)

#define NODE_CLEAR(state, node)                                               \
    do {                                                                      \
        NODE_XDECREF(state, node);                                            \
        node = NULL;                                                          \
    } while(0)

#ifdef DEBUG
    #define _ENSURE_LOCAL(state, o)                                           \
        assert(IS_LOCALLY_TRACKED(state, (PyObject *)o));

    #define INCREF(state, o)                                                  \
        do {                                                                  \
            assert(o != NULL);                                                \
            assert(!IS_NODE_SLOW(state, o));                                  \
            _ENSURE_LOCAL(state, o);                                          \
            Py_IncRef((PyObject*)o);                                          \
        } while(0)

    #define DECREF(state, o)                                                  \
        do {                                                                  \
            assert(o != NULL);                                                \
            assert(!IS_NODE_SLOW(state, o));                                  \
            _ENSURE_LOCAL(state, o);                                          \
            Py_DecRef((PyObject*)o);                                          \
        } while(0)

    #define XINCREF(state, o)                                                 \
        if (o != NULL) {                                                      \
            assert(!IS_NODE_SLOW(state, o));                                  \
            _ENSURE_LOCAL(state, o);                                          \
            Py_IncRef((PyObject*)o);                                          \
        } while(0)

    #define XDECREF(state, o)                                                 \
        if (o != NULL) {                                                      \
            assert(!IS_NODE_SLOW(state, o));                                  \
            _ENSURE_LOCAL(state, o);                                          \
            Py_DecRef((PyObject*)o);                                          \
        } while(0)

    #define CLEAR(state, o)                                                   \
        if (o != NULL) {                                                      \
            assert(!IS_NODE_SLOW(state, o));                                  \
            _ENSURE_LOCAL(state, o);                                          \
            Py_DecRef((PyObject*)o);                                          \
            o = NULL;                                                         \
        } while(0)

    #define SETREF(state, dst, src)                                           \
        do {                                                                  \
            assert(dst != NULL && !IS_NODE_SLOW(state, dst));                 \
            _ENSURE_LOCAL(state, dst);                                        \
            Py_DecRef((PyObject*)dst);                                        \
            assert(src == NULL || !IS_NODE_SLOW(state, src));                 \
            dst = src;                                                        \
        } while(0)

    #define XSETREF(state, dst, src)                                          \
        do {                                                                  \
            assert(dst == NULL || (                                           \
                !IS_NODE_SLOW(state, dst) && IS_LOCALLY_TRACKED(state, dst)   \
            ));                                                               \
            Py_DecRef((PyObject*)dst);                                        \
            assert(src == NULL || !IS_NODE_SLOW(state, src));                 \
            dst = src;                                                        \
        } while(0)

    #undef Py_INCREF
    #undef Py_XINCREF
    #undef Py_DECREF
    #undef Py_XDECREF
    #undef Py_SETREF
    #undef Py_XSETREF
    #undef Py_CLEAR

#else
    #define INCREF(state, o) Py_INCREF(o)
    #define DECREF(state, o) Py_DECREF(o)
    #define XDECREF(state, o) Py_XDECREF(o)
    #define XINCREF(state, o) Py_XINCREF(o)
    #define SETREF(state, dst, src) Py_SETREF(dst, src)
    #define XSETREF(state, dst, src) Py_XSETREF(dst, src)
    #define CLEAR(state, o) Py_CLEAR(o)
#endif


#ifdef DEBUG
static PyObject *
__CopyObject_Debug(module_state *state, PyObject *obj)
{
    assert(obj != NULL);
    assert(!IS_NODE_SLOW(state, obj));
    return MemHive_CopyObject(state, (RemoteObject*)obj);
}
#define COPY_OBJ(state, o) __CopyObject_Debug(state, o)
#else
#define COPY_OBJ(state, o) MemHive_CopyObject(state, o)
#endif


/* Return type for 'find' (lookup a key) functions.

   * F_ERROR - an error occurred;
   * F_NOT_FOUND - the key was not found;
   * F_FOUND - the key was found.
*/
typedef enum {F_ERROR, F_NOT_FOUND, F_FOUND, F_FOUND_EXT} map_find_t;


/* Return type for 'without' (delete a key) functions.

   * W_ERROR - an error occurred;
   * W_NOT_FOUND - the key was not found: there's nothing to delete;
   * W_EMPTY - the key was found: the node/tree would be empty
     if the key is deleted;
   * W_NEWNODE - the key was found: a new node/tree is returned
     without that key.
*/
typedef enum {W_ERROR, W_NOT_FOUND, W_EMPTY, W_NEWNODE} map_without_t;


/* Low-level iterator protocol type.

   * I_ITEM - a new item has been yielded;
   * I_END - the whole tree was visited (similar to StopIteration).
*/
typedef enum {I_ITEM, I_END} map_iter_t;

typedef enum {N_BITMAP, N_ARRAY, N_COLLISION} map_node_t;


#define HAMT_ARRAY_NODE_SIZE 32

#define _MapNodeCommonFields    \
    _InterpreterFields          \
    map_node_t node_kind;


typedef struct MapNode {
    PyObject_VAR_HEAD
    _MapNodeCommonFields
} MapNode;


typedef struct {
    PyObject_VAR_HEAD // XXX
    _MapNodeCommonFields
    MapNode *a_array[HAMT_ARRAY_NODE_SIZE];
    Py_ssize_t a_count;
    uint64_t a_mutid;
} MapNode_Array;


typedef struct {
    PyObject_VAR_HEAD
    _MapNodeCommonFields
    uint64_t b_mutid;
    uint32_t b_bitmap;
    PyObject *b_array[1];
} MapNode_Bitmap;


typedef struct {
    PyObject_VAR_HEAD
    _MapNodeCommonFields
    uint64_t c_mutid;
    int32_t c_hash;
    PyObject *c_array[1];
} MapNode_Collision;


/* Create a new HAMT immutable mapping. */
static MapObject *
map_new(module_state *mod);

/* Return a new collection based on "o", but with an additional
   key/val pair. */
static MapObject *
map_assoc(module_state *state,
          MapObject *o, PyObject *key, PyObject *val);

/* Return a new collection based on "o", but without "key". */
static MapObject *
map_without(module_state *state, MapObject *o, PyObject *key);

/* Check if "v" is equal to "w".

   Return:
   - 0: v != w
   - 1: v == w
   - -1: An error occurred.
*/
static int
map_eq(module_state *state, BaseMapObject *v, BaseMapObject *w);

static map_find_t
map_find(module_state *state, BaseMapObject *o, PyObject *key, PyObject **val);

/* Return the size of "o"; equivalent of "len(o)". */
static Py_ssize_t
map_len(BaseMapObject *o);


static MapObject *
map_alloc(module_state *);

static MapNode *
map_node_assoc(module_state *state,
               MapNode *node,
               uint32_t shift, int32_t hash,
               PyObject *key, PyObject *val, int* added_leaf,
               uint64_t mutid);

static map_without_t
map_node_without(module_state *state,
                 MapNode *node,
                 uint32_t shift, int32_t hash,
                 PyObject *key,
                 MapNode **new_node,
                 uint64_t mutid);

static map_find_t
map_node_find(module_state *state,
              MapNode *node,
              uint32_t shift, int32_t hash,
              PyObject *key, PyObject **val);

static int
map_node_dump(module_state *state,
              MapNode *node,
              _PyUnicodeWriter *writer, int level);

static MapNode *
map_node_array_new(module_state *state, Py_ssize_t, uint64_t mutid);

static MapNode *
map_node_collision_new(module_state *state,
                       int32_t hash,
                       Py_ssize_t size,
                       uint64_t mutid);

static inline Py_ssize_t
map_node_collision_count(MapNode_Collision *node);

static int
map_node_update(module_state *state,
                uint64_t mutid,
                PyObject *seq,
                MapNode *root, Py_ssize_t count,
                MapNode **new_root, Py_ssize_t *new_count);


static int
map_update_inplace(module_state *state,
                   uint64_t mutid,
                   BaseMapObject *o,
                   PyObject *src);

static MapObject *
map_update(module_state *state, uint64_t mutid, MapObject *o, PyObject *src);


#ifdef DEBUG

static int
debug_is_local_object_to_node(
    module_state *state, PyObject *obj, PyObject *node)
{
    if (!IS_TRACKABLE(state, obj)) {
        // really we have no idea in this case, so just say yes.
        return 1;
    }

    if (!IS_TRACKING(state)) {
        // tracking is disabled, hence nothing to do here.
        return 1;
    }

    return IS_LOCALLY_TRACKED(state, obj) == IS_NODE_LOCAL(state, node);
}

static void
map_node_bitmap_validate(module_state *state, MapNode_Bitmap *node)
{
    for (Py_ssize_t i = 0; i < Py_SIZE(node); i++) {
        if (i % 2 == 0) {
            // key
            if (node->b_array[i] != NULL) {
                assert(
                    debug_is_local_object_to_node(
                        state, node->b_array[i], (PyObject *)node)
                );
            }
        } else {
            assert(node->b_array[i] != NULL);
            if (node->b_array[i - 1] != NULL) {
                // this is a value for a non-NULL key, so it
                // must be a scalar value (not a node)
                assert(
                    debug_is_local_object_to_node(
                        state, node->b_array[i], (PyObject *)node)
                );
            } else {
                assert(IS_NODE_SLOW(state, node->b_array[i]));
            }
        }
    }
}

static void
map_node_array_validate(module_state *state, MapNode_Array *node)
{
    assert(node->a_count <= HAMT_ARRAY_NODE_SIZE);
    Py_ssize_t i = 0, count = 0;
    for (; i < HAMT_ARRAY_NODE_SIZE; i++) {
        if (node->a_array[i] != NULL) {
            count++;
        }
    }
    assert(count == node->a_count);
}

static void
map_node_collision_validate(module_state *state, MapNode_Collision *node)
{
    for (Py_ssize_t i = 0; i < Py_SIZE(node); i++) {
        assert(
            debug_is_local_object_to_node(
                state, node->c_array[i], (PyObject *)node)
        );
    }
}

static void
map_node_validate(module_state *state, PyObject *node)
{
    if (IS_BITMAP_NODE(state, node)) {
        return map_node_bitmap_validate(state, (MapNode_Bitmap *)node);
    }
    else if (IS_ARRAY_NODE(state, node)) {
        return map_node_array_validate(state, (MapNode_Array *)node);
    }
    else {
        assert(IS_COLLISION_NODE(state, node));
        return map_node_collision_validate(state, (MapNode_Collision *)node);
    }
}

#define VALIDATE_NODE(state, NODE) \
    do { map_node_validate(state, (PyObject*)NODE); } while (0);
#else
#define VALIDATE_NODE(state, NODE)
#endif


/* Returns -1 on error */
static inline int32_t
map_hash(PyObject *o)
{
    Py_hash_t hash = PyObject_Hash(o);

#if SIZEOF_PY_HASH_T <= 4
    return hash;
#else
    if (hash == -1) {
        /* exception */
        return -1;
    }

    /* While it's suboptimal to reduce Python's 64 bit hash to
       32 bits via XOR, it seems that the resulting hash function
       is good enough (this is also how Long type is hashed in Java.)
       Storing 10, 100, 1000 Python strings results in a relatively
       shallow and uniform tree structure.

       Please don't change this hashing algorithm, as there are many
       tests that test some exact tree shape to cover all code paths.
    */
    int32_t xored = (int32_t)(hash & 0xffffffffl) ^ (int32_t)(hash >> 32);
    return xored == -1 ? -2 : xored;
#endif
}

static inline uint32_t
map_mask(int32_t hash, uint32_t shift)
{
    return (((uint32_t)hash >> shift) & 0x01f);
}

static inline uint32_t
map_bitpos(int32_t hash, uint32_t shift)
{
    return (uint32_t)1 << map_mask(hash, shift);
}

static inline uint32_t
map_bitcount(uint32_t i)
{
    #if (defined(__clang__) || defined(__GNUC__))
        return (uint32_t)__builtin_popcount(i);
    #else
        /* The algorithm is copied from:
        https://graphics.stanford.edu/~seander/bithacks.html
        */
        i = i - ((i >> 1) & 0x55555555);
        i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
        return (((i + (i >> 4)) & 0xF0F0F0F) * 0x1010101) >> 24;
    #endif
}

static inline uint32_t
map_bitindex(uint32_t bitmap, uint32_t bit)
{
    return map_bitcount(bitmap & (bit - 1));
}

#ifdef _PyTime_GetMonotonicClock
#define get_monotonic_clock() (int64_t)_PyTime_GetMonotonicClock()
#else
static int64_t
get_monotonic_clock()
{
    PyTime_t result;
    PyTime_MonotonicRaw(&result);
    return (int64_t)result;
}
#endif

static uint64_t
new_mutid(module_state *state)
{
    // TODO: we used to have just a global counter for this,
    // but that would no longer work when the module
    // is initialized in multiple subinterpreters. Ideally,
    // the main subinterpreter should have the counter and
    // a lock around it in the root module state. Generating
    // new mutids isn't that frequent, so no lock contention
    // is expected. Maybe this is a good use case for stdatomic.
    // For now, we can just read the monotonic clock, it has
    // nanosecond precision (which should be enough) and
    // it might actually be quicker than messing with locks
    // (hey, we should benchmark that.)
    uint64_t mutid = (uint64_t)get_monotonic_clock();
    if (mutid == 0) {
        mutid = (uint64_t)get_monotonic_clock();
    }
    if (mutid == 0) {
        // mutid=0 is special, that's what new nodes are created with.
        // If we can't generate non-zero mutids we have a problem.
        // Luckily this can only happen if Python's time API is
        // malfunctioning, so let's pretend that can't happen.
        abort();
    }
    return mutid;
}


/////////////////////////////////// Dump Helpers

static int
_map_dump_ident(_PyUnicodeWriter *writer, int level)
{
    /* Write `'    ' * level` to the `writer` */
    PyObject *str = NULL;
    PyObject *num = NULL;
    PyObject *res = NULL;
    int ret = -1;

    str = PyUnicode_FromString("    ");
    if (str == NULL) {
        goto error;
    }

    num = PyLong_FromLong((long)level);
    if (num == NULL) {
        goto error;
    }

    res = PyNumber_Multiply(str, num);
    if (res == NULL) {
        goto error;
    }

    ret = _PyUnicodeWriter_WriteStr(writer, res);

error:
    Py_DecRef(res);
    Py_DecRef(str);
    Py_DecRef(num);
    return ret;
}

static int
_map_dump_format(_PyUnicodeWriter *writer, const char *format, ...)
{
    /* A convenient helper combining _PyUnicodeWriter_WriteStr and
       PyUnicode_FromFormatV.
    */
    PyObject* msg;
    int ret;

    va_list vargs;
#if PY_VERSION_HEX < 0x030C00A1 && !defined(HAVE_STDARG_PROTOTYPES)
    va_start(vargs);
#else
    va_start(vargs, format);
#endif
    msg = PyUnicode_FromFormatV(format, vargs);
    va_end(vargs);

    if (msg == NULL) {
        return -1;
    }

    ret = _PyUnicodeWriter_WriteStr(writer, msg);
    Py_DecRef(msg);
    return ret;
}

/////////////////////////////////// Bitmap Node


MapNode *
_map_node_bitmap_new(module_state *state, Py_ssize_t size, uint64_t mutid)
{
    /* Create a new bitmap node of size 'size' */

    MapNode_Bitmap *node;
    Py_ssize_t i;

    assert(size >= 0);
    assert(size % 2 == 0);

    /* No freelist; allocate a new bitmap node */
    node = PyObject_GC_NewVar(MapNode_Bitmap, state->BitmapNodeType, size);
    if (node == NULL) {
        return NULL;
    }
    TRACK(state, node);

    node->interpreter_id = state->interpreter_id;
    node->node_kind = N_BITMAP;

    Py_SET_SIZE(node, size);

    for (i = 0; i < size; i++) {
        node->b_array[i] = NULL;
    }

    node->b_bitmap = 0;
    node->b_mutid = mutid;

    PyObject_GC_Track(node);
    return (MapNode *)node;
}

static MapNode *
map_node_bitmap_new(module_state *state, Py_ssize_t size, uint64_t mutid)
{
    // TODO: re-introduce the empty bitmap node cache
    return _map_node_bitmap_new(state, size, mutid);
}

static inline Py_ssize_t
map_node_bitmap_count(MapNode_Bitmap *node)
{
    return Py_SIZE(node) / 2;
}

int
map_node_bitmap_copyels(module_state *state,
                        MapNode_Bitmap *from,
                        MapNode_Bitmap *to,
                        Py_ssize_t start,
                        Py_ssize_t end,
                        Py_ssize_t offset)
{
    int8_t local_from = IS_NODE_LOCAL(state, from);
    assert(start >= 0);
    assert(end <= Py_SIZE(from));
    assert(end + offset <= Py_SIZE(to));
    for (Py_ssize_t i = start; i < end; i++) {
        if (i % 2 == 0) {
            // key
            if (from->b_array[i] != NULL) {
                if (local_from) {
                    // object must be from our interp
                    to->b_array[i+offset] = from->b_array[i];
                    INCREF(state, to->b_array[i+offset]);
                } else {
                    PyObject *copy = COPY_OBJ(state, from->b_array[i]);
                    if (copy == NULL) {
                        return -1;
                    }
                    to->b_array[i+offset] = copy;
                }
            }
        } else {
            // value
            assert(from->b_array[i] != NULL);
            if (from->b_array[i - 1] == NULL) {
                // value must be a node, and can be a foreign one
                assert(IS_NODE(state, from->b_array[i]));
                to->b_array[i+offset] = from->b_array[i];
                NODE_INCREF(state, from->b_array[i]);
            } else {
                // value is an object (not a node!)
                if (local_from) {
                    // object must be from our interp
                    to->b_array[i+offset] = from->b_array[i];
                    INCREF(state, to->b_array[i+offset]);
                } else {
                    PyObject *copy = COPY_OBJ(state, from->b_array[i]);
                    if (copy == NULL) {
                        return -1;
                    }
                    to->b_array[i+offset] = copy;
                }
            }
        }
    }

    return 0;
}

static MapNode_Bitmap *
map_node_bitmap_clone(module_state *state, MapNode_Bitmap *node,
                      uint64_t mutid)
{
    VALIDATE_NODE(state, node);

    MapNode_Bitmap *clone = (MapNode_Bitmap *)map_node_bitmap_new(
        state, Py_SIZE(node), mutid);
    if (clone == NULL) {
        return NULL;
    }

    if (map_node_bitmap_copyels(state, node, clone, 0, Py_SIZE(node), 0)) {
        return NULL;
    }

    clone->b_bitmap = node->b_bitmap;

    VALIDATE_NODE(state, clone);
    return clone;
}

static MapNode_Bitmap *
map_node_bitmap_clone_without(module_state *state,
                              MapNode_Bitmap *o, uint32_t bit, uint64_t mutid)
{
    assert(bit & o->b_bitmap);
    assert(map_node_bitmap_count(o) > 1);

    uint32_t idx = map_bitindex(o->b_bitmap, bit);

    VALIDATE_NODE(state, o);

    MapNode_Bitmap *clone = (MapNode_Bitmap *)map_node_bitmap_new(
        state, Py_SIZE(o) - 2, mutid);
    if (clone == NULL) {
        return NULL;
    }

    if (map_node_bitmap_copyels(state, o, clone, 0, 2 * idx, 0)) {
        return NULL;
    }

    if (map_node_bitmap_copyels(state, o, clone, 2 * idx + 2, Py_SIZE(o), -2)) {
        return NULL;
    }

    clone->b_bitmap = o->b_bitmap & ~bit;

    VALIDATE_NODE(state, clone);
    return clone;
}

static MapNode *
map_node_new_bitmap_or_collision(module_state *state,
                                 uint32_t shift,
                                 PyObject *key1, PyObject *val1,
                                 int32_t key2_hash,
                                 PyObject *key2, PyObject *val2,
                                 uint64_t mutid)
{
    /* Helper method.  Creates a new node for key1/val and key2/val2
       pairs.

       If key1 hash is equal to the hash of key2, a Collision node
       will be created.  If they are not equal, a Bitmap node is
       created.
    */

    int32_t key1_hash = map_hash(key1);
    if (key1_hash == -1) {
        return NULL;
    }

    if (key1_hash == key2_hash) {
        MapNode_Collision *n;
        n = (MapNode_Collision *)map_node_collision_new(
            state, key1_hash, 4, mutid);
        if (n == NULL) {
            return NULL;
        }

        INCREF(state, key1);
        n->c_array[0] = key1;
        INCREF(state, val1);
        n->c_array[1] = val1;

        INCREF(state, key2);
        n->c_array[2] = key2;
        INCREF(state, val2);
        n->c_array[3] = val2;

        VALIDATE_NODE(state, n);

        return (MapNode *)n;
    }
    else {
        int added_leaf = 0;
        MapNode *n = map_node_bitmap_new(state, 0, mutid);
        if (n == NULL) {
            return NULL;
        }

        MapNode *n2 = map_node_assoc(
            state, n, shift, key1_hash, key1, val1, &added_leaf, mutid);
        NODE_DECREF(state, n);
        if (n2 == NULL) {
            return NULL;
        }

        assert(IS_NODE_LOCAL(state, n2));
        VALIDATE_NODE(state, n2);

        n = map_node_assoc(
            state, n2, shift, key2_hash, key2, val2, &added_leaf, mutid);
        NODE_DECREF(state, n2);
        if (n == NULL) {
            return NULL;
        }

        VALIDATE_NODE(state, n);

        return n;
    }
}

static MapNode *
map_node_bitmap_assoc(module_state *state,
                      MapNode_Bitmap *self,
                      uint32_t shift, int32_t hash,
                      PyObject *key, PyObject *val, int* added_leaf,
                      uint64_t mutid)
{
    /* assoc operation for bitmap nodes.

       Return: a new node, or self if key/val already is in the
       collection.

       'added_leaf' is later used in 'map_assoc' to determine if
       `map.set(key, val)` increased the size of the collection.
    */

    uint32_t bit = map_bitpos(hash, shift);
    uint32_t idx = map_bitindex(self->b_bitmap, bit);

    /* Bitmap node layout:

    +------+------+------+------+  ---  +------+------+
    | key1 | val1 | key2 | val2 |  ...  | keyN | valN |
    +------+------+------+------+  ---  +------+------+
    where `N < Py_SIZE(node)`.

    The `node->b_bitmap` field is a bitmap.  For a given
    `(shift, hash)` pair we can determine:

     - If this node has the corresponding key/val slots.
     - The index of key/val slots.
    */

    uint8_t local_node = IS_NODE_LOCAL(state, self);

    if (self->b_bitmap & bit) {
        /* The key is set in this node */

        uint32_t key_idx = 2 * idx;
        uint32_t val_idx = key_idx + 1;

        assert(val_idx < (size_t)Py_SIZE(self));

        PyObject *key_or_null = self->b_array[key_idx];
        PyObject *val_or_node = self->b_array[val_idx];

        if (key_or_null == NULL) {
            /* key is NULL.  This means that we have a few keys
               that have the same (hash, shift) pair. */

            assert(val_or_node != NULL);

            MapNode *sub_node = map_node_assoc(
                state,
                (MapNode *)val_or_node,
                shift + 5, hash, key, val, added_leaf,
                mutid);
            if (sub_node == NULL) {
                return NULL;
            }

            if (val_or_node == (PyObject *)sub_node) {
                NODE_DECREF(state, sub_node);
                NODE_INCREF(state, self);
                VALIDATE_NODE(state, self);
                return (MapNode *)self;
            }

            if (mutid != 0 && self->b_mutid == mutid) {
                assert(local_node);
                // We won't allow passing mutation objects between interpreters.
                // If we have the same mutid, it means that we must be mutating
                // a node that we've created in this subinterpreter.
                NODE_SETREF(state, self->b_array[val_idx], (PyObject*)sub_node);
                NODE_INCREF(state, self);
                VALIDATE_NODE(state, self);
                return (MapNode *)self;
            }
            else {
                MapNode_Bitmap *ret = map_node_bitmap_clone(state, self, mutid);
                if (ret == NULL) {
                    return NULL;
                }
                NODE_CLEAR(state, ret->b_array[val_idx]);
                NODE_INCREF(state, sub_node);
                ret->b_array[val_idx] = (PyObject*)sub_node;
                VALIDATE_NODE(state, ret);
                return (MapNode *)ret;
            }
        }

        assert(key_or_null != NULL);
        /* key_or_null is not NULL.  This means that we have only one other
           key in this collection that matches our hash for this shift. */

        int comp_err = PyObject_RichCompareBool(key, key_or_null, Py_EQ);
        if (comp_err < 0) {  /* exception in __eq__ */
            return NULL;
        }
        if (comp_err == 1) {  /* key == key_or_null */
            if (val == val_or_node) {
                /* we already have the same key/val pair; return self. */
                NODE_INCREF(state, self);
                VALIDATE_NODE(state, self);
                return (MapNode *)self;
            }

            /* We're setting a new value for the key we had before. */
            if (mutid != 0 && self->b_mutid == mutid) {
                assert(local_node);
                // We won't allow passing mutation objects between interpreters.
                // If we have the same mutid, it means that we must be mutating
                // a node that we've created in this subinterpreter.

                /* We've been mutating this node before: update inplace. */
                INCREF(state, val);
                SETREF(state, self->b_array[val_idx], val);
                NODE_INCREF(state, self);
                VALIDATE_NODE(state, self);
                return (MapNode *)self;
            }
            else {
                /* Make a new bitmap node with a replaced value,
                   and return it. */
                MapNode_Bitmap *ret = map_node_bitmap_clone(state, self, mutid);
                if (ret == NULL) {
                    return NULL;
                }
                INCREF(state, val);
                SETREF(state, ret->b_array[val_idx], val);
                VALIDATE_NODE(state, ret);
                return (MapNode *)ret;
            }
        }

        /* It's a new key, and it has the same index as *one* another key.
           We have a collision.  We need to create a new node which will
           combine the existing key and the key we're adding.

           `map_node_new_bitmap_or_collision` will either create a new
           Collision node if the keys have identical hashes, or
           a new Bitmap node.
        */

        if (!local_node) {
            key_or_null = COPY_OBJ(state, key_or_null);
            val_or_node = COPY_OBJ(state, val_or_node);
        }

        MapNode *sub_node = map_node_new_bitmap_or_collision(
            state,
            shift + 5,
            key_or_null, val_or_node,  /* existing key/val */
            hash,
            key, val,  /* new key/val */
            self->b_mutid
        );

        if (!local_node) {
            CLEAR(state, key_or_null);
            CLEAR(state, val_or_node);
        }

        if (sub_node == NULL) {
            return NULL;
        }

        if (mutid != 0 && self->b_mutid == mutid) {
            assert(local_node);
            SETREF(state, self->b_array[key_idx], NULL);
            DECREF(state, self->b_array[val_idx]);
            self->b_array[val_idx] = (PyObject *)sub_node;
            NODE_INCREF(state, self);

            *added_leaf = 1;
            VALIDATE_NODE(state, self);
            return (MapNode *)self;
        }
        else {
            MapNode_Bitmap *ret = map_node_bitmap_clone(state, self, mutid);
            if (ret == NULL) {
                NODE_DECREF(state, sub_node);
                return NULL;
            }
            SETREF(state, ret->b_array[key_idx], NULL);
            DECREF(state, ret->b_array[val_idx]);
            ret->b_array[val_idx] = (PyObject *)sub_node;

            *added_leaf = 1;
            VALIDATE_NODE(state, ret);
            return (MapNode *)ret;
        }
    }
    else {
        /* There was no key before with the same (shift,hash). */

        uint32_t n = map_bitcount(self->b_bitmap);

        if (n >= 16) {
            /* When we have a situation where we want to store more
               than 16 nodes at one level of the tree, we no longer
               want to use the Bitmap node with bitmap encoding.

               Instead we start using an Array node, which has
               simpler (faster) implementation at the expense of
               having prealocated 32 pointers for its keys/values
               pairs.

               Small map objects (<30 keys) usually don't have any
               Array nodes at all.  Between ~30 and ~400 keys map
               objects usually have one Array node, and usually it's
               a root node.
            */

            uint32_t jdx = map_mask(hash, shift);
            /* 'jdx' is the index of where the new key should be added
               in the new Array node we're about to create. */

            MapNode *empty = NULL;
            MapNode_Array *new_node = NULL;
            MapNode *res = NULL;

            /* Create a new Array node. */
            new_node = (MapNode_Array *)map_node_array_new(state, n + 1, mutid);
            if (new_node == NULL) {
                goto fin;
            }

            /* Create an empty bitmap node for the next
               map_node_assoc call. */
            empty = map_node_bitmap_new(state, 0, mutid);
            if (empty == NULL) {
                goto fin;
            }

            /* Make a new bitmap node for the key/val we're adding.
               Set that bitmap node to new-array-node[jdx]. */
            new_node->a_array[jdx] = map_node_assoc(
                state,
                empty, shift + 5, hash, key, val, added_leaf, mutid);
            if (new_node->a_array[jdx] == NULL) {
                goto fin;
            }

            /* Copy existing key/value pairs from the current Bitmap
               node to the new Array node we've just created. */
            Py_ssize_t i, j;
            for (i = 0, j = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
                if (((self->b_bitmap >> i) & 1) != 0) {
                    /* Ensure we don't accidentally override `jdx` element
                       we set few lines above.
                    */
                    assert(new_node->a_array[i] == NULL);

                    if (self->b_array[j] == NULL) {
                        new_node->a_array[i] =
                            (MapNode *)self->b_array[j + 1];
                        NODE_INCREF(state, new_node->a_array[i]);
                    }
                    else {
                        int32_t rehash = map_hash(self->b_array[j]);
                        if (rehash == -1) {
                            goto fin;
                        }

                        PyObject *bk = self->b_array[j];
                        PyObject *bv = self->b_array[j + 1];

                        if (bk != NULL) {
                            bk = COPY_OBJ(state, bk);
                            bv = COPY_OBJ(state, bv);
                        }

                        new_node->a_array[i] = map_node_assoc(
                            state,
                            empty, shift + 5,
                            rehash,
                            bk,
                            bv,
                            added_leaf,
                            mutid);

                        if (bk != NULL) {
                            CLEAR(state, bk);
                            CLEAR(state, bv);
                        }

                        if (new_node->a_array[i] == NULL) {
                            goto fin;
                        }
                    }
                    j += 2;
                }
            }

            VALIDATE_NODE(state, new_node)

            /* That's it! */
            res = (MapNode *)new_node;

        fin:
            NODE_XDECREF(state, empty);
            if (res == NULL) {
                NODE_XDECREF(state, new_node);
            }
            return res;
        }
        else {
            /* We have less than 16 keys at this level; let's just
               create a new bitmap node out of this node with the
               new key/val pair added. */

            uint32_t key_idx = 2 * idx;
            uint32_t val_idx = key_idx + 1;

            *added_leaf = 1;

            /* Allocate new Bitmap node which can have one more key/val
               pair in addition to what we have already. */
            MapNode_Bitmap *new_node =
                (MapNode_Bitmap *)map_node_bitmap_new(
                    state, 2 * (n + 1), mutid);
            if (new_node == NULL) {
                return NULL;
            }

            if (map_node_bitmap_copyels(state, self, new_node, 0, key_idx, 0)) {
                return NULL;
            }

            /* Set the new key/value to the new Bitmap node. */
            INCREF(state, key);
            new_node->b_array[key_idx] = key;
            INCREF(state, val);
            new_node->b_array[val_idx] = val;

            if (map_node_bitmap_copyels(
                    state, self, new_node, key_idx, Py_SIZE(self), 2))
            {
                return NULL;
            }

            new_node->b_bitmap = self->b_bitmap | bit;
            VALIDATE_NODE(state, new_node);
            return (MapNode *)new_node;
        }
    }
}

static map_without_t
map_node_bitmap_without(module_state *state,
                        MapNode_Bitmap *self,
                        uint32_t shift, int32_t hash,
                        PyObject *key,
                        MapNode **new_node,
                        uint64_t mutid)
{
    uint32_t bit = map_bitpos(hash, shift);
    if ((self->b_bitmap & bit) == 0) {
        return W_NOT_FOUND;
    }

    uint32_t idx = map_bitindex(self->b_bitmap, bit);

    uint32_t key_idx = 2 * idx;
    uint32_t val_idx = key_idx + 1;

    PyObject *key_or_null = self->b_array[key_idx];
    PyObject *val_or_node = self->b_array[val_idx];

    if (key_or_null == NULL) {
        /* key == NULL means that 'value' is another tree node. */

        MapNode *sub_node = NULL;
        MapNode_Bitmap *target = NULL;

        map_without_t res = map_node_without(
            state,
            (MapNode *)val_or_node,
            shift + 5, hash, key, &sub_node,
            mutid);

        VALIDATE_NODE(state, sub_node);

        switch (res) {
            case W_EMPTY:
                /* It's impossible for us to receive a W_EMPTY here:

                    - Array nodes are converted to Bitmap nodes when
                      we delete 16th item from them;

                    - Collision nodes are converted to Bitmap when
                      there is one item in them;

                    - Bitmap node's without() inlines single-item
                      sub-nodes.

                   So in no situation we can have a single-item
                   Bitmap child of another Bitmap node.
                */
                abort();

            case W_NEWNODE: {
                assert(sub_node != NULL);

                if (IS_BITMAP_NODE(state, sub_node)) {
                    MapNode_Bitmap *sub_tree = (MapNode_Bitmap *)sub_node;
                    if (map_node_bitmap_count(sub_tree) == 1 &&
                            sub_tree->b_array[0] != NULL)
                    {
                        /* A bitmap node with one key/value pair.  Just
                           merge it into this node.

                           Note that we don't inline Bitmap nodes that
                           have a NULL key -- those nodes point to another
                           tree level, and we cannot simply move tree levels
                           up or down.
                        */

                        if (mutid != 0 && self->b_mutid == mutid) {
                            assert(IS_NODE_LOCAL(state, self));
                            target = self;
                            NODE_INCREF(state, target);
                        }
                        else {
                            target = map_node_bitmap_clone(state, self, mutid);
                            if (target == NULL) {
                                NODE_DECREF(state, sub_node);
                                return W_ERROR;
                            }
                        }

                        PyObject *key = sub_tree->b_array[0];
                        PyObject *val = sub_tree->b_array[1];

                        if (target->b_array[key_idx] == NULL) {
                            NODE_CLEAR(state, target->b_array[val_idx]);
                        }
                        INCREF(state, val);
                        XSETREF(state, target->b_array[val_idx], val);

                        INCREF(state, key);
                        XSETREF(state, target->b_array[key_idx], key);

                        NODE_DECREF(state, sub_tree);

                        *new_node = (MapNode *)target;
                        return W_NEWNODE;
                    }
                }

#ifdef DEBUG
                /* Ensure that Collision.without implementation
                   converts to Bitmap nodes itself.
                */
                if (IS_COLLISION_NODE(state, sub_node)) {
                    assert(map_node_collision_count(
                            (MapNode_Collision*)sub_node) > 1);
                }
#endif

                if (mutid != 0 && self->b_mutid == mutid) {
                    target = self;
                    NODE_INCREF(state, target);
                }
                else {
                    target = map_node_bitmap_clone(state, self, mutid);
                    if (target == NULL) {
                        return W_ERROR;
                    }
                }

                NODE_SETREF(
                    state,
                    target->b_array[val_idx],
                    (PyObject *)sub_node);  /* borrow */

                *new_node = (MapNode *)target;
                VALIDATE_NODE(state, *new_node);
                return W_NEWNODE;
            }

            case W_ERROR:
            case W_NOT_FOUND:
                assert(sub_node == NULL);
                return res;

            default:
                abort();
        }
    }
    else {
        /* We have a regular key/value pair */

        int cmp = PyObject_RichCompareBool(key_or_null, key, Py_EQ);
        if (cmp < 0) {
            return W_ERROR;
        }
        if (cmp == 0) {
            return W_NOT_FOUND;
        }

        if (map_node_bitmap_count(self) == 1) {
            return W_EMPTY;
        }

        *new_node = (MapNode *)
            map_node_bitmap_clone_without(state, self, bit, mutid);
        if (*new_node == NULL) {
            return W_ERROR;
        }

        VALIDATE_NODE(state, *new_node);
        return W_NEWNODE;
    }
}

static map_find_t
map_node_bitmap_find(module_state *state,
                     MapNode_Bitmap *self,
                     uint32_t shift, int32_t hash,
                     PyObject *key, PyObject **val)
{
    /* Lookup a key in a Bitmap node. */

    uint32_t bit = map_bitpos(hash, shift);
    uint32_t idx;
    uint32_t key_idx;
    uint32_t val_idx;
    PyObject *key_or_null;
    PyObject *val_or_node;
    int comp_err;

    if ((self->b_bitmap & bit) == 0) {
        return F_NOT_FOUND;
    }

    idx = map_bitindex(self->b_bitmap, bit);
    key_idx = idx * 2;
    val_idx = key_idx + 1;

    assert(val_idx < (size_t)Py_SIZE(self));

    key_or_null = self->b_array[key_idx];
    val_or_node = self->b_array[val_idx];

    if (key_or_null == NULL) {
        /* There are a few keys that have the same hash at the current shift
           that match our key.  Dispatch the lookup further down the tree. */
        assert(val_or_node != NULL);
        return map_node_find(state,
                             (MapNode *)val_or_node,
                             shift + 5, hash, key, val);
    }

    /* We have only one key -- a potential match.  Let's compare if the
       key we are looking at is equal to the key we are looking for. */
    assert(key != NULL);
    comp_err = PyObject_RichCompareBool(key, key_or_null, Py_EQ);
    if (comp_err < 0) {  /* exception in __eq__ */
        return F_ERROR;
    }
    if (comp_err == 1) {  /* key == key_or_null */
        *val = val_or_node;
        if (self->interpreter_id != state->interpreter_id) {
            return F_FOUND_EXT;
        } else {
            return F_FOUND;
        }
    }

    return F_NOT_FOUND;
}

static int
map_node_bitmap_traverse(MapNode_Bitmap *self, visitproc visit, void *arg)
{
    /* Bitmap's tp_traverse */
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    MAYBE_VISIT(state, Py_TYPE(self));

    Py_ssize_t i;
    for (i = Py_SIZE(self); --i >= 0; ) {
        if (i % 2 == 1 && self->b_array[i - 1] == NULL) {
            MAYBE_VISIT_NODE(state, self->b_array[i]);
        } else {
            MAYBE_VISIT(state, self->b_array[i]);
        }
    }

    return 0;
}

static void
map_node_bitmap_dealloc(MapNode_Bitmap *self)
{
    PyTypeObject *tp = Py_TYPE(self);
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    PyObject_GC_UnTrack(self);
    Py_TRASHCAN_BEGIN(self, map_node_bitmap_dealloc)

    for (Py_ssize_t i = 0; i < Py_SIZE(self); i++) {
        if (i % 2 == 0) {
            XDECREF(state, self->b_array[i]);
        } else {
            if (self->b_array[i - 1] == NULL) {
                NODE_XDECREF(state, self->b_array[i]);
            } else {
                XDECREF(state, self->b_array[i]);
            }
        }
    }

    tp->tp_free((PyObject *)self);
    Py_TRASHCAN_END

    Py_DecRef((PyObject*)tp);
}

static int
map_node_bitmap_dump(module_state *state,
                     MapNode_Bitmap *node,
                     _PyUnicodeWriter *writer, int level)
{
    /* Debug build: __dump__() method implementation for Bitmap nodes. */

    Py_ssize_t i;
    PyObject *tmp1;
    PyObject *tmp2;

    if (_map_dump_ident(writer, level + 1)) {
        goto error;
    }

    if (_map_dump_format(writer, "BitmapNode(interpreter=%zd size=%zd count=%zd ",
                         node->interpreter_id,
                         Py_SIZE(node), Py_SIZE(node) / 2))
    {
        goto error;
    }

    tmp1 = PyLong_FromUnsignedLong(node->b_bitmap);
    if (tmp1 == NULL) {
        goto error;
    }
    TRACK(state, tmp1);
    tmp2 = PyNumber_ToBase(tmp1, 2);
    DECREF(state, tmp1);
    if (tmp2 == NULL) {
        goto error;
    }
    TRACK(state, tmp2);
    if (_map_dump_format(writer, "bitmap=%S id=%p):\n", tmp2, node)) {
        DECREF(state, tmp2);
        goto error;
    }
    DECREF(state, tmp2);

    for (i = 0; i < Py_SIZE(node); i += 2) {
        PyObject *key_or_null = node->b_array[i];
        PyObject *val_or_node = node->b_array[i + 1];

        if (_map_dump_ident(writer, level + 2)) {
            goto error;
        }

        if (key_or_null == NULL) {
            if (_map_dump_format(writer, "NULL:\n")) {
                goto error;
            }

            if (map_node_dump(state,
                              (MapNode *)val_or_node,
                              writer, level + 2))
            {
                goto error;
            }
        }
        else {
            if (_map_dump_format(writer, "%R: %R", key_or_null,
                                 val_or_node))
            {
                goto error;
            }
        }

        if (_map_dump_format(writer, "\n")) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
}


/////////////////////////////////// Collision Node


static MapNode *
map_node_collision_new(module_state *state,
                       int32_t hash, Py_ssize_t size, uint64_t mutid)
{
    /* Create a new Collision node. */

    MapNode_Collision *node;
    Py_ssize_t i;

    assert(size >= 4);
    assert(size % 2 == 0);

    node = PyObject_GC_NewVar(
        MapNode_Collision, state->CollisionNodeType, size);
    if (node == NULL) {
        return NULL;
    }
    TRACK(state, node);

    node->interpreter_id = state->interpreter_id;
    node->node_kind = N_COLLISION;

    for (i = 0; i < size; i++) {
        node->c_array[i] = NULL;
    }

    Py_SET_SIZE(node, size);
    node->c_hash = hash;

    node->c_mutid = mutid;

    PyObject_GC_Track(node);
    return (MapNode *)node;
}

static map_find_t
map_node_collision_find_index(module_state *state,
                              MapNode_Collision *self, PyObject *key,
                              Py_ssize_t *idx)
{
    /* Lookup `key` in the Collision node `self`.  Set the index of the
       found key to 'idx'. */

    Py_ssize_t i;
    PyObject *el;

    for (i = 0; i < Py_SIZE(self); i += 2) {
        el = self->c_array[i];

        assert(el != NULL);
        int cmp = PyObject_RichCompareBool(key, el, Py_EQ);
        if (cmp < 0) {
            return F_ERROR;
        }
        if (cmp == 1) {
            *idx = i;
            if (self->interpreter_id != state->interpreter_id) {
                return F_FOUND_EXT;
            } else {
                return F_FOUND;
            }
        }
    }

    return F_NOT_FOUND;
}

static MapNode *
map_node_collision_assoc(module_state *state,
                         MapNode_Collision *self,
                         uint32_t shift, int32_t hash,
                         PyObject *key, PyObject *val, int* added_leaf,
                         uint64_t mutid)
{
    /* Set a new key to this level (currently a Collision node)
       of the tree. */

    if (hash == self->c_hash) {
        /* The hash of the 'key' we are adding matches the hash of
           other keys in this Collision node. */

        Py_ssize_t key_idx = -1;
        map_find_t found;
        MapNode_Collision *new_node;
        Py_ssize_t i;
        int local_node = IS_NODE_LOCAL(state, self);

        /* Let's try to lookup the new 'key', maybe we already have it. */
        found = map_node_collision_find_index(state, self, key, &key_idx);
        switch (found) {
            case F_ERROR:
                /* Exception. */
                return NULL;

            case F_NOT_FOUND:
                /* This is a totally new key.  Clone the current node,
                   add a new key/value to the cloned node. */

                new_node = (MapNode_Collision *)map_node_collision_new(
                    state,
                    self->c_hash, Py_SIZE(self) + 2, mutid);
                if (new_node == NULL) {
                    return NULL;
                }

                for (i = 0; i < Py_SIZE(self); i++) {
                    if (local_node) {
                        INCREF(state, self->c_array[i]);
                        new_node->c_array[i] = self->c_array[i];
                    } else {
                        new_node->c_array[i] =
                            COPY_OBJ(state, self->c_array[i]);
                    }
                }

                INCREF(state, key);
                new_node->c_array[i] = key;
                INCREF(state, val);
                new_node->c_array[i + 1] = val;

                *added_leaf = 1;
                VALIDATE_NODE(state, new_node);
                return (MapNode *)new_node;

            case F_FOUND_EXT:
                assert(!local_node);
            case F_FOUND:
                /* There's a key which is equal to the key we are adding. */

                assert(key_idx >= 0);
                assert(key_idx < Py_SIZE(self));
                Py_ssize_t val_idx = key_idx + 1;

                if (self->c_array[val_idx] == val) {
                    /* We're setting a key/value pair that's already set. */
                    // Theoretically this can happen even on remote node,
                    // e.g. val is `True`. No reason to recreate the node in
                    // that case.
                    NODE_INCREF(state, self);
                    VALIDATE_NODE(state, self);
                    return (MapNode *)self;
                }

                /* We need to replace old value for the key with
                   a new value. */

                if (mutid != 0 && self->c_mutid == mutid) {
                    assert(local_node);
                    new_node = self;
                    NODE_INCREF(state, self);
                }
                else {
                    /* Create a new Collision node.*/
                    new_node = (MapNode_Collision *)map_node_collision_new(
                        state,
                        self->c_hash, Py_SIZE(self), mutid);
                    if (new_node == NULL) {
                        return NULL;
                    }

                    /* Copy all elements of the old node to the new one. */
                    for (i = 0; i < Py_SIZE(self); i++) {
                        if (local_node) {
                            INCREF(state, self->c_array[i]);
                            new_node->c_array[i] = self->c_array[i];
                        } else {
                            new_node->c_array[i] =
                                COPY_OBJ(state, self->c_array[i]);
                        }
                    }
                }

                /* Replace the old value with the new value for the our key. */
                DECREF(state, new_node->c_array[val_idx]);
                INCREF(state, val);
                new_node->c_array[val_idx] = val;

                VALIDATE_NODE(state, new_node);
                return (MapNode *)new_node;

            default:
                abort();
        }
    }
    else {
        /* The hash of the new key is different from the hash that
           all keys of this Collision node have.

           Create a Bitmap node inplace with two children:
           key/value pair that we're adding, and the Collision node
           we're replacing on this tree level.
        */

        MapNode_Bitmap *new_node;
        MapNode *assoc_res;

        new_node = (MapNode_Bitmap *)map_node_bitmap_new(state, 2, mutid);
        if (new_node == NULL) {
            return NULL;
        }
        new_node->b_bitmap = map_bitpos(self->c_hash, shift);
        NODE_INCREF(state, self);
        new_node->b_array[1] = (PyObject*) self;

        VALIDATE_NODE(state, new_node);

        assoc_res = map_node_bitmap_assoc(
            state, new_node, shift, hash, key, val, added_leaf, mutid);
        NODE_DECREF(state, new_node);

        VALIDATE_NODE(state, assoc_res);
        return assoc_res;
    }
}

static inline Py_ssize_t
map_node_collision_count(MapNode_Collision *node)
{
    return Py_SIZE(node) / 2;
}

static map_without_t
map_node_collision_without(module_state *state,
                           MapNode_Collision *self,
                           uint32_t shift, int32_t hash,
                           PyObject *key,
                           MapNode **new_node,
                           uint64_t mutid)
{
    if (hash != self->c_hash) {
        return W_NOT_FOUND;
    }

    Py_ssize_t key_idx = -1;
    map_find_t found = map_node_collision_find_index(
        state, self, key, &key_idx);

    int local_node = IS_NODE_LOCAL(state, self);

    switch (found) {
        case F_ERROR:
            return W_ERROR;

        case F_NOT_FOUND:
            return W_NOT_FOUND;

        case F_FOUND_EXT:
            assert(!local_node);
        case F_FOUND:
            assert(key_idx >= 0);
            assert(key_idx < Py_SIZE(self));

            Py_ssize_t new_count = map_node_collision_count(self) - 1;

            if (new_count == 0) {
                /* The node has only one key/value pair and it's for the
                   key we're trying to delete.  So a new node will be empty
                   after the removal.
                */
                return W_EMPTY;
            }

            if (new_count == 1) {
                /* The node has two keys, and after deletion the
                   new Collision node would have one.  Collision nodes
                   with one key shouldn't exist, so convert it to a
                   Bitmap node.
                */
                MapNode_Bitmap *node = (MapNode_Bitmap *)
                    map_node_bitmap_new(state, 2, mutid);
                if (node == NULL) {
                    return W_ERROR;
                }

                if (key_idx == 0) {
                    if (local_node) {
                        INCREF(state, self->c_array[2]);
                        node->b_array[0] = self->c_array[2];
                        INCREF(state, self->c_array[3]);
                        node->b_array[1] = self->c_array[3];
                    } else {
                        node->b_array[0] = COPY_OBJ(state, self->c_array[2]);
                        node->b_array[1] = COPY_OBJ(state, self->c_array[3]);
                    }
                }
                else {
                    assert(key_idx == 2);
                    if (local_node) {
                        INCREF(state, self->c_array[0]);
                        node->b_array[0] = self->c_array[0];
                        INCREF(state, self->c_array[1]);
                        node->b_array[1] = self->c_array[1];
                    } else {
                        node->b_array[0] = COPY_OBJ(state, self->c_array[0]);
                        node->b_array[1] = COPY_OBJ(state, self->c_array[1]);
                    }
                }

                node->b_bitmap = map_bitpos(hash, shift);

                *new_node = (MapNode *)node;
                VALIDATE_NODE(state, node);
                return W_NEWNODE;
            }

            /* Allocate a new Collision node with capacity for one
               less key/value pair */
            MapNode_Collision *new = (MapNode_Collision *)
                map_node_collision_new(
                    state, self->c_hash, Py_SIZE(self) - 2, mutid);
            if (new == NULL) {
                return W_ERROR;
            }

            /* Copy all other keys from `self` to `new` */
            Py_ssize_t i;
            if (local_node) {
                for (i = 0; i < key_idx; i++) {
                    INCREF(state, self->c_array[i]);
                    new->c_array[i] = self->c_array[i];
                }
                for (i = key_idx + 2; i < Py_SIZE(self); i++) {
                    INCREF(state, self->c_array[i]);
                    new->c_array[i - 2] = self->c_array[i];
                }
            } else {
                for (i = 0; i < key_idx; i++) {
                    new->c_array[i] = COPY_OBJ(state, self->c_array[i]);
                }
                for (i = key_idx + 2; i < Py_SIZE(self); i++) {
                    new->c_array[i - 2] = COPY_OBJ(state, self->c_array[i]);
                }
            }

            *new_node = (MapNode*)new;
            VALIDATE_NODE(state, new);
            return W_NEWNODE;

        default:
            abort();
    }
}

static map_find_t
map_node_collision_find(module_state *state,
                        MapNode_Collision *self,
                        uint32_t shift, int32_t hash,
                        PyObject *key, PyObject **val)
{
    /* Lookup `key` in the Collision node `self`.  Set the value
       for the found key to 'val'. */

    Py_ssize_t idx = -1;
    map_find_t res;

    res = map_node_collision_find_index(state, self, key, &idx);
    if (res == F_ERROR || res == F_NOT_FOUND) {
        return res;
    }

    assert(idx >= 0);
    assert(idx + 1 < Py_SIZE(self));

    *val = self->c_array[idx + 1];
    assert(*val != NULL);

    assert(res == F_FOUND || res == F_FOUND_EXT);
    return res;
}


static int
map_node_collision_traverse(MapNode_Collision *self,
                            visitproc visit, void *arg)
{
    /* Collision's tp_traverse */

    #ifdef DEBUG
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    USED_IN_DEBUG(state);
    #endif

    MAYBE_VISIT(state, Py_TYPE(self));

    Py_ssize_t i;
    for (i = Py_SIZE(self); --i >= 0; ) {
        MAYBE_VISIT(state, self->c_array[i]);
    }

    return 0;
}

static void
map_node_collision_dealloc(MapNode_Collision *self)
{
    /* Collision's tp_dealloc */

    PyTypeObject *tp = Py_TYPE(self);

    #ifdef DEBUG
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    #endif

    Py_ssize_t len = Py_SIZE(self);

    PyObject_GC_UnTrack(self);
    Py_TRASHCAN_BEGIN(self, map_node_collision_dealloc)

    if (len > 0) {

        while (--len >= 0) {
            XDECREF(state, self->c_array[len]);
        }
    }

    tp->tp_free((PyObject *)self);
    Py_TRASHCAN_END

    Py_DecRef((PyObject*)tp);
}

static int
map_node_collision_dump(module_state *state,
                        MapNode_Collision *node,
                        _PyUnicodeWriter *writer, int level)
{
    /* Debug build: __dump__() method implementation for Collision nodes. */

    Py_ssize_t i;

    if (_map_dump_ident(writer, level + 1)) {
        goto error;
    }

    if (_map_dump_format(writer, "CollisionNode(interpreter=%zd size=%zd id=%p):\n",
                         node->interpreter_id, Py_SIZE(node), node))
    {
        goto error;
    }

    for (i = 0; i < Py_SIZE(node); i += 2) {
        PyObject *key = node->c_array[i];
        PyObject *val = node->c_array[i + 1];

        if (_map_dump_ident(writer, level + 2)) {
            goto error;
        }

        if (_map_dump_format(writer, "%R: %R\n", key, val)) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
}


/////////////////////////////////// Array Node


static MapNode *
map_node_array_new(module_state *state, Py_ssize_t count, uint64_t mutid)
{
    Py_ssize_t i;

    MapNode_Array *node = PyObject_GC_New(MapNode_Array, state->ArrayNodeType);
    if (node == NULL) {
        return NULL;
    }
    TRACK(state, node);

    node->interpreter_id = state->interpreter_id;
    node->node_kind = N_ARRAY;

    for (i = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
        node->a_array[i] = NULL;
    }

    node->a_count = count;
    node->a_mutid = mutid;

    PyObject_GC_Track(node);
    return (MapNode *)node;
}

static MapNode_Array *
map_node_array_clone(module_state *state, MapNode_Array *node, uint64_t mutid)
{
    MapNode_Array *clone;
    Py_ssize_t i;

    VALIDATE_NODE(state, node)
    assert(node->a_count <= HAMT_ARRAY_NODE_SIZE);

    /* Create a new Array node. */
    clone = (MapNode_Array *)map_node_array_new(state, node->a_count, mutid);
    if (clone == NULL) {
        return NULL;
    }

    /* Copy all elements from the current Array node to the new one. */
    for (i = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
        NODE_XINCREF(state, node->a_array[i]);
        clone->a_array[i] = node->a_array[i];
    }

    clone->a_mutid = mutid;

    VALIDATE_NODE(state, clone)
    return clone;
}

static MapNode *
map_node_array_assoc(module_state *state,
                     MapNode_Array *self,
                     uint32_t shift, int32_t hash,
                     PyObject *key, PyObject *val, int* added_leaf,
                     uint64_t mutid)
{
    /* Set a new key to this level (currently a Collision node)
       of the tree.

       Array nodes don't store values, they can only point to
       other nodes.  They are simple arrays of 32 BaseNode pointers/
     */

    uint32_t idx = map_mask(hash, shift);
    MapNode *node = self->a_array[idx];
    MapNode *child_node;
    MapNode_Array *new_node;
    Py_ssize_t i;

    if (node == NULL) {
        /* There's no child node for the given hash.  Create a new
           Bitmap node for this key. */

        MapNode_Bitmap *empty = NULL;

        /* Get an empty Bitmap node to work with. */
        empty = (MapNode_Bitmap *)map_node_bitmap_new(state, 0, mutid);
        if (empty == NULL) {
            return NULL;
        }

        /* Set key/val to the newly created empty Bitmap, thus
           creating a new Bitmap node with our key/value pair. */
        child_node = map_node_bitmap_assoc(
            state,
            empty,
            shift + 5, hash, key, val, added_leaf, mutid);
        VALIDATE_NODE(state, child_node);
        NODE_DECREF(state, empty);
        if (child_node == NULL) {
            return NULL;
        }

        if (mutid != 0 && self->a_mutid == mutid) {
            new_node = self;
            self->a_count++;
            NODE_INCREF(state, self);
        }
        else {
            /* Create a new Array node. */
            new_node = (MapNode_Array *)map_node_array_new(
                state,
                self->a_count + 1, mutid);
            if (new_node == NULL) {
                NODE_DECREF(state, child_node);
                return NULL;
            }

            /* Copy all elements from the current Array node to the
               new one. */
            for (i = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
                NODE_XINCREF(state, self->a_array[i]);
                new_node->a_array[i] = self->a_array[i];
            }
        }

        assert(new_node->a_array[idx] == NULL);
        new_node->a_array[idx] = child_node;  /* borrow */
        VALIDATE_NODE(state, new_node)
    }
    else {
        /* There's a child node for the given hash.
           Set the key to it./ */

        child_node = map_node_assoc(
            state, node, shift + 5, hash, key, val, added_leaf, mutid);
        if (child_node == NULL) {
            return NULL;
        }
        else if (child_node == (MapNode *)self) {
            NODE_DECREF(state, child_node);
            return (MapNode *)self;
        }

        if (mutid != 0 && self->a_mutid == mutid) {
            new_node = self;
            NODE_INCREF(state, self);
        }
        else {
            new_node = map_node_array_clone(state, self, mutid);
        }

        if (new_node == NULL) {
            NODE_DECREF(state, child_node);
            return NULL;
        }

        NODE_SETREF(state, new_node->a_array[idx], child_node);  /* borrow */
        VALIDATE_NODE(state, new_node)
    }

    return (MapNode *)new_node;
}

static map_without_t
map_node_array_without(module_state *state,
                       MapNode_Array *self,
                       uint32_t shift, int32_t hash,
                       PyObject *key,
                       MapNode **new_node,
                       uint64_t mutid)
{
    uint32_t idx = map_mask(hash, shift);
    MapNode *node = self->a_array[idx];

    if (node == NULL) {
        return W_NOT_FOUND;
    }

    MapNode *sub_node = NULL;
    MapNode_Array *target = NULL;
    map_without_t res = map_node_without(
        state,
        (MapNode *)node,
        shift + 5, hash, key, &sub_node, mutid);

    switch (res) {
        case W_NOT_FOUND:
        case W_ERROR:
            assert(sub_node == NULL);
            return res;

        case W_NEWNODE: {
            /* We need to replace a node at the `idx` index.
               Clone this node and replace.
            */
            assert(sub_node != NULL);

            VALIDATE_NODE(state, sub_node);

            if (mutid != 0 && self->a_mutid == mutid) {
                target = self;
                NODE_INCREF(state, self);
            }
            else {
                target = map_node_array_clone(state, self, mutid);
                if (target == NULL) {
                    NODE_DECREF(state, sub_node);
                    return W_ERROR;
                }
            }

            NODE_SETREF(state, target->a_array[idx], sub_node);  /* borrow */
            *new_node = (MapNode*)target;  /* borrow */
            VALIDATE_NODE(state, target);
            return W_NEWNODE;
        }

        case W_EMPTY: {
            assert(sub_node == NULL);
            /* We need to remove a node at the `idx` index.
               Calculate the size of the replacement Array node.
            */
            Py_ssize_t new_count = self->a_count - 1;

            if (new_count == 0) {
                return W_EMPTY;
            }

            if (new_count >= 16) {
                /* We convert Bitmap nodes to Array nodes, when a
                   Bitmap node needs to store more than 15 key/value
                   pairs.  So we will create a new Array node if we
                   the number of key/values after deletion is still
                   greater than 15.
                */

                if (mutid != 0 && self->a_mutid == mutid) {
                    target = self;
                    NODE_INCREF(state, self);
                }
                else {
                    target = map_node_array_clone(state, self, mutid);
                    if (target == NULL) {
                        return W_ERROR;
                    }
                }

                target->a_count = new_count;
                NODE_CLEAR(state, target->a_array[idx]);

                *new_node = (MapNode*)target;  /* borrow */
                VALIDATE_NODE(state, target);
                return W_NEWNODE;
            }

            /* New Array node would have less than 16 key/value
               pairs.  We need to create a replacement Bitmap node. */

            Py_ssize_t bitmap_size = new_count * 2;
            uint32_t bitmap = 0;

            MapNode_Bitmap *new = (MapNode_Bitmap *)
                map_node_bitmap_new(state, bitmap_size, mutid);
            if (new == NULL) {
                return W_ERROR;
            }

            Py_ssize_t new_i = 0;
            for (uint32_t i = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
                if (i == idx) {
                    /* Skip the node we are deleting. */
                    continue;
                }

                MapNode *node = self->a_array[i];
                if (node == NULL) {
                    /* Skip any missing nodes. */
                    continue;
                }

                bitmap |= 1u << i;

                if (IS_BITMAP_NODE(state, node)) {
                    MapNode_Bitmap *child = (MapNode_Bitmap *)node;

                    if (map_node_bitmap_count(child) == 1 &&
                            child->b_array[0] != NULL)
                    {
                        /* node is a Bitmap with one key/value pair, just
                           merge it into the new Bitmap node we're building.

                           Note that we don't inline Bitmap nodes that
                           have a NULL key -- those nodes point to another
                           tree level, and we cannot simply move tree levels
                           up or down.
                        */
                        PyObject *key = child->b_array[0];
                        PyObject *val = child->b_array[1];

                        if (IS_NODE_LOCAL(state, child)) {
                            INCREF(state, key);
                            new->b_array[new_i] = key;
                            INCREF(state, val);
                            new->b_array[new_i + 1] = val;
                        } else {
                            new->b_array[new_i] = COPY_OBJ(state, key);
                            new->b_array[new_i + 1] = COPY_OBJ(state, val);
                        }
                    }
                    else {
                        new->b_array[new_i] = NULL;
                        NODE_INCREF(state, node);
                        new->b_array[new_i + 1] = (PyObject*)node;
                    }
                }
                else {

#ifdef DEBUG
                    if (IS_COLLISION_NODE(state, node)) {
                        assert(
                            (map_node_collision_count(
                                (MapNode_Collision*)node)) > 1);
                    }
                    else if (IS_ARRAY_NODE(state, node)) {
                        assert(((MapNode_Array*)node)->a_count >= 16);
                    }
#endif

                    /* Just copy the node into our new Bitmap */
                    new->b_array[new_i] = NULL;
                    NODE_INCREF(state, node);
                    new->b_array[new_i + 1] = (PyObject*)node;
                }

                new_i += 2;
            }

            new->b_bitmap = bitmap;
            *new_node = (MapNode*)new;  /* borrow */
            VALIDATE_NODE(state, new);
            return W_NEWNODE;
        }

        default:
            abort();
    }
}

static map_find_t
map_node_array_find(module_state *state,
                    MapNode_Array *self,
                    uint32_t shift, int32_t hash,
                    PyObject *key, PyObject **val)
{
    /* Lookup `key` in the Array node `self`.  Set the value
       for the found key to 'val'. */

    uint32_t idx = map_mask(hash, shift);
    MapNode *node;

    node = self->a_array[idx];
    if (node == NULL) {
        return F_NOT_FOUND;
    }

    /* Dispatch to the generic map_node_find */
    return map_node_find(state, node, shift + 5, hash, key, val);
}

static int
map_node_array_traverse(MapNode_Array *self,
                        visitproc visit, void *arg)
{
    /* Array's tp_traverse */
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    MAYBE_VISIT(state, Py_TYPE(self));

    Py_ssize_t i;
    for (i = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
        MAYBE_VISIT_NODE(state, self->a_array[i]);
    }

    return 0;
}

static void
map_node_array_dealloc(MapNode_Array *self)
{
    /* Array's tp_dealloc */

    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    PyTypeObject *tp = Py_TYPE(self);

    Py_ssize_t i;

    PyObject_GC_UnTrack(self);
    Py_TRASHCAN_BEGIN(self, map_node_array_dealloc)

    for (i = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
        NODE_XDECREF(state, self->a_array[i]);
    }

    tp->tp_free((PyObject *)self);
    Py_TRASHCAN_END

    Py_DecRef((PyObject*)tp);
}

static int
map_node_array_dump(module_state *state,
                    MapNode_Array *node,
                    _PyUnicodeWriter *writer, int level)
{
    /* Debug build: __dump__() method implementation for Array nodes. */

    Py_ssize_t i;

    if (_map_dump_ident(writer, level + 1)) {
        goto error;
    }

    if (_map_dump_format(writer, "ArrayNode(interpreter=%zd id=%p count=%zd):\n",
                         node->interpreter_id, node, node->a_count)
    ) {
        goto error;
    }

    for (i = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
        if (node->a_array[i] == NULL) {
            continue;
        }

        if (_map_dump_ident(writer, level + 2)) {
            goto error;
        }

        if (_map_dump_format(writer, "%d::\n", i)) {
            goto error;
        }

        if (map_node_dump(state, node->a_array[i], writer, level + 1)) {
            goto error;
        }

        if (_map_dump_format(writer, "\n")) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
}


/////////////////////////////////// Node Dispatch


static MapNode *
map_node_assoc(module_state *state,
               MapNode *node,
               uint32_t shift, int32_t hash,
               PyObject *key, PyObject *val, int* added_leaf,
               uint64_t mutid)
{
    /* Set key/value to the 'node' starting with the given shift/hash.
       Return a new node, or the same node if key/value already
       set.

       added_leaf will be set to 1 if key/value wasn't in the
       tree before.

       This method automatically dispatches to the suitable
       map_node_{nodetype}_assoc method.
    */

    *added_leaf = 0;

    if (IS_BITMAP_NODE(state, node)) {
        return map_node_bitmap_assoc(
            state,
            (MapNode_Bitmap *)node,
            shift, hash, key, val, added_leaf, mutid);
    }
    else if (IS_ARRAY_NODE(state, node)) {
        return map_node_array_assoc(
            state,
            (MapNode_Array *)node,
            shift, hash, key, val, added_leaf, mutid);
    }
    else {
        assert(IS_COLLISION_NODE(state, node));
        return map_node_collision_assoc(
            state,
            (MapNode_Collision *)node,
            shift, hash, key, val, added_leaf, mutid);
    }
}

static map_without_t
map_node_without(module_state *state,
                 MapNode *node,
                 uint32_t shift, int32_t hash,
                 PyObject *key,
                 MapNode **new_node,
                 uint64_t mutid)
{
    if (IS_BITMAP_NODE(state, node)) {
        return map_node_bitmap_without(
            state,
            (MapNode_Bitmap *)node,
            shift, hash, key,
            new_node,
            mutid);
    }
    else if (IS_ARRAY_NODE(state, node)) {
        return map_node_array_without(
            state,
            (MapNode_Array *)node,
            shift, hash, key,
            new_node,
            mutid);
    }
    else {
        assert(IS_COLLISION_NODE(state, node));
        return map_node_collision_without(
            state,
            (MapNode_Collision *)node,
            shift, hash, key,
            new_node,
            mutid);
    }
}

static map_find_t
map_node_find(module_state *state,
              MapNode *node,
              uint32_t shift, int32_t hash,
              PyObject *key, PyObject **val)
{
    /* Find the key in the node starting with the given shift/hash.

       If a value is found, the result will be set to F_FOUND, and
       *val will point to the found value object.

       If a value wasn't found, the result will be set to F_NOT_FOUND.

       If an exception occurs during the call, the result will be F_ERROR.

       This method automatically dispatches to the suitable
       map_node_{nodetype}_find method.
    */

    if (IS_BITMAP_NODE(state, node)) {
        return map_node_bitmap_find(
            state,
            (MapNode_Bitmap *)node,
            shift, hash, key, val);

    }
    else if (IS_ARRAY_NODE(state, node)) {
        return map_node_array_find(
            state,
            (MapNode_Array *)node,
            shift, hash, key, val);
    }
    else {
        assert(IS_COLLISION_NODE(state, node));
        return map_node_collision_find(
            state,
            (MapNode_Collision *)node,
            shift, hash, key, val);
    }
}

static int
map_node_dump(module_state *state,
              MapNode *node,
              _PyUnicodeWriter *writer, int level)
{
    /* Debug build: __dump__() method implementation for a node.

       This method automatically dispatches to the suitable
       map_node_{nodetype})_dump method.
    */

    if (IS_BITMAP_NODE(state, node)) {
        return map_node_bitmap_dump(
            state,
            (MapNode_Bitmap *)node, writer, level);
    }
    else if (IS_ARRAY_NODE(state, node)) {
        return map_node_array_dump(
            state,
            (MapNode_Array *)node, writer, level);
    }
    else {
        assert(IS_COLLISION_NODE(state, node));
        return map_node_collision_dump(
            state,
            (MapNode_Collision *)node, writer, level);
    }
}


/////////////////////////////////// Iterators: Machinery


static map_iter_t
map_iterator_next(module_state *state,
                  MapIteratorState *iter,
                  PyObject **n, PyObject **key, PyObject **val);


static void
map_iterator_init(module_state *state, MapIteratorState *iter, MapNode *root)
{
    for (uint32_t i = 0; i < _Py_HAMT_MAX_TREE_DEPTH; i++) {
        iter->i_nodes[i] = NULL;
        iter->i_pos[i] = 0;
    }

    iter->i_level = 0;

    /* Note: we don't incref/decref nodes in i_nodes. */
    iter->i_nodes[0] = root;
}

static map_iter_t
map_iterator_bitmap_next(module_state *state,
                         MapIteratorState *iter,
                         PyObject **n, PyObject **key, PyObject **val)
{
    int8_t level = iter->i_level;

    MapNode_Bitmap *node = (MapNode_Bitmap *)(iter->i_nodes[level]);
    Py_ssize_t pos = iter->i_pos[level];

    if (pos + 1 >= Py_SIZE(node)) {
#ifdef DEBUG
        assert(iter->i_level >= 0);
        iter->i_nodes[iter->i_level] = NULL;
#endif
        iter->i_level--;
        return map_iterator_next(state, iter, n, key, val);
    }

    if (node->b_array[pos] == NULL) {
        iter->i_pos[level] = pos + 2;

        assert(level + 1 < _Py_HAMT_MAX_TREE_DEPTH);
        int8_t next_level = (int8_t)(level + 1);
        iter->i_level = next_level;
        iter->i_pos[next_level] = 0;
        iter->i_nodes[next_level] = (MapNode *)
            node->b_array[pos + 1];

        return map_iterator_next(state, iter, n, key, val);
    }

    *n = (PyObject *)node;
    *key = node->b_array[pos];
    *val = node->b_array[pos + 1];
    iter->i_pos[level] = pos + 2;
    return I_ITEM;
}

static map_iter_t
map_iterator_collision_next(module_state *state,
                            MapIteratorState *iter,
                            PyObject **n, PyObject **key, PyObject **val)
{
    int8_t level = iter->i_level;

    MapNode_Collision *node = (MapNode_Collision *)(iter->i_nodes[level]);
    Py_ssize_t pos = iter->i_pos[level];

    if (pos + 1 >= Py_SIZE(node)) {
#ifdef DEBUG
        assert(iter->i_level >= 0);
        iter->i_nodes[iter->i_level] = NULL;
#endif
        iter->i_level--;
        return map_iterator_next(state, iter, n, key, val);
    }

    *n = (PyObject *)node;
    *key = node->c_array[pos];
    *val = node->c_array[pos + 1];
    iter->i_pos[level] = pos + 2;
    return I_ITEM;
}

static map_iter_t
map_iterator_array_next(module_state *state,
                        MapIteratorState *iter,
                        PyObject **n, PyObject **key, PyObject **val)
{
    int8_t level = iter->i_level;

    MapNode_Array *node = (MapNode_Array *)(iter->i_nodes[level]);
    Py_ssize_t pos = iter->i_pos[level];

    if (pos >= HAMT_ARRAY_NODE_SIZE) {
#ifdef DEBUG
        assert(iter->i_level >= 0);
        iter->i_nodes[iter->i_level] = NULL;
#endif
        iter->i_level--;
        return map_iterator_next(state, iter, n, key, val);
    }

    for (Py_ssize_t i = pos; i < HAMT_ARRAY_NODE_SIZE; i++) {
        if (node->a_array[i] != NULL) {
            iter->i_pos[level] = i + 1;

            assert((level + 1) < _Py_HAMT_MAX_TREE_DEPTH);
            int8_t next_level = (int8_t)(level + 1);
            iter->i_pos[next_level] = 0;
            iter->i_nodes[next_level] = node->a_array[i];
            iter->i_level = next_level;

            return map_iterator_next(state, iter, n, key, val);
        }
    }

#ifdef DEBUG
    assert(iter->i_level >= 0);
    iter->i_nodes[iter->i_level] = NULL;
#endif

    iter->i_level--;
    return map_iterator_next(state, iter, n, key, val);
}

static map_iter_t
map_iterator_next(module_state *state,
                  MapIteratorState *iter,
                  PyObject **n, PyObject **key, PyObject **val)
{
    if (iter->i_level < 0) {
        return I_END;
    }

    assert(iter->i_level < _Py_HAMT_MAX_TREE_DEPTH);

    MapNode *current = iter->i_nodes[iter->i_level];

    if (IS_BITMAP_NODE(state, current)) {
        return map_iterator_bitmap_next(state, iter, n, key, val);
    }
    else if (IS_ARRAY_NODE(state, current)) {
        return map_iterator_array_next(state, iter, n, key, val);
    }
    else {
        assert(IS_COLLISION_NODE(state, current));
        return map_iterator_collision_next(state, iter, n, key, val);
    }
}


/////////////////////////////////// HAMT high-level functions


static MapObject *
map_assoc(module_state *state,
          MapObject *o, PyObject *key, PyObject *val)
{
    int32_t key_hash;
    int added_leaf = 0;
    MapNode *new_root;
    MapObject *new_o;

    TRACK(state, key);
    TRACK(state, val);

    key_hash = map_hash(key);
    if (key_hash == -1) {
        return NULL;
    }

    new_root = map_node_assoc(
        state,
        (MapNode *)(o->h_root),
        0, key_hash, key, val, &added_leaf,
        0);
    if (new_root == NULL) {
        return NULL;
    }

    VALIDATE_NODE(state, new_root);

    if (new_root == o->h_root) {
        NODE_DECREF(state, new_root);
        INCREF(state, o);
        return o;
    }

    new_o = map_alloc(state);
    if (new_o == NULL) {
        NODE_DECREF(state, new_root);
        return NULL;
    }

    new_o->h_root = new_root;  /* borrow */
    new_o->h_count = added_leaf ? o->h_count + 1 : o->h_count;

    return new_o;
}

static MapObject *
map_without(module_state *state, MapObject *o, PyObject *key)
{
    int32_t key_hash = map_hash(key);
    if (key_hash == -1) {
        return NULL;
    }

    MapNode *new_root = NULL;

    map_without_t res = map_node_without(
        state,
        (MapNode *)(o->h_root),
        0, key_hash, key,
        &new_root,
        0);

    switch (res) {
        case W_ERROR:
            return NULL;
        case W_EMPTY:
            return map_new(state);
        case W_NOT_FOUND:
            PyErr_SetObject(PyExc_KeyError, key);
            return NULL;
        case W_NEWNODE: {
            assert(new_root != NULL);

            VALIDATE_NODE(state, new_root);

            MapObject *new_o = map_alloc(state);
            if (new_o == NULL) {
                NODE_DECREF(state, new_root);
                return NULL;
            }

            new_o->h_root = new_root;  /* borrow */
            new_o->h_count = o->h_count - 1;
            assert(new_o->h_count >= 0);
            return new_o;
        }
        default:
            abort();
    }
}

static map_find_t
map_find(module_state *state,
         BaseMapObject *o, PyObject *key, PyObject **val)
{
    if (o->b_count == 0) {
        return F_NOT_FOUND;
    }

    int32_t key_hash = map_hash(key);
    if (key_hash == -1) {
        return F_ERROR;
    }

    return map_node_find(state, o->b_root, 0, key_hash, key, val);
}

static int
map_eq(module_state *state, BaseMapObject *v, BaseMapObject *w)
{
    if (v == w) {
        return 1;
    }

    if (v->b_count != w->b_count) {
        return 0;
    }

    MapIteratorState iter;
    map_iter_t iter_res;
    map_find_t find_res;
    PyObject *_node;
    PyObject *v_key;
    PyObject *v_val;
    PyObject *w_val;

    map_iterator_init(state, &iter, v->b_root);

    do {
        iter_res = map_iterator_next(state, &iter, &_node, &v_key, &v_val);
        if (iter_res == I_ITEM) {
            find_res = map_find(state, w, v_key, &w_val);
            switch (find_res) {
                case F_ERROR:
                    return -1;

                case F_NOT_FOUND:
                    return 0;

                case F_FOUND_EXT:
                case F_FOUND: {
                    int cmp = PyObject_RichCompareBool(v_val, w_val, Py_EQ);
                    if (cmp < 0) {
                        return -1;
                    }
                    if (cmp == 0) {
                        return 0;
                    }
                }
            }
        }
    } while (iter_res != I_END);

    return 1;
}

static Py_ssize_t
map_len(BaseMapObject *o)
{
    return o->b_count;
}

static MapObject *
map_alloc(module_state *state)
{
    MapObject *o;
    o = PyObject_GC_New(MapObject, state->MapType);
    if (o == NULL) {
        return NULL;
    }
    TRACK(state, o);
    #ifndef Py_TPFLAGS_MANAGED_WEAKREF
    o->h_weakreflist = NULL;
    #endif
    o->h_hash = -1;
    o->h_count = 0;
    o->h_root = NULL;
    o->interpreter_id = state->interpreter_id;
    ((ProxyableObject*)o)->proxy_desc = state->proxy_desc_template;
    PyObject_GC_Track(o);
    return o;
}

static MapObject *
map_new(module_state *state)
{
    MapObject *o = map_alloc(state);
    if (o == NULL) {
        return NULL;
    }

    o->h_root = map_node_bitmap_new(state, 0, 0);
    if (o->h_root == NULL) {
        DECREF(state, o);
        return NULL;
    }

    return o;
}

static PyObject *
map_dump(module_state *state, MapObject *self)
{
    _PyUnicodeWriter writer;

    _PyUnicodeWriter_Init(&writer);

    if (_map_dump_format(&writer, "HAMT(len=%zd):\n", self->h_count)) {
        goto error;
    }

    if (map_node_dump(state, self->h_root, &writer, 0)) {
        goto error;
    }

    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;
}


/////////////////////////////////// Iterators: Shared Iterator Implementation


static int
map_baseiter_tp_clear(MapIterator *it)
{
    Py_DecRef((PyObject*)it->mi_obj);
    it->mi_obj = NULL;
    return 0;
}

static void
map_baseiter_tp_dealloc(MapIterator *it)
{
    PyTypeObject *tp = Py_TYPE(it);
    PyObject_GC_UnTrack(it);
    (void)map_baseiter_tp_clear(it);
    PyObject_GC_Del(it);
    Py_DecRef((PyObject*)tp);
}

static int
map_baseiter_tp_traverse(MapIterator *it, visitproc visit, void *arg)
{
    #ifdef DEBUG
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)it);
    USED_IN_DEBUG(state);
    #endif
    MAYBE_VISIT(state, Py_TYPE(it));
    MAYBE_VISIT(state, it->mi_obj);
    return 0;
}

static PyObject *
map_baseiter_tp_iternext(MapIterator *it)
{
    PyObject *node;
    PyObject *key;
    PyObject *val;
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)it);
    map_iter_t res = map_iterator_next(state, &it->mi_iter, &node, &key, &val);

    int need_copy = state->interpreter_id != ((MapNode *)node)->interpreter_id;

    switch (res) {
        case I_END:
            PyErr_SetNone(PyExc_StopIteration);
            return NULL;

        case I_ITEM: {
            return (*(it->mi_yield))(need_copy, state, key, val);
        }

        default: {
            abort();
        }
    }
}

static int
map_baseview_tp_clear(MapView *view)
{
    Py_DecRef((PyObject*)view->mv_obj);
    view->mv_obj = NULL;
    Py_DecRef((PyObject*)view->mv_itertype);
    view->mv_itertype = NULL;
    return 0;
}

static void
map_baseview_tp_dealloc(MapView *view)
{
    PyTypeObject *tp = Py_TYPE(view);
    PyObject_GC_UnTrack(view);
    (void)map_baseview_tp_clear(view);
    PyObject_GC_Del(view);
    Py_DecRef((PyObject*)tp);
}

static int
map_baseview_tp_traverse(MapView *view, visitproc visit, void *arg)
{
    #ifdef DEBUG
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)view);
    USED_IN_DEBUG(state);
    #endif
    MAYBE_VISIT(state, Py_TYPE(view));
    MAYBE_VISIT(state, view->mv_obj);
    return 0;
}

static Py_ssize_t
map_baseview_tp_len(MapView *view)
{
    return view->mv_obj->h_count;
}

static PyObject *
map_baseview_newiter(module_state *state,
                     PyTypeObject *type,
                     iteryield yield,
                     MapObject *map)
{
    MapIterator *iter = PyObject_GC_New(MapIterator, type);
    if (iter == NULL) {
        return NULL;
    }
    TRACK(state, iter);

    iter->interpreter_id = state->interpreter_id;

    INCREF(state, map);
    iter->mi_obj = map;
    iter->mi_yield = yield;
    map_iterator_init(state, &iter->mi_iter, map->h_root);

    PyObject_GC_Track(iter);
    return (PyObject *)iter;
}

static PyObject *
map_baseview_iter(MapView *view)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)view);
    return map_baseview_newiter(
        state, view->mv_itertype, view->mv_yield, view->mv_obj);
}

static PyObject *
map_baseview_new(module_state *state,
                 PyTypeObject *type, iteryield yield,
                 MapObject *o, PyTypeObject *itertype)
{

    MapView *view = PyObject_GC_New(MapView, type);
    if (view == NULL) {
        return NULL;
    }
    TRACK(state, view);

    view->interpreter_id = state->interpreter_id;

    INCREF(state, o);
    view->mv_obj = o;
    view->mv_yield = yield;

    INCREF(state, itertype);
    view->mv_itertype = itertype;

    PyObject_GC_Track(view);
    return (PyObject *)view;
}


#define ITERATOR_TYPE_SHARED_SLOTS                              \
    {Py_tp_dealloc, (destructor)map_baseiter_tp_dealloc},       \
    {Py_tp_traverse, (traverseproc)map_baseiter_tp_traverse},   \
    {Py_tp_clear, (inquiry)map_baseiter_tp_clear},              \
    {Py_tp_iter, PyObject_SelfIter},                            \
    {Py_tp_iternext, (iternextfunc)map_baseiter_tp_iternext},

#define ITERATOR_TYPE_SHARED_SPEC                               \
    .basicsize = sizeof(MapIterator),                           \
    .itemsize = 0,                                              \
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,           \


#define VIEW_TYPE_SHARED_SLOTS                                  \
    {Py_mp_length, (lenfunc)map_baseview_tp_len},               \
    {Py_tp_dealloc, (destructor)map_baseview_tp_dealloc},       \
    {Py_tp_traverse, (traverseproc)map_baseview_tp_traverse},   \
    {Py_tp_clear, (inquiry)map_baseview_tp_clear},              \
    {Py_tp_iter, (getiterfunc)map_baseview_iter},

#define VIEW_TYPE_SHARED_SPEC                                   \
    .basicsize = sizeof(MapView),                               \
    .itemsize = 0,                                              \
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,



/////////////////////////////////// _MapItems_Type


PyType_Slot MapItems_TypeSlots[] = {
    VIEW_TYPE_SHARED_SLOTS
    {0, NULL},
};

PyType_Spec MapItems_TypeSpec = {
    .name = "immutable._map.items",
    VIEW_TYPE_SHARED_SPEC
    .slots = MapItems_TypeSlots,
};

PyType_Slot MapItemsIter_TypeSlots[] = {
    ITERATOR_TYPE_SHARED_SLOTS
    {0, NULL},
};

PyType_Spec MapItemsIter_TypeSpec = {
    .name = "immutable._map.items_iterator",
    ITERATOR_TYPE_SHARED_SPEC
    .slots = MapItemsIter_TypeSlots,
};


static PyObject *
map_iter_yield_items(int need_copy,
                     module_state *state,
                     PyObject *key,
                     PyObject *val)
{
    if (need_copy) {
        key = COPY_OBJ(state, key);
        if (key == NULL) {
            return NULL;
        }
        val = COPY_OBJ(state, val);
        if (val == NULL) {
            return NULL;
        }
    }
    PyObject *ret = PyTuple_Pack(2, key, val);
    if (need_copy) {
        DECREF(state, key);
        DECREF(state, val);
    }
    return ret;
}

static PyObject *
map_new_items_view(module_state *state, MapObject *o)
{
    return map_baseview_new(
        state,
        state->MapItemsType, map_iter_yield_items, o,
        state->MapItemsIterType);
}


/////////////////////////////////// _MapKeys_Type


static int
map_tp_contains(BaseMapObject *self, PyObject *key);

static int
_map_keys_tp_contains(MapView *self, PyObject *key)
{
	return map_tp_contains((BaseMapObject *)self->mv_obj, key);
}


PyType_Slot MapKeys_TypeSlots[] = {
    {Py_sq_contains, (objobjproc)_map_keys_tp_contains},
    VIEW_TYPE_SHARED_SLOTS
    {0, NULL},
};

PyType_Spec MapKeys_TypeSpec = {
    .name = "immutable._map.keys",
    VIEW_TYPE_SHARED_SPEC
    .slots = MapKeys_TypeSlots,
};

PyType_Slot MapKeysIter_TypeSlots[] = {
    ITERATOR_TYPE_SHARED_SLOTS
    {0, NULL},
};

PyType_Spec MapKeysIter_TypeSpec = {
    .name = "immutable._map.keys_iterator",
    ITERATOR_TYPE_SHARED_SPEC
    .slots = MapKeysIter_TypeSlots,
};


static PyObject *
map_iter_yield_keys(int need_copy,
                    module_state *state,
                    PyObject *key,
                    PyObject *val)
{
    if (need_copy) {
        return COPY_OBJ(state, key);
    }
    INCREF(state, key);
    return key;
}

static PyObject *
map_new_keys_iter(module_state *state, MapObject *o)
{
    return map_baseview_newiter(
        state,
        state->MapKeysIterType, map_iter_yield_keys, o);
}

static PyObject *
map_new_keys_view(module_state *state, MapObject *o)
{
    return map_baseview_new(
        state,
        state->MapKeysType, map_iter_yield_keys, o,
        state->MapKeysIterType);
}

/////////////////////////////////// _MapValues_Type


PyType_Slot MapValues_TypeSlots[] = {
    VIEW_TYPE_SHARED_SLOTS
    {0, NULL},
};

PyType_Spec MapValues_TypeSpec = {
    .name = "immutable._map.values",
    VIEW_TYPE_SHARED_SPEC
    .slots = MapValues_TypeSlots,
};

PyType_Slot MapValuesIter_TypeSlots[] = {
    ITERATOR_TYPE_SHARED_SLOTS
    {0, NULL},
};

PyType_Spec MapValuesIter_TypeSpec = {
    .name = "immutable._map.values_iterator",
    ITERATOR_TYPE_SHARED_SPEC
    .slots = MapValuesIter_TypeSlots,
};


static PyObject *
map_iter_yield_values(int need_copy,
                      module_state *state,
                      PyObject *key,
                      PyObject *val)
{
    if (need_copy) {
        return COPY_OBJ(state, val);
    }
    INCREF(state, val);
    return val;
}

static PyObject *
map_new_values_view(module_state *state, MapObject *o)
{
    return map_baseview_new(
        state,
        state->MapValuesType, map_iter_yield_values, o,
        state->MapValuesIterType);
}


/////////////////////////////////// _Map_Type


static PyObject *
map_dump(module_state *state, MapObject *self);


static PyObject *
map_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    module_state *state = MemHive_GetModuleStateByType(type);
    return (PyObject*)map_new(state);
}


static int
map_tp_init(MapObject *self, PyObject *args, PyObject *kwds)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    PyObject *arg = NULL;
    uint64_t mutid = 0;

    if (!PyArg_UnpackTuple(args, TYPENAME_MAP, 0, 1, &arg)) {
        return -1;
    }

    if (arg != NULL) {
        if (Map_Check(state, arg)) {
            MapObject *other = (MapObject *)arg;

            NODE_INCREF(state, other->h_root);
            NODE_SETREF(state, self->h_root, other->h_root);

            self->h_count = other->h_count;
            self->h_hash = other->h_hash;
        }
        else if (MapMutation_Check(state, arg)) {
            PyErr_Format(
                PyExc_TypeError,
                "cannot create Maps from MapMutations");
            return -1;
        }
        else {
            mutid = new_mutid(state);
            if (map_update_inplace(state, mutid, (BaseMapObject *)self, arg)) {
                return -1;
            }
        }
    }

    if (kwds != NULL) {
        if (!PyArg_ValidateKeywordArguments(kwds)) {
            return -1;
        }

        if (!mutid) {
            mutid = new_mutid(state);
        }

        if (map_update_inplace(state, mutid, (BaseMapObject *)self, kwds)) {
            return -1;
        }
    }

    return 0;
}


static int
map_tp_clear(BaseMapObject *self)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    NODE_CLEAR(state, self->b_root);
    return 0;
}


static int
map_tp_traverse(BaseMapObject *self, visitproc visit, void *arg)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    MAYBE_VISIT(state, Py_TYPE(self));
    MAYBE_VISIT_NODE(state, self->b_root);
    return 0;
}

static void
map_tp_dealloc(BaseMapObject *self)
{
    PyTypeObject *tp = Py_TYPE(self);
    PyObject_GC_UnTrack(self);
    PyObject_ClearWeakRefs((PyObject*)self);
    (void)map_tp_clear(self);
    Py_TYPE(self)->tp_free(self);
    Py_DecRef((PyObject*)tp);
}


static PyObject *
map_tp_richcompare(PyObject *v, PyObject *w, int op)
{
    module_state *state = MemHive_GetModuleStateByObj(v);
    if (!Map_Check(state, v) ||
        !Map_Check(state, w) ||
        (op != Py_EQ && op != Py_NE)
    ) {
        Py_RETURN_NOTIMPLEMENTED;
    }

    int res = map_eq(state, (BaseMapObject *)v, (BaseMapObject *)w);
    if (res < 0) {
        return NULL;
    }

    if (op == Py_NE) {
        res = !res;
    }

    if (res) {
        Py_RETURN_TRUE;
    }
    else {
        Py_RETURN_FALSE;
    }
}

static int
map_tp_contains(BaseMapObject *self, PyObject *key)
{
    PyObject *val;
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    map_find_t res = map_find(state, self, key, &val);
    switch (res) {
        case F_ERROR:
            return -1;
        case F_NOT_FOUND:
            return 0;
        case F_FOUND_EXT:
        case F_FOUND:
            return 1;
        default:
            abort();
    }
}

static PyObject *
map_tp_subscript(BaseMapObject *self, PyObject *key)
{
    PyObject *val;
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    map_find_t res = map_find(state, self, key, &val);

    switch (res) {
        case F_ERROR:
            return NULL;
        case F_FOUND:
            INCREF(state, val);
            return val;
        case F_FOUND_EXT:
            return COPY_OBJ(state, val);
        case F_NOT_FOUND:
            PyErr_SetObject(PyExc_KeyError, key);
            return NULL;
        default:
            abort();
    }
}

static Py_ssize_t
map_tp_len(BaseMapObject *self)
{
    return map_len(self);
}

static PyObject *
map_tp_iter(MapObject *self)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    return map_new_keys_iter(state, self);
}

static PyObject *
map_py_set(MapObject *self, PyObject *args)
{
    PyObject *key;
    PyObject *val;
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    if (self->interpreter_id != state->interpreter_id) {
        PyErr_SetString(
            PyExc_RuntimeError,
            "can't set values from another interpreter");
        return NULL;
    }

    if (!PyArg_UnpackTuple(args, "set", 2, 2, &key, &val)) {
        return NULL;
    }

    return (PyObject *)map_assoc(state, self, key, val);
}

static PyObject *
map_get(module_state *state, BaseMapObject *self, PyObject *key, PyObject *def)
{
    PyObject *val = NULL;
    map_find_t res = map_find(state, self, key, &val);
    switch (res) {
        case F_ERROR:
            return NULL;
        case F_FOUND:
            INCREF(state, val);
            return val;
        case F_FOUND_EXT:
            return COPY_OBJ(state, val);
        case F_NOT_FOUND:
            if (def == NULL) {
                Py_RETURN_NONE;
            }
            INCREF(state, def);
            return def;
        default:
            abort();
    }
}

static PyObject *
map_py_get(BaseMapObject *self, PyObject *args)
{
    PyObject *key;
    PyObject *def = NULL;

    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    if (!PyArg_UnpackTuple(args, "get", 1, 2, &key, &def)) {
        return NULL;
    }

    return map_get(state, self, key, def);
}

static PyObject *
map_py_delete(MapObject *self, PyObject *key)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    return (PyObject *)map_without(state, self, key);
}

static PyObject *
map_py_mutate(MapObject *self, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    MapMutationObject *o;
    o = PyObject_GC_New(MapMutationObject, state->MapMutationType);
    if (o == NULL) {
        return NULL;
    }
    TRACK(state, o);
    #ifndef Py_TPFLAGS_MANAGED_WEAKREF
    o->m_weakreflist = NULL;
    #endif
    o->m_count = self->h_count;
    o->interpreter_id = state->interpreter_id;
    ((ProxyableObject*)o)->proxy_desc = NULL;

    NODE_INCREF(state, self->h_root);
    o->m_root = self->h_root;

    o->m_mutid = new_mutid(state);

    PyObject_GC_Track(o);
    return (PyObject *)o;
}

static PyObject *
map_py_update(MapObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL;
    MapObject *new = NULL;
    uint64_t mutid = 0;
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    if (!PyArg_UnpackTuple(args, "update", 0, 1, &arg)) {
        return NULL;
    }

    if (arg != NULL) {
        mutid = new_mutid(state);
        new = map_update(state, mutid, self, arg);
        if (new == NULL) {
            return NULL;
        }
    }
    else {
        INCREF(state, self);
        new = self;
    }

    if (kwds != NULL) {
        if (!PyArg_ValidateKeywordArguments(kwds)) {
            DECREF(state, new);
            return NULL;
        }

        if (!mutid) {
            mutid = new_mutid(state);
        }

        MapObject *new2 = map_update(state, mutid, new, kwds);
        DECREF(state, new);
        if (new2 == NULL) {
            return NULL;
        }
        new = new2;
    }

    return (PyObject *)new;
}

static PyObject *
map_py_items(MapObject *self, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    return map_new_items_view(state, self);
}

static PyObject *
map_py_values(MapObject *self, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    return map_new_values_view(state, self);
}

static PyObject *
map_py_keys(MapObject *self, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    return map_new_keys_view(state, self);
}

static PyObject *
map_py_dump(MapObject *self, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    return map_dump(state, self);
}


static PyObject *
map_py_repr(BaseMapObject *m)
{
    Py_ssize_t i;
    _PyUnicodeWriter writer;

    module_state *state = MemHive_GetModuleStateByObj((PyObject *)m);


    i = Py_ReprEnter((PyObject *)m);
    if (i != 0) {
        return i > 0 ? PyUnicode_FromString("{...}") : NULL;
    }

    _PyUnicodeWriter_Init(&writer);

    if (MapMutation_Check(state, m)) {
        if (_PyUnicodeWriter_WriteASCIIString(
                &writer, "memhive.MapMutation({", 21) < 0)
        {
            goto error;
        }
    }
    else {
        if (_PyUnicodeWriter_WriteASCIIString(
                &writer, "memhive.Map({", 13) < 0)
        {
            goto error;
        }
    }

    MapIteratorState iter;
    map_iter_t iter_res;
    map_iterator_init(state, &iter, m->b_root);
    int second = 0;
    do {
        PyObject *_node;
        PyObject *v_key;
        PyObject *v_val;

        iter_res = map_iterator_next(state, &iter, &_node, &v_key, &v_val);
        if (iter_res == I_ITEM) {
            if (second) {
                if (_PyUnicodeWriter_WriteASCIIString(&writer, ", ", 2) < 0) {
                    goto error;
                }
            }

            PyObject *s = PyObject_Repr(v_key);
            if (s == NULL) {
                goto error;
            }
            TRACK(state, s);
            if (_PyUnicodeWriter_WriteStr(&writer, s) < 0) {
                DECREF(state, s);
                goto error;
            }
            DECREF(state, s);

            if (_PyUnicodeWriter_WriteASCIIString(&writer, ": ", 2) < 0) {
                goto error;
            }

            s = PyObject_Repr(v_val);
            if (s == NULL) {
                goto error;
            }
            TRACK(state, s);
            if (_PyUnicodeWriter_WriteStr(&writer, s) < 0) {
                DECREF(state, s);
                goto error;
            }
            DECREF(state, s);
        }

        second = 1;
    } while (iter_res != I_END);

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "})", 2) < 0) {
        goto error;
    }

    Py_ReprLeave((PyObject *)m);
    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    Py_ReprLeave((PyObject *)m);
    return NULL;
}


static Py_uhash_t
_shuffle_bits(Py_uhash_t h)
{
    return ((h ^ 89869747UL) ^ (h << 16)) * 3644798167UL;
}


static Py_hash_t
map_py_hash(MapObject *self)
{
    /* Adapted version of frozenset.__hash__: it's important
       that Map.__hash__ is independant of key/values order.

       Optimization idea: compute and memoize intermediate
       hash values for HAMT nodes.
    */

    if (self->h_hash != -1) {
        return self->h_hash;
    }

    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    Py_uhash_t hash = 0;

    MapIteratorState iter;
    map_iter_t iter_res;
    map_iterator_init(state, &iter, self->h_root);
    do {
        PyObject *_node;
        PyObject *v_key;
        PyObject *v_val;

        iter_res = map_iterator_next(state, &iter, &_node, &v_key, &v_val);
        if (iter_res == I_ITEM) {
            Py_hash_t vh = PyObject_Hash(v_key);
            if (vh == -1) {
                return -1;
            }
            hash ^= _shuffle_bits((Py_uhash_t)vh);

            vh = PyObject_Hash(v_val);
            if (vh == -1) {
                return -1;
            }
            hash ^= _shuffle_bits((Py_uhash_t)vh);
        }
    } while (iter_res != I_END);

    hash ^= ((Py_uhash_t)self->h_count * 2 + 1) * 1927868237UL;

    hash ^= (hash >> 11) ^ (hash >> 25);
    hash = hash * 69069U + 907133923UL;

    self->h_hash = (Py_hash_t)hash;
    if (self->h_hash == -1) {
        self->h_hash = 1;
    }
    return self->h_hash;
}

static PyObject *
map_reduce(MapObject *self)
{
    MapIteratorState iter;
    map_iter_t iter_res;

    PyObject *dict = PyDict_New();
    if (dict == NULL) {
        return NULL;
    }

    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);
    TRACK(state, dict);

    map_iterator_init(state, &iter, self->h_root);
    do {
        PyObject *node;
        PyObject *key;
        PyObject *val;

        iter_res = map_iterator_next(state, &iter, &node, &key, &val);
        if (iter_res == I_ITEM) {
            int need_copy = (
                state->interpreter_id != ((MapNode *)node)->interpreter_id
            );

            if (need_copy) {
                PyObject *key_copy = COPY_OBJ(state, key);
                if (key_copy == NULL) {
                    goto err;
                }
                PyObject *val_copy = COPY_OBJ(state, val);
                if (key_copy == NULL) {
                    CLEAR(state, key_copy);
                    goto err;
                }
                int ret = PyDict_SetItem(dict, key_copy, val_copy);
                CLEAR(state, key_copy);
                CLEAR(state, val_copy);
                if (ret < 0) {
                    goto err;
                }
            } else {
                if (PyDict_SetItem(dict, key, val) < 0) {
                    goto err;
                }
            }
        }
    } while (iter_res != I_END);

    PyObject *args = PyTuple_Pack(1, dict);
    CLEAR(state, dict);
    TRACK(state, args);
    if (args == NULL) {
        return NULL;
    }

    PyObject *tup = PyTuple_Pack(2, Py_TYPE(self), args);
    DECREF(state, args);
    return tup;

err:
    CLEAR(state, dict);
    return NULL;
}

#if PY_VERSION_HEX < 0x030900A6
static PyObject *
map_py_class_getitem(PyObject *type, PyObject *item)
{
    Py_IncRef(type);
    return type;
}
#endif

static PyMethodDef Map_methods[] = {
    {"set", (PyCFunction)map_py_set, METH_VARARGS, NULL},
    {"get", (PyCFunction)map_py_get, METH_VARARGS, NULL},
    {"delete", (PyCFunction)map_py_delete, METH_O, NULL},
    {"mutate", (PyCFunction)map_py_mutate, METH_NOARGS, NULL},
    {"items", (PyCFunction)map_py_items, METH_NOARGS, NULL},
    {"keys", (PyCFunction)map_py_keys, METH_NOARGS, NULL},
    {"values", (PyCFunction)map_py_values, METH_NOARGS, NULL},
    {"update", (PyCFunction)map_py_update, METH_VARARGS | METH_KEYWORDS, NULL},
    {"__reduce__", (PyCFunction)map_reduce, METH_NOARGS, NULL},
    {"__dump__", (PyCFunction)map_py_dump, METH_NOARGS, NULL},
    {
        "__class_getitem__",
#if PY_VERSION_HEX < 0x030900A6
        (PyCFunction)map_py_class_getitem,
#else
        Py_GenericAlias,
#endif
        METH_O|METH_CLASS,
        "See PEP 585"
    },
    {NULL, NULL}
};


#ifndef Py_TPFLAGS_MANAGED_WEAKREF
static PyMemberDef Map_members[] = {
    {"__weaklistoffset__",
        T_PYSSIZET, offsetof(MapObject, h_weakreflist), READONLY},
    {NULL},
};
#endif


PyType_Slot Map_TypeSlots[] = {
    {Py_mp_length, (lenfunc)map_tp_len},
    {Py_mp_subscript, (binaryfunc)map_tp_subscript},
    {Py_sq_contains, (objobjproc)map_tp_contains},
    {Py_tp_methods, Map_methods},
    #ifndef Py_TPFLAGS_MANAGED_WEAKREF
    {Py_tp_members, Map_members},
    #endif
    {Py_tp_iter, (getiterfunc)map_tp_iter},
    {Py_tp_dealloc, (destructor)map_tp_dealloc},
    {Py_tp_richcompare, map_tp_richcompare},
    {Py_tp_traverse, (traverseproc)map_tp_traverse},
    {Py_tp_clear, (inquiry)map_tp_clear},
    {Py_tp_new, map_tp_new},
    {Py_tp_init, (initproc)map_tp_init},
    {Py_tp_hash, (hashfunc)map_py_hash},
    {Py_tp_repr, (reprfunc)map_py_repr},
    {0, NULL},
};


PyType_Spec Map_TypeSpec = {
    .name = TYPENAME_MAP,
    .basicsize = sizeof(MapObject),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC
    #ifdef Py_TPFLAGS_MAPPING
        | Py_TPFLAGS_MAPPING
    #endif
    #ifdef Py_TPFLAGS_MANAGED_WEAKREF
        | Py_TPFLAGS_MANAGED_WEAKREF
    #endif
        | MEMHIVE_TPFLAGS_PROXYABLE
    ,
    .slots = Map_TypeSlots,
};


/////////////////////////////////// MapMutation


static int
map_node_update_from_map(module_state *state,
                         uint64_t mutid,
                         MapObject *map,
                         MapNode *root, Py_ssize_t count,
                         MapNode **new_root, Py_ssize_t *new_count)
{
    MapIteratorState iter;
    map_iter_t iter_res;

    MapNode *last_root;
    Py_ssize_t last_count;

    NODE_INCREF(state, root);
    last_root = root;
    last_count = count;

    map_iterator_init(state, &iter, map->h_root);
    do {
        PyObject *_node;
        PyObject *key;
        PyObject *val;
        int32_t key_hash;
        int added_leaf;

        iter_res = map_iterator_next(state, &iter, &_node, &key, &val);
        if (iter_res == I_ITEM) {
            TRACK(state, key);
            TRACK(state, val);

            key_hash = map_hash(key);
            if (key_hash == -1) {
                goto err;
            }

            MapNode *iter_root = map_node_assoc(
                state,
                last_root,
                0, key_hash, key, val, &added_leaf,
                mutid);

            if (iter_root == NULL) {
                goto err;
            }

            if (added_leaf) {
                last_count++;
            }

            NODE_SETREF(state, last_root, iter_root);
        }
    } while (iter_res != I_END);

    *new_root = last_root;
    *new_count = last_count;

    return 0;

err:
    NODE_DECREF(state, last_root);
    return -1;
}


static int
map_node_update_from_dict(module_state *state,
                          uint64_t mutid,
                          PyObject *dct,
                          MapNode *root, Py_ssize_t count,
                          MapNode **new_root, Py_ssize_t *new_count)
{
    assert(PyDict_Check(dct));

    PyObject *it = PyObject_GetIter(dct);
    if (it == NULL) {
        return -1;
    }
    TRACK(state, it);

    MapNode *last_root;
    Py_ssize_t last_count;

    NODE_INCREF(state, root);
    last_root = root;
    last_count = count;

    PyObject *key;

    while ((key = PyIter_Next(it))) {
        PyObject *val;
        int added_leaf;
        int32_t key_hash;

        key_hash = map_hash(key);
        if (key_hash == -1) {
            DECREF(state, key);
            goto err;
        }

        val = PyDict_GetItemWithError(dct, key);
        if (val == NULL) {
            DECREF(state, key);
            goto err;
        }

        TRACK(state, key);
        TRACK(state, val);

        MapNode *iter_root = map_node_assoc(
            state,
            last_root,
            0, key_hash, key, val, &added_leaf,
            mutid);

        DECREF(state, key);

        if (iter_root == NULL) {
            goto err;
        }

        if (added_leaf) {
            last_count++;
        }

        NODE_SETREF(state, last_root, iter_root);
    }

    if (key == NULL && PyErr_Occurred()) {
        goto err;
    }

    DECREF(state, it);

    *new_root = last_root;
    *new_count = last_count;

    return 0;

err:
    DECREF(state, it);
    NODE_DECREF(state, last_root);
    return -1;
}


static int
map_node_update_from_seq(module_state *state,
                         uint64_t mutid,
                         PyObject *seq,
                         MapNode *root, Py_ssize_t count,
                         MapNode **new_root, Py_ssize_t *new_count)
{
    PyObject *it;
    Py_ssize_t i;
    PyObject *item = NULL;
    PyObject *fast = NULL;

    MapNode *last_root;
    Py_ssize_t last_count;

    it = PyObject_GetIter(seq);
    if (it == NULL) {
        return -1;
    }

    NODE_INCREF(state, root);
    last_root = root;
    last_count = count;

    for (i = 0; ; i++) {
        PyObject *key, *val;
        Py_ssize_t n;
        int32_t key_hash;
        int added_leaf;

        item = PyIter_Next(it);
        if (item == NULL) {
            if (PyErr_Occurred()) {
                goto err;
            }
            break;
        }

        fast = PySequence_Fast(item, "");
        if (fast == NULL) {
            if (PyErr_ExceptionMatches(PyExc_TypeError))
                PyErr_Format(PyExc_TypeError,
                    "cannot convert map update "
                    "sequence element #%zd to a sequence",
                    i);
            goto err;
        }

        n = PySequence_Fast_GET_SIZE(fast);
        if (n != 2) {
            PyErr_Format(PyExc_ValueError,
                         "map update sequence element #%zd "
                         "has length %zd; 2 is required",
                         i, n);
            goto err;
        }

        key = PySequence_Fast_GET_ITEM(fast, 0);
        val = PySequence_Fast_GET_ITEM(fast, 1);
        INCREF(state, key);
        INCREF(state, val);

        TRACK(state, key);
        TRACK(state, val);

        key_hash = map_hash(key);
        if (key_hash == -1) {
            DECREF(state, key);
            DECREF(state, val);
            goto err;
        }

        MapNode *iter_root = map_node_assoc(
            state,
            last_root,
            0, key_hash, key, val, &added_leaf,
            mutid);

        DECREF(state, key);
        DECREF(state, val);

        if (iter_root == NULL) {
            goto err;
        }

        if (added_leaf) {
            last_count++;
        }

        NODE_SETREF(state, last_root, iter_root);

        DECREF(state, fast);
        DECREF(state, item);
    }

    DECREF(state, it);

    *new_root = last_root;
    *new_count = last_count;

    return 0;

err:
    NODE_DECREF(state, last_root);
    XDECREF(state, item);
    XDECREF(state, fast);
    DECREF(state, it);
    return -1;
}


static int
map_node_update(module_state *state,
                uint64_t mutid,
                PyObject *src,
                MapNode *root, Py_ssize_t count,
                MapNode **new_root, Py_ssize_t *new_count)
{
    if (Map_Check(state, src)) {
        return map_node_update_from_map(
            state,
            mutid, (MapObject *)src, root, count, new_root, new_count);
    }
    else if (PyDict_Check(src)) {
        return map_node_update_from_dict(
            state,
            mutid, src, root, count, new_root, new_count);
    }
    else {
        return map_node_update_from_seq(
            state,
            mutid, src, root, count, new_root, new_count);
    }
}


static int
map_update_inplace(module_state *state,
                   uint64_t mutid, BaseMapObject *o, PyObject *src)
{
    MapNode *new_root = NULL;
    Py_ssize_t new_count;

    int ret = map_node_update(
        state,
        mutid, src,
        o->b_root, o->b_count,
        &new_root, &new_count);

    if (ret) {
        return -1;
    }

    assert(new_root);

    NODE_SETREF(state, o->b_root, new_root);
    o->b_count = new_count;

    return 0;
}


static MapObject *
map_update(module_state *state,
           uint64_t mutid, MapObject *o, PyObject *src)
{
    MapNode *new_root = NULL;
    Py_ssize_t new_count;

    int ret = map_node_update(
        state,
        mutid, src,
        o->h_root, o->h_count,
        &new_root, &new_count);

    if (ret) {
        return NULL;
    }

    assert(new_root);

    MapObject *new = map_alloc(state);
    if (new == NULL) {
        NODE_DECREF(state, new_root);
        return NULL;
    }

    NODE_XSETREF(state, new->h_root, new_root);
    new->h_count = new_count;

    return new;
}

static int
mapmut_check_finalized(MapMutationObject *o)
{
    if (o->m_mutid == 0) {
        PyErr_Format(
            PyExc_ValueError,
            "mutation %R has been finished",
            o, NULL);
        return -1;
    }

    return 0;
}

static int
mapmut_delete(module_state *state,
              MapMutationObject *o, PyObject *key, int32_t key_hash)
{
    MapNode *new_root = NULL;

    assert(key_hash != -1);
    map_without_t res = map_node_without(
        state,
        (MapNode *)(o->m_root),
        0, key_hash, key,
        &new_root,
        o->m_mutid);

    switch (res) {
        case W_ERROR:
            return -1;

        case W_EMPTY:
            new_root = map_node_bitmap_new(state, 0, o->m_mutid);
            if (new_root == NULL) {
                return -1;
            }
            NODE_SETREF(state, o->m_root, new_root);
            o->m_count = 0;
            return 0;

        case W_NOT_FOUND:
            PyErr_SetObject(PyExc_KeyError, key);
            return -1;

        case W_NEWNODE: {
            assert(new_root != NULL);
            NODE_SETREF(state, o->m_root, new_root);
            o->m_count--;
            return 0;
        }

        default:
            abort();
    }
}

static int
mapmut_set(module_state *state,
           MapMutationObject *o, PyObject *key, int32_t key_hash,
           PyObject *val)
{
    int added_leaf = 0;

    TRACK(state, key);
    TRACK(state, val);

    assert(key_hash != -1);
    MapNode *new_root = map_node_assoc(
        state,
        (MapNode *)(o->m_root),
        0, key_hash, key, val, &added_leaf,
        o->m_mutid);
    if (new_root == NULL) {
        return -1;
    }

    if (added_leaf) {
        o->m_count++;
    }

    if (new_root == o->m_root) {
        NODE_DECREF(state, new_root);
        return 0;
    }

    NODE_SETREF(state, o->m_root, new_root);
    return 0;
}

static int
mapmut_finish(MapMutationObject *o)
{
    o->m_mutid = 0;
    return 0;
}

static PyObject *
mapmut_py_set(MapMutationObject *o, PyObject *args)
{
    PyObject *key;
    PyObject *val;

    module_state *state = MemHive_GetModuleStateByObj((PyObject *)o);

    if (!PyArg_UnpackTuple(args, "set", 2, 2, &key, &val)) {
        return NULL;
    }

    if (mapmut_check_finalized(o)) {
        return NULL;
    }

    int32_t key_hash = map_hash(key);
    if (key_hash == -1) {
        return NULL;
    }

    if (mapmut_set(state, o, key, key_hash, val)) {
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *
mapmut_tp_richcompare(PyObject *v, PyObject *w, int op)
{
    module_state *state = MemHive_GetModuleStateByObj(v);
    if (!MapMutation_Check(state, v) || !MapMutation_Check(state, w) ||
            (op != Py_EQ && op != Py_NE))
    {
        Py_RETURN_NOTIMPLEMENTED;
    }

    int res = map_eq(state, (BaseMapObject *)v, (BaseMapObject *)w);
    if (res < 0) {
        return NULL;
    }

    if (op == Py_NE) {
        res = !res;
    }

    if (res) {
        Py_RETURN_TRUE;
    }
    else {
        Py_RETURN_FALSE;
    }
}

static PyObject *
mapmut_py_update(MapMutationObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *arg = NULL;
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    if (!PyArg_UnpackTuple(args, "update", 0, 1, &arg)) {
        return NULL;
    }

    if (mapmut_check_finalized(self)) {
        return NULL;
    }

    if (arg != NULL) {
        if (map_update_inplace(state, self->m_mutid,
                               (BaseMapObject *)self, arg))
        {
            return NULL;
        }
    }

    if (kwds != NULL) {
        if (!PyArg_ValidateKeywordArguments(kwds)) {
            return NULL;
        }

        if (map_update_inplace(state, self->m_mutid,
                               (BaseMapObject *)self, kwds))
        {
            return NULL;
        }
    }

    Py_RETURN_NONE;
}


static PyObject *
mapmut_py_finish(MapMutationObject *self, PyObject *args)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    if (mapmut_finish(self)) {
        return NULL;
    }

    MapObject *o = map_alloc(state);
    if (o == NULL) {
        return NULL;
    }

    NODE_INCREF(state, self->m_root);
    o->h_root = self->m_root;
    o->h_count = self->m_count;

    return (PyObject *)o;
}

static PyObject *
mapmut_py_enter(MapMutationObject *self, PyObject *args)
{
    Py_IncRef((PyObject*)self);
    return (PyObject *)self;
}

static PyObject *
mapmut_py_exit(MapMutationObject *self, PyObject *args)
{
    if (mapmut_finish(self)) {
        return NULL;
    }
    Py_RETURN_FALSE;
}

static int
mapmut_tp_ass_sub(MapMutationObject *self, PyObject *key, PyObject *val)
{
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    if (mapmut_check_finalized(self)) {
        return -1;
    }

    int32_t key_hash = map_hash(key);
    if (key_hash == -1) {
        return -1;
    }

    if (val == NULL) {
        return mapmut_delete(state, self, key, key_hash);
    }
    else {
        return mapmut_set(state, self, key, key_hash, val);
    }
}

static PyObject *
mapmut_py_pop(MapMutationObject *self, PyObject *args)
{
    PyObject *key, *deflt = NULL, *val = NULL;
    module_state *state = MemHive_GetModuleStateByObj((PyObject *)self);

    if(!PyArg_UnpackTuple(args, "pop", 1, 2, &key, &deflt)) {
        return NULL;
    }

    if (mapmut_check_finalized(self)) {
        return NULL;
    }

    if (!self->m_count) {
        goto not_found;
    }

    int32_t key_hash = map_hash(key);
    if (key_hash == -1) {
        return NULL;
    }

    map_find_t find_res = map_node_find(
        state, self->m_root, 0, key_hash, key, &val);

    switch (find_res) {
        case F_ERROR:
            return NULL;

        case F_NOT_FOUND:
            goto not_found;

        case F_FOUND:
            break;

        default:
            abort();
    }

    INCREF(state, val);

    if (mapmut_delete(state, self, key, key_hash)) {
        DECREF(state, val);
        return NULL;
    }

    return val;

not_found:
    if (deflt) {
        INCREF(state, deflt);
        return deflt;
    }

    PyErr_SetObject(PyExc_KeyError, key);
    return NULL;
}


static PyMethodDef MapMutation_methods[] = {
    {"set", (PyCFunction)mapmut_py_set, METH_VARARGS, NULL},
    {"get", (PyCFunction)map_py_get, METH_VARARGS, NULL},
    {"pop", (PyCFunction)mapmut_py_pop, METH_VARARGS, NULL},
    {"finish", (PyCFunction)mapmut_py_finish, METH_NOARGS, NULL},
    {"update", (PyCFunction)mapmut_py_update,
        METH_VARARGS | METH_KEYWORDS, NULL},
    {"__enter__", (PyCFunction)mapmut_py_enter, METH_NOARGS, NULL},
    {"__exit__", (PyCFunction)mapmut_py_exit, METH_VARARGS, NULL},
    {NULL, NULL}
};


#ifndef Py_TPFLAGS_MANAGED_WEAKREF
static PyMemberDef MapMutation_members[] = {
    {"__weaklistoffset__",
        T_PYSSIZET, offsetof(MapMutationObject, m_weakreflist), READONLY},
    {NULL},
};
#endif


PyType_Slot MapMutation_TypeSlots[] = {
    {Py_mp_length, (lenfunc)map_tp_len},
    {Py_mp_subscript, (binaryfunc)map_tp_subscript},
    {Py_mp_ass_subscript, (objobjargproc)mapmut_tp_ass_sub},
    {Py_sq_contains, (objobjproc)map_tp_contains},
    {Py_tp_methods, MapMutation_methods},
    #ifndef Py_TPFLAGS_MANAGED_WEAKREF
    {Py_tp_members, MapMutation_members},
    #endif
    {Py_tp_dealloc, (destructor)map_tp_dealloc},
    {Py_tp_richcompare, mapmut_tp_richcompare},
    {Py_tp_traverse, (traverseproc)map_tp_traverse},
    {Py_tp_clear, (inquiry)map_tp_clear},
    {Py_tp_hash, PyObject_HashNotImplemented},
    {Py_tp_repr, (reprfunc)map_py_repr},
    {0, NULL},
};



PyType_Spec MapMutation_TypeSpec = {
    .name = TYPENAME_MAPMUT,
    .basicsize = sizeof(MapMutationObject),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC
    #ifdef Py_TPFLAGS_MANAGED_WEAKREF
        | Py_TPFLAGS_MANAGED_WEAKREF
    #endif
    ,
    .slots = MapMutation_TypeSlots,
};


/////////////////////////////////// Tree Node Types


PyType_Slot ArrayNode_TypeSlots[] = {
    {Py_tp_dealloc, (destructor)map_node_array_dealloc},
    {Py_tp_traverse, (traverseproc)map_node_array_traverse},
    {Py_tp_free, PyObject_GC_Del},
    {Py_tp_hash, PyObject_HashNotImplemented},
    {0, NULL},
};

PyType_Spec ArrayNode_TypeSpec = {
    .name = TYPENAME_ARRAY_NODE,
    .basicsize = sizeof(MapNode_Array),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = ArrayNode_TypeSlots,
};


PyType_Slot BitmapNode_TypeSlots[] = {
    {Py_tp_dealloc, (destructor)map_node_bitmap_dealloc},
    {Py_tp_traverse, (traverseproc)map_node_bitmap_traverse},
    {Py_tp_free, PyObject_GC_Del},
    {Py_tp_hash, PyObject_HashNotImplemented},
    {0, NULL},
};

PyType_Spec BitmapNode_TypeSpec = {
    .name = TYPENAME_BITMAP_NODE,
    .basicsize = sizeof(MapNode_Bitmap) - sizeof(PyObject *),
    .itemsize = sizeof(PyObject *),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = BitmapNode_TypeSlots,
};


PyType_Slot CollisionNode_TypeSlots[] = {
    {Py_tp_dealloc, (destructor)map_node_collision_dealloc},
    {Py_tp_traverse, (traverseproc)map_node_collision_traverse},
    {Py_tp_free, PyObject_GC_Del},
    {Py_tp_hash, PyObject_HashNotImplemented},
    {0, NULL},
};

PyType_Spec CollisionNode_TypeSpec = {
    .name = TYPENAME_COLLISION_NODE,
    .basicsize = sizeof(MapNode_Collision) - sizeof(PyObject *),
    .itemsize = sizeof(PyObject *),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = CollisionNode_TypeSlots,
};


PyObject *
MemHive_NewMap(module_state *state)
{
    return (PyObject *)map_new(state);
}


PyObject *
MemHive_NewMapProxy(module_state *state, PyObject *map)
{
    MapObject *o = map_alloc(state);
    if (o == NULL) {
        return NULL;
    }

    // here goes nothing...
    o->h_root = ((MapObject *)map)->h_root;
    o->h_count = ((MapObject *)map)->h_count;

    // What are we even doing here otherwise
    assert(state->interpreter_id != o->h_root->interpreter_id);

    NODE_INCREF(state, o->h_root);

    TRACK(state, (PyObject *)o);

    return (PyObject *)o;
}


PyObject *
MemHive_MapGetItem(module_state *state, PyObject *self,
                   PyObject *key, PyObject *def)
{
    if (!IS_MAP_SLOW(state, self)) {
        PyErr_SetString(PyExc_TypeError, "not a map");
        return NULL;
    }
    TRACK(state, key);
    BaseMapObject *map = (BaseMapObject *)self;
    return map_get(state, map, key, def);
};


PyObject *
MemHive_MapSetItem(module_state *state, PyObject *self,
                   PyObject *key, PyObject *val)
{
    if (!IS_MAP_SLOW(state, self)) {
        PyErr_SetString(PyExc_TypeError, "not a map");
        return NULL;
    }
    TRACK(state, key);
    MapObject *map = (MapObject *)self;
    return (PyObject *)map_assoc(state, map, key, val);
}


int
MemHive_MapContains(module_state *state, PyObject *self, PyObject *key)
{
    if (!IS_MAP_SLOW(state, self)) {
        PyErr_SetString(PyExc_TypeError, "not a map");
        return -1;
    }

    TRACK(state, key);
    PyObject *val;
    map_find_t res = map_find(state, (BaseMapObject *)self, key, &val);
    switch (res) {
        case F_ERROR:
            return -1;
        case F_NOT_FOUND:
            return 0;
        case F_FOUND_EXT:
        case F_FOUND:
            return 1;
        default:
            abort();
    }
}


////////////////////////////////////////////////////////////////////////////////


static PyObject *
map_node_unproxy(module_state *state, MapNode *node);

static PyObject *
map_node_bitmap_unproxy(module_state *state, MapNode_Bitmap *node)
{
    if (node->interpreter_id == state->interpreter_id) {
        Py_INCREF((PyObject *)node);
        return (PyObject *)node;
    }

    MapNode_Bitmap *new_node = (MapNode_Bitmap*)map_node_bitmap_new(
        state, Py_SIZE(node), 0);
    if (new_node == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < Py_SIZE(node); i++) {
        if (i % 2 == 0) {
            if (node->b_array[i] != NULL) {
                PyObject *o = COPY_OBJ(state, node->b_array[i]);
                if (o == NULL) {
                    return NULL;
                }
                new_node->b_array[i] = o;
            } else {
                new_node->b_array[i] = NULL;
            }
        } else {
            assert(node->b_array[i] != NULL);
            if (node->b_array[i-1] == NULL) {
                assert(IS_NODE_SLOW(state, node->b_array[i]));
                PyObject *child = map_node_unproxy(
                    state, (MapNode *)node->b_array[i]);
                if (child == NULL) {
                    return NULL;
                }
                new_node->b_array[i] = child;
            } else {
                assert(!IS_NODE_SLOW(state, node->b_array[i]));
                PyObject *o = COPY_OBJ(state, node->b_array[i]);
                if (o == NULL) {
                    return NULL;
                }
                new_node->b_array[i] = o;
            }
        }
    }

    new_node->b_bitmap = node->b_bitmap;
    return (PyObject *)new_node;
}

static PyObject *
map_node_array_unproxy(module_state *state, MapNode_Array *node)
{
    if (node->interpreter_id == state->interpreter_id) {
        Py_INCREF((PyObject *)node);
        return (PyObject *)node;
    }

    MapNode_Array *new_node = (MapNode_Array *)map_node_array_new(
        state, node->a_count, 0);
    if (new_node == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < HAMT_ARRAY_NODE_SIZE; i++) {
        if (node->a_array[i] == NULL) {
            continue;
        }

        assert(IS_NODE_SLOW(state, node->a_array[i]));
        PyObject *child = map_node_unproxy(state, (MapNode *)node->a_array[i]);
        if (child == NULL) {
            return NULL;
        }
        new_node->a_array[i] = (MapNode *)child;
    }

    return (PyObject *)new_node;
}

static PyObject *
map_node_collision_unproxy(module_state *state, MapNode_Collision *node)
{
    if (node->interpreter_id == state->interpreter_id) {
        Py_INCREF((PyObject *)node);
        return (PyObject *)node;
    }

    MapNode_Collision *new_node = (MapNode_Collision *)map_node_collision_new(
        state, node->c_hash, Py_SIZE(node), 0);
    if (new_node == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < Py_SIZE(node); i++) {
        assert(node->c_array[i] != NULL);
        assert(!IS_NODE_SLOW(state, node->c_array[i]));
        PyObject *o = COPY_OBJ(state, node->c_array[i]);
        if (o == NULL) {
            return NULL;
        }
        new_node->c_array[i] = o;
    }

    return (PyObject *)new_node;
}

static PyObject *
map_node_unproxy(module_state *state, MapNode *node)
{
    if (IS_BITMAP_NODE(state, node)) {
        return map_node_bitmap_unproxy(state, (MapNode_Bitmap *)node);
    }
    else if (IS_ARRAY_NODE(state, node)) {
        return map_node_array_unproxy(state, (MapNode_Array *)node);
    }
    else {
        assert(IS_COLLISION_NODE(state, node));
        return map_node_collision_unproxy(state, (MapNode_Collision *)node);
    }
}


PyObject *
MemHive_CopyMapProxy(module_state *state, PyObject *map)
{
    MapObject *o = map_alloc(state);
    if (o == NULL) {
        return NULL;
    }

    o->h_count = ((MapObject *)map)->h_count;

    MapNode *root = ((MapObject *)map)->h_root;
    MapNode *new_root = (MapNode *)map_node_unproxy(state, root);
    if (new_root == NULL) {
        return NULL;
    }

    o->h_root = new_root;

    VALIDATE_NODE(state, new_root);

    return (PyObject *)o;
}
