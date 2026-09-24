# -*- coding: utf-8 -*-

import uuid
from threading import Lock
from weakref import WeakKeyDictionary, WeakValueDictionary

import dask.array as da
from dask.core import flatten
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import cull, inline

# dask 2024.11.0 replaced tuple-based tasks with Task objects (TaskSpec refactor)
# and removed the private _execute_task helper.
#
# In old dask (< 2024.11.0):
#   - graph values are plain tuples: (func, arg1, arg2, ...)
#   - dask.core._execute_task(task, cache) is available
#   - Task objects exist in dask._task_spec but lack .substitute()
#
# In new dask (>= 2024.11.0):
#   - graph values are Task objects callable with a values dict
#   - _execute_task is removed
#   - Task.substitute({key: task}) recursively inlines dependencies
#   - convert_legacy_graph() normalises old-style tuples to Task objects
#
# dask-ms itself still builds some graph layers with old-style (func, *args)
# tuples.  convert_legacy_graph handles the normalisation so that the new-dask
# code paths can cross those layer boundaries.
try:
    from dask.core import _execute_task as _legacy_execute_task

    _DASK_HAS_LEGACY_TASKS = True
except ImportError:
    from dask._task_spec import convert_legacy_graph as _convert_legacy_graph

    _legacy_execute_task = None
    _DASK_HAS_LEGACY_TASKS = False


def _make_new_style_dsk(dsk):
    """Return a dict where every value is a new-style Task object.

    dask-ms builds some graph layers with old-style (func, *args) tuples even
    when running on new dask.  _convert_legacy_graph normalises them so that
    _fully_inline_task can resolve dependencies across layer boundaries.
    """
    return dict(_convert_legacy_graph(dsk))


def _fully_inline_task(task, dsk, only_from=None):
    """Recursively substitute dependencies into *task* (new dask only).

    Parameters
    ----------
    task : Task
        A new-style Task object.
    dsk : dict
        Full task graph mapping keys to Task objects.
    only_from : set, optional
        If provided, only substitute dependencies whose keys are in this set.
        Dependencies not in the set remain as key references.
    """
    deps = getattr(task, "dependencies", frozenset())
    if not deps:
        return task
    eligible = (deps & only_from) if only_from is not None else (deps & dsk.keys())
    if not eligible:
        return task
    sub = {
        dep: _fully_inline_task(dsk[dep], dsk, only_from)
        for dep in eligible
        if dep in dsk
    }
    if not sub:
        return task
    return task.substitute(sub)


_key_cache = WeakValueDictionary()
_key_cache_lock = Lock()


class KeyMetaClass(type):
    """
    Ensures that Key identities are the same,
    given the same constructor arguments
    """

    def __call__(cls, key):
        try:
            return _key_cache[key]
        except KeyError:
            pass

        with _key_cache_lock:
            try:
                return _key_cache[key]
            except KeyError:
                _key_cache[key] = instance = type.__call__(cls, key)
                return instance


class Key(metaclass=KeyMetaClass):
    """
    Suitable for storing a tuple
    (or other dask key type) in a WeakKeyDictionary.
    Uniques of key identity guaranteed by KeyMetaClass
    """

    __slots__ = ("key", "__weakref__")

    def __init__(self, key):
        self.key = key

    def __hash__(self):
        return hash(self.key)

    def __repr__(self):
        return f"Key{self.key}"

    def __reduce__(self):
        return (Key, (self.key,))

    __str__ = __repr__


def cache_entry(cache, key, *task):
    """Old-dask cache wrapper: executes a legacy tuple task and caches result."""
    with cache.lock:
        try:
            return cache.cache[key]
        except KeyError:
            cache.cache[key] = value = _legacy_execute_task(task, {})
            return value


class _CachedCompute:
    """New-dask cache wrapper: a picklable zero-arg callable that wraps a
    self-contained Task object.

    Stored as the sole element of an old-style ``(callable,)`` graph tuple so
    that new dask's convert_legacy_graph does not inspect or eagerly evaluate
    the inner Task while building the graph.

    Holds a strong reference to its Key so that _key_cache (WeakValueDictionary)
    keeps the Key alive for the lifetime of the enclosing dask Array graph.
    """

    __slots__ = ("_cache", "_key", "_inner")

    def __init__(self, cache, key_tuple, inner_task):
        self._cache = cache
        self._key = Key(key_tuple)  # strong ref keeps Key alive in _key_cache
        self._inner = inner_task

    def __call__(self):
        with self._cache.lock:
            try:
                return self._cache.cache[self._key]
            except KeyError:
                result = self._inner({})
                self._cache.cache[self._key] = result
                return result

    def __reduce__(self):
        return (_CachedCompute, (self._cache, self._key.key, self._inner))


_array_cache_cache = WeakValueDictionary()
_array_cache_lock = Lock()


class ArrayCacheMetaClass(type):
    """
    Ensures that Array Cache identities are the same,
    given the same constructor arguments
    """

    def __call__(cls, token):
        key = (cls, token)

        try:
            return _array_cache_cache[key]
        except KeyError:
            pass

        with _array_cache_lock:
            try:
                return _array_cache_cache[key]
            except KeyError:
                instance = type.__call__(cls, token)
                _array_cache_cache[key] = instance
                return instance


class ArrayCache(metaclass=ArrayCacheMetaClass):
    """
    Thread-safe array data cache. token makes this picklable.

    Cached on a WeakKeyDictionary with ``Key`` objects.
    """

    def __init__(self, token):
        self.token = token
        self.cache = WeakKeyDictionary()
        self.lock = Lock()

    def __reduce__(self):
        return (ArrayCache, (self.token,))

    def __repr__(self):
        return f"ArrayCache[{self.token}]"


def cached_array(array, token=None):
    """
    Return a new array that functionally has the same values as array,
    but flattens the underlying graph and introduces a cache lookup
    when the individual array chunks are accessed.

    Useful for caching data that can fit in-memory for the duration
    of the graph's execution.

    Parameters
    ----------
    array : :class:`dask.array.Array`
        dask array to cache.
    token : optional, str
        A unique token for identifying the internal cache.
        If None, it will be automatically generated.
    """
    dsk = dict(array.__dask_graph__())
    keys = set(flatten(array.__dask_keys__()))

    if token is None:
        token = uuid.uuid4().hex

    cache = ArrayCache(token)

    if _DASK_HAS_LEGACY_TASKS:
        # Old dask (< 2024.11.0): tuple tasks; inline + cull works correctly.
        inline_keys = set(dsk.keys() - keys)
        dsk2 = inline(dsk, inline_keys, inline_constants=True)
        dsk3, _ = cull(dsk2, keys)

        assert len(dsk3) == len(keys)

        for k in keys:
            dsk3[k] = (cache_entry, cache, Key(k), *dsk3.pop(k))
    else:
        # New dask (>= 2024.11.0): Task objects.
        #
        # dask-ms builds some layers with old-style tuples, producing mixed
        # graphs.  _make_new_style_dsk converts everything to Task objects so
        # that _fully_inline_task can substitute deps across layer boundaries.
        #
        # We cannot pass a Task as an argument to an old-style tuple because
        # convert_legacy_graph eagerly evaluates self-contained Tasks when they
        # appear as arguments.  Instead, _CachedCompute wraps the inner Task in
        # a picklable zero-arg callable stored as (callable,) in the graph.
        dsk_new = _make_new_style_dsk(dsk)
        dsk3 = {
            k: (_CachedCompute(cache, k, _fully_inline_task(dsk_new[k], dsk_new)),)
            for k in keys
        }

    graph = HighLevelGraph.from_collections(array.name, dsk3, [])

    return da.Array(graph, array.name, array.chunks, array.dtype)


def inlined_array(a, inline_arrays=None):
    """Flatten underlying graph"""
    agraph = a.__dask_graph__()
    akeys = set(flatten(a.__dask_keys__()))

    if inline_arrays is None:
        # Inline everything except the output keys.
        if _DASK_HAS_LEGACY_TASKS:
            inline_keys = set(agraph.keys()) - akeys
            dsk2 = inline(agraph, keys=inline_keys, inline_constants=True)
            dsk3, _ = cull(dsk2, akeys)
        else:
            full_dsk = _make_new_style_dsk(dict(agraph))
            dsk3 = {k: _fully_inline_task(full_dsk[k], full_dsk) for k in akeys}

        graph = HighLevelGraph.from_collections(a.name, dsk3, [])
        return da.Array(graph, a.name, a.chunks, dtype=a.dtype)

    # We're given specific arrays to inline, promote to list
    if isinstance(inline_arrays, da.Array):
        inline_arrays = [inline_arrays]
    elif isinstance(inline_arrays, tuple):
        inline_arrays = list(inline_arrays)

    if not isinstance(inline_arrays, list):
        raise TypeError(
            "Invalid inline_arrays, must be " "(None, list, tuple, dask.array.Array)"
        )

    inline_names = set(arr.name for arr in inline_arrays)
    layers = agraph.layers.copy()
    deps = {k: v.copy() for k, v in agraph.dependencies.items()}
    # We want to inline layers that depend on the inlined arrays
    inline_layers = set(
        k for k, v in deps.items() if len(inline_names.intersection(v)) > 0
    )

    for layer_name in inline_layers:
        layer_tasks = dict(layers[layer_name])
        layer_keys = set(layer_tasks.keys())

        if _DASK_HAS_LEGACY_TASKS:
            dsk = dict(layer_tasks)
            inline_keys = set()
            for arr in inline_arrays:
                dsk.update(layers[arr.name])
                deps.pop(arr.name, None)
                deps[layer_name].discard(arr.name)
                inline_keys.update(layers[arr.name].keys())

            dsk2 = inline(dsk, keys=inline_keys, inline_constants=True)
            layers[layer_name], _ = cull(dsk2, layer_keys)
        else:
            # Build a combined flat dict spanning this layer and the arrays to
            # inline, convert everything to Task objects, then substitute only
            # the specified arrays' keys into this layer's tasks.
            combined = dict(layer_tasks)
            inline_keys = set()
            for arr in inline_arrays:
                arr_tasks = dict(layers[arr.name])
                combined.update(arr_tasks)
                deps.pop(arr.name, None)
                deps[layer_name].discard(arr.name)
                inline_keys.update(arr_tasks.keys())

            combined_new = _make_new_style_dsk(combined)
            layers[layer_name] = {
                k: _fully_inline_task(
                    combined_new[k], combined_new, only_from=inline_keys
                )
                for k in layer_keys
            }

    # Remove layers containing the inlined arrays
    for inline_name in inline_names:
        layers.pop(inline_name)

    return da.Array(HighLevelGraph(layers, deps), a.name, a.chunks, a.dtype)
