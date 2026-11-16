=== ISSUE #77 | Support dask's process scheduler | labels: enhancement | comments: [] | created: 2019-12-09T15:11:03Z ===
The problem
----------------

1. A ThreadPoolExecutor is used to make access to CASA tables threadsafe in a threaded environment.
2. The ThreadPoolExecutor is created and embedded in TableProxy objects during graph creation.
3. Processes are forked during dask.compute leading to the issues well-described here: https://www.linuxprogrammingblog.com/threads-and-fork-think-twice-before-using-them

The solution
----------------

1. User configures dask-ms to use processes in some fashion, possibly as a keyword to xds_from_{ms,table} or via some global configuration option.
2. Instead of creating a ThreadPoolExecutor a DummyExecutor can be created.

```python
class DummyFuture(object):
    __slots__ = ("result",)

    def __init__(self, result):
        self.result = result

    def result(self):
        return self.result

class DummyExecutor(object):
    def submit(self, fn, *args, **kwargs):
         return DummyFuture(fn(*args, **kwargs))
```

Comments
-------------

As supporting processes is not currently a high priority, this feature is also not a high priority but exists to describe how it might be supported.

As an aside **test_multiprocess_table** shows that it's possible to spin up xds_from_ms once Processes have been forked and a fair amount of effort is taken to shutdown any threads that are started in the main Process before this happens
