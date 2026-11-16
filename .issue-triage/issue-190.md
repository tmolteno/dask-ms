=== ISSUE #190 | How to deal with file locks properly in a distributed environment | labels:  | comments: [] | created: 2022-03-24T06:59:55Z ===
Just plotting down ideas for discussion for now:

From what I can tell in my browse of this there are the following existing issues:
- In the distributed case casacore::tables rely on OS-supported flocks (sysctl) which are not guaranteed to be safe to use in shared storage between machines (there is no awareness of node IP or other identifying criteria. Looking at this: https://github.com/ratt-ru/dask-ms/blob/master/daskms/table_executor.py#L38-L54 if multiple dask processes each backed with a threadpool and queue pointing to a database on a shared filesystem are started on multiple nodes there is no guarantee that the flocks will hold.
-  One possibility (since dask is being used here) is to implement something like: https://github.com/pydata/xarray/blob/main/xarray/backends/locks.py with spinning locks to block until the lock becomes available as a wrapper around the entire table system to make it distribution-safe.
- The same "user-style" read and write locking will need to be applied for xarray-backed datasets as far as I can tell via context management, although I'm not sure how finely the arrays are "bucketed" in the array specification for this to work.
