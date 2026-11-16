=== ISSUE #106 | Add dask-ms profiling functionality | labels: enhancement | comments: [{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"Hi @smasoka. See above the outline of the profiling functionality for dask-ms. Let me know if there's anything that needs expanding.","createdAt":"2020-04-21T15:08:34Z","id":"MDEyOklzc3VlQ29tbWVudDYxNzI0MDE5Mw==","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/106#issuecomment-617240193","viewerDidAuthor":false},{"author":{"login":"smasoka"},"authorAssociation":"NONE","body":"Hi @sjperkins,  I have a branch that I am working on [dask-ms-profiling](https://github.com/smasoka/dask-ms/tree/dask-ms-profiling)\r\n\r\nThe variable `DASK_MS_PROFILE` should hold `True` or `False` right? \r\nThe wrappers by default will profile, but the variable will control if the user wants to have the run times info or not.\r\n\r\nalso, am I in the right direction here?","createdAt":"2020-05-05T15:12:57Z","id":"MDEyOklzc3VlQ29tbWVudDYyNDExNTI0OA==","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/106#issuecomment-624115248","viewerDidAuthor":false}] | created: 2020-04-21T15:05:57Z ===
It would be helpful to profile where dask-ms is spending it's time. dask-ms submits operations to a CASA table object from a ThreadPoolExecutor containing a single thread.

There are two sets of function wrappers that are executed within the thread:

- [Wrappers around existing CASA Table methods](https://github.com/ska-sa/dask-ms/blob/0d401f234ccbcafbaa419003d199518c7f11981d/daskms/table_proxy.py#L84-L122)
- [Wrappers around user constructed functions which access the CASA Table](https://github.com/ska-sa/dask-ms/blob/0d401f234ccbcafbaa419003d199518c7f11981d/daskms/table_proxy.py#L196-L233)

Profiling should be done as follows:

1. For now, turn on profiling with a DASK_MS_PROFILE environment variable, checked
   with `os.environ("DASK_MS_PROFILE")`. In future I'd like to configure functionality with [donfig](https://github.com/pytroll/donfig), but this can be a future PR.
2. swap out these wrappers for profiling wrappers. Something like https://stackoverflow.com/a/27737385 as a mechanism would be a good start.
3. Store the run times as a `{function_name: Profiling(accumulated_time, accumulated_counts)}`
dictionary that gets printed `atexit`.  @o-smirnov you may have further ideas on what you want to see here.

One interesting thing that's also worth checking here is the effect that table locking has on performance. If all table access is mediated through a single thread, then locking shouldn't strictly be necessary, *for a single node*.

Unfortunately, if multiple nodes are accessing the same table then that table lock is probably necessary, although I see there are options for turning it off completely in casacore, presumably for the cases where the access patterns are non-overlapping reads and writes.
