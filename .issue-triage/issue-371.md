=== ISSUE #371 | Remove low-level graph construction | labels: enhancement | comments: [] | created: 2026-04-21T09:45:23Z ===
dask-ms is showing it's age: Many of the routines for constructing read or write arrays were constructed with low level dictionary task graphs. In particular the use of cached and inlined arrays were built using low level graphs and this lead to compatibility issues with newer dask versions:

- https://github.com/ratt-ru/dask-ms/issues/350

This was resolved by #368, but the core issue of low level graph construction still remains. This issue tracks the requirement to upgrade to newer dask constructs that will be better supported in future. However, thanks to #368, the urgency for this upgrade is reduced.
