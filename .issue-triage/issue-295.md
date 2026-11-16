=== ISSUE #295 | Test suite fails on pyarrow 13.0.0 due to new dependency on Pandas | labels:  | comments: [] | created: 2023-10-27T07:13:18Z ===
* dask-ms version: 0.2.18
* Python version: 3.10
* Operating System: Ubuntu 20.04

A dependency on pandas has been introduced in pyarrow 13.0.0 which caused the test suite to fail

- https://github.com/apache/arrow/issues/38428
