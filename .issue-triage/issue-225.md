=== ISSUE #225 | Document performance tuning | labels: documentation | comments: [] | created: 2022-07-04T10:38:06Z ===
# Description

Currently, there are a number of parameters that can affect performance in multiple formats. It would be useful to document them.

## CASA

1. Choice of storage manager. Tiled Storage Managers have better performance than Standard Storage Managers. Data can be tiled (chunked) on multiple dimensions.
2. Grouping, indexing columns and TAQL where queries can produce non-contiguous row ordering patterns.
4.  Chunk Sizes specified in `xds_from_*(chunks={"row": 10000})` affect performance:

    - Larger chunk sizes generally perform better as requests for larger quantities of data can be issued in one call (although this is affected by (2)).
    - Functions that process larger chunk sizes can drop the GIL (especially if written in numba or C++). This is useful in a dask environment.
    - Matching some multiple of TiledStorageManager chunks would also  probably improve performance.


## Zarr

1. Can specify on disk chunk sizes, similar to TiledStorageManager

## Arrow/Parquet

1. Arrow datasets are composed of multiple Arrow tables, partitioned by row. Thus, can only chunk on row.
