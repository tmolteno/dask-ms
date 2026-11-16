=== ISSUE #283 | `daskms.experimental.xds_to_zarr` may fail due to large chunks | labels:  | comments: [] | created: 2023-08-14T13:47:07Z ===
* dask-ms version: master
* Python version: 3.10.12
* Operating System: Ubuntu 20.04 LTS

### Description

When attempting to write an `xarray.Dataset` with large chunks to zarr, I got the following error:

```bash
ValueError: Column FLAG has a chunk of dimension (132750, 4096, 4) that will exceed zarr's 2GiB chunk limit
```

### What I Did

```python
xds_to_zarr([xds_with_large_chunks], "test.zarr", rechunk=True)
```

### Possible solution
This problem can be solved in a number of ways:

1. In user code: I could manually figure out and rechunk before writing.
2. As part of `maybe_rechunk` in `xds_to_zarr`: This functionality currently forces chunks to match those already on disk. It would not be a stretch to extend this to include the case where a new zarr output is being written.
3. As a utility function: A function could be added e.g. `rechunk_for_disk`, which a user could call prior to writing an `xarray.Dataset` to disk.

Practically, a combination of 2 and 3 will likely be best i.e. write a utility function and then call it as part of `xds_to_zarr`. I will take a stab at this this week.
