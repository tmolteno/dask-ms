=== ISSUE #314 | Capitalised ROWID breaks xarray lowercase coordinate naming convention | labels: bug | comments: [] | created: 2024-03-20T11:16:08Z ===
* dask-ms version: 0.20.0
* Python version: 3.9
* Operating System: Ubuntu

### Description

dask-ms chose to capitalise ROWID, a dask coordinate array. By convention, xarray treats uppercase zarr arrays as data variables and lowercase zarr arrays as coordinates. Thus, when `xarray.open_zarr` is called, `ROWID` is loaded as a data variable.
