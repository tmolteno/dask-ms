=== ISSUE #373 | xarray datasets derived from the katdal backend are not pickleable | labels:  | comments: [] | created: 2026-05-18T11:24:31Z ===
This follows from the fact that the underlying katdal datasets backing the dask arrays are not pickleable:

- https://github.com/ska-sa/katdal/issues/392
