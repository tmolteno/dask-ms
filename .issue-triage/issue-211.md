=== ISSUE #211 | Support rechunking of non-row axes in `xds_from_parquet`. | labels: enhancement | comments: [] | created: 2022-04-26T07:56:58Z ===
### Description

Parquet does not allow for on-disk chunking of non-row dimensions. In the interests of providing as uniform an interface as possible to the casa, zarr and parquet backends, I propose that we support chunking in non-row dimensions using `dask.Array.rechunk` functionality. This will have some obvious limitations, as we will still end up with a row-only chunked array in memory i.e. we will read all channels for a number of rows, even if we subsequently process them in smaller chunks. This will will also introduce a shared root in resulting graph (although we may be able to circumvent this with inlining/caching).

An alternative would be to re-read and slice the data for non-row chunks. This would result in a large amount of memory and disk overhead as we would need to repeatedly allocate memory and read from disk.

My instinct is to go with the first option for now as this is still highly experimental functionality.
