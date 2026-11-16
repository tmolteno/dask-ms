=== ISSUE #67 | force_write option for xds_to_table | labels: enhancement | comments: [] | created: 2019-10-21T11:48:51Z ===
* dask-ms version: 0.2.0
* Python version: 3.6.8
* Operating System: Ubuntu 18.04 under WSL

Sometimes it is possible to end up with a partially added/incorrect column in the MS. Whilst testing, I accidentally wrote a column as complex128 which is not understood by wsclean. I was forced to remove the column in question manually using pyrap. It would be useful to have the option to force a write in situations where the column being written to exists but is not consistent with the data we want to write to it. This could be as simple as removing the old column before readding the new.
