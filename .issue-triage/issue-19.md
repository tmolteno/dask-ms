=== ISSUE #19 | Consider setting MS index arrays to be coordinates | labels: enhancement | comments: [{"author":{"login":"miguelcarcamov"},"authorAssociation":"NONE","body":"Hi,\r\n\r\nI have a suggestion also. It would be cool if the baselines could have an ID as well. So the index columns could be like TIME, ANTENNA1, ANTENNA2, DATA_DESC_ID and BASELINE_ID :) ","createdAt":"2021-08-07T12:29:01Z","id":"IC_kwDOBh8_I841U0FN","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/19#issuecomment-894648653","viewerDidAuthor":false}] | created: 2018-09-01T11:20:31Z ===
1. xarray supports the idea of coordinate arrays to support indexing and selection of data.
2. The MS index columns (TIME, ANTENNA1, ANTENNA2, DATA_DESC_ID) etc. are appropriate to use as coordinate arrays.
3. It's trivial to convert them from standard xarray arrays to xarray coordinates via [Dataset.set_coords](http://xarray.pydata.org/en/stable/generated/xarray.Dataset.set_coords.html#xarray.Dataset.set_coords)
4. It might useful to support this as standard practice on the xarray-ms, but at some point a decision may need to be made as to whether to support this via numpy or dask arrays.

    - dask arrays are good for distributed problems and lazy loading, but care must be taken when using them for indices for e.g. http://dask.pydata.org/en/latest/array-chunks.html?highlight=nan#unknown-chunks
    - numpy arrays avoid the above issues but could be very large given the data volumes we are now encountering.

@bennahugo and @mulan-94, before deciding that 4. is a good idea, I'd be interested in hearing your experiences with 1, 2 and 3, if you have the need to use the xarray coordinate functionality for performing selections..
