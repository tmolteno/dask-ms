=== ISSUE #273 | Consolidate MS support into xds_{from,to}_ms | labels: enhancement | comments: [{"author":{"login":"JSKenyon"},"authorAssociation":"COLLABORATOR","body":"I had had a similar thought and I am all for it. This would likely be a breaking change, but I think it would be worth it. It means that we can abandon MS-like syntax in `xds_{to,from}_table` (i.e. don't introduce a MAIN table for no reason) and remove the unintuitive double colon syntax that it necessitated. This functionality can be retained/added in `xds_{to,from}_ms` to make it \"equivalent\" with MS access via `python-casacore`. I think that `xds_{to,from}_table` should be sufficient for all other use-cases.","createdAt":"2022-12-05T08:22:54Z","id":"IC_kwDOBh8_I85PsCFI","includesCreatedEdit":true,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/273#issuecomment-1336942920","viewerDidAuthor":false}] | created: 2022-12-05T08:14:32Z ===
* dask-ms version: 0.2.15
* Python version: 3.8
* Operating System: Ubuntu 20.04

In #272, @caseyjlaw tried to use `xds_from_ms` to open a sub-table of the MS. This didn't work because `xds_from_ms` adds some default values (grouping and indexing columns) for opening the MAIN table of an MS. Due to this, we currently use `xds_from_table` to access sub-tables in our applications.

However, it occurs to me that @caseyjlaw was trying to do the most obvious thing, given the [affordance](https://uxplanet.org/ux-design-glossary-how-to-use-affordances-in-user-interfaces-393c8e9686e4). It could be that split is unnecessary and confusing and that `xds_{from,to}_ms` should be the interface for dealing with all Measurement Set-like datastores (CASA/zarr/parquet), as well as their sub-tables.

Then, `xds_{from,to}_table` can be used for *general* undefined table types, and we could define `xds_{from,to}_xxx` methods to handle some `xxx` specification (calibration methods for example).

Any opinions on this @JSKenyon, @landmanbester, @o-smirnov ?
