=== ISSUE #212 | Add quiet mode to `xds_from_storage*` functions | labels: enhancement | comments: [] | created: 2022-04-26T08:07:26Z ===
### Description

These functions can raise a large number of `UserWarnings` due to slight discrepancies in the supported features. Canny developers may want to silence these warnings if they are aware of the repercussions and don't want the messages confusing users.
