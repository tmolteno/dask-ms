=== ISSUE #223 | Reintroduce warnings for non-contiguous data | labels:  | comments: [] | created: 2022-06-28T10:52:27Z ===
### Description

Historically we used to warn about highly fragmented row runs, but this was removed as it spammed the log.

However, it would be better to reintroduce this in a more limited form to enable faster debugging of disk performance issues.

cc @JSKenyon @o-smirnov
