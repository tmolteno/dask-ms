=== ISSUE #347 | dask-ms seems to ignore per-xds chunking when grouping by `__row__` | labels:  | comments: [{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"This could probably be improved in `dask-ms` but is it necessary to supply chunking in the `group_cols=\"__row__\"` case?","createdAt":"2025-06-03T15:22:28Z","id":"IC_kwDOBh8_I86u_ud1","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/347#issuecomment-2935940981","viewerDidAuthor":false},{"author":{"login":"JSKenyon"},"authorAssociation":"COLLABORATOR","body":"I think that in this case it is to ensure that the chunking on the datasets matches that of the frequency axis of the actual data. So the `__row__` entry in the chunking dictionary is almost certainly superfluous, but it shouldn't cause any harm. The real issue is that the frequency with heterogenous SPWs, the above fails. I dislike heterogeneous SPWs as much as the next person, hence asking if this is something that should be supported.","createdAt":"2025-06-03T15:32:00Z","id":"IC_kwDOBh8_I86u_9SX","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/347#issuecomment-2936001687","viewerDidAuthor":false},{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":">  So the __row__ entry in the chunking dictionary is almost certainly superfluous, but it shouldn't cause any harm.\n\nAgreed, I suspect I've always historically been using a pattern like:\n\n```\ntable = dask.compute(xds_from_table(\"blah.ms::SPECTRAL_WINDOW\"), group_cols=\"__row__\")\n```\n\nand then manually chunking the resulting numpy arrays, but this shouldn't surprise developers.","createdAt":"2025-06-04T07:50:23Z","id":"IC_kwDOBh8_I86vLWED","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/347#issuecomment-2938986755","viewerDidAuthor":false}] | created: 2025-06-03T14:25:55Z ===
I encountered this while attempting reproduce a different (but possibly related) bug.

Doing the following on an ms with 3 SPWs, containing 11, 4 and 17 channels respectively:
```python
from daskms import xds_from_storage_table

xds = xds_from_storage_table(
    "path/to/ms::SPECTRAL_WINDOW",
    group_cols="__row__",
    chunks=[{'__row__': 1, 'chan': (4, 4, 3)}, {'__row__': 1, 'chan': (4,)}, {'__row__': 1, 'chan': (4, 4, 4, 4, 1)}]
)
```
fails with the following traceback:

![Image](https://github.com/user-attachments/assets/da3f3fc4-6cb6-4cbc-bd01-5a69eb8b395a)

This seems to be due to [this code](https://github.com/ratt-ru/dask-ms/blob/e098821925585b46b30176791ceb1612545c159e/daskms/reads.py#L339-L366). Note that it always takes the first element of `self.chunks` which means the tables grouped on only `__row__` do not honour the chunks option correctly.

Is this behaviour that should be supported? I can make the tiny test dataset I used available if required.
