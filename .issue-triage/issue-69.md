=== ISSUE #69 | Incorrect data type used for column keyword | labels: enhancement | comments: [{"author":{"login":"JSKenyon"},"authorAssociation":"COLLABORATOR","body":"This may have been a false alarm - I have managed to fix the problem by manually removing and re-adding the keywords using pyrap. I will leave this open for now as I am not sure what caused the original problem (an interrupted write perhaps?).","createdAt":"2019-11-05T06:33:46Z","id":"MDEyOklzc3VlQ29tbWVudDU0OTY4MjQ1Mg==","includesCreatedEdit":true,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/69#issuecomment-549682452","viewerDidAuthor":false},{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"As per our discussion, the likely cause of this is updating an existing keyword with a value of a different type.","createdAt":"2019-11-05T08:16:54Z","id":"MDEyOklzc3VlQ29tbWVudDU0OTcxMjA5Mw==","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/69#issuecomment-549712093","viewerDidAuthor":false},{"author":{"login":"JSKenyon"},"authorAssociation":"COLLABORATOR","body":"This just bit me again. I now have a better understanding of why. The issue stems from the fact that `ms.getcolkeyword` does not preserve underlying dtype. By that I mean that if you read a column keyword which is say an `int32`, it will be loaded as a standard Python `int` (64-bit). This means that if you read a keyword and then try to write it back, it can fail as the dtype may have changed. This is difficult to keep track of as you cannot query the dtype of a column keyword.  ","createdAt":"2020-07-24T08:44:44Z","id":"MDEyOklzc3VlQ29tbWVudDY2MzQxNTUxNQ==","includesCreatedEdit":true,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/69#issuecomment-663415515","viewerDidAuthor":false},{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"For future me's edification, a possible solution might be casting python types to numpy types before column keyword writes. ","createdAt":"2020-07-24T08:50:14Z","id":"MDEyOklzc3VlQ29tbWVudDY2MzQyMDM0Nw==","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/69#issuecomment-663420347","viewerDidAuthor":false}] | created: 2019-11-05T06:15:22Z ===
* dask-ms version: 0.2.2
* Python version: 3.6.8
* Operating System: Ubuntu via WSL

### Description

I updated all my dependencies to the latest versions today and I now get the following error message when attempting to set up a write to column keywords.

```
RuntimeError: TableRecordRep::defineDataField - incorrect data type used for field FLAGSET_legacy.
```

The value currently in that field is an int, as is the value I am attempting to write to it.

### What I Did

I get the same behaviour outside of my project. I ran the following via iPython:

```python
from daskms import xds_from_ms, xds_to_table

xds, kwrds = xds_from_ms("C147_unflagged.MS/", columns=("BITFLAG"), column_keywords=True)
xds_to_table(xds, "C147_unflagged.MS/", columns="BITFLAG", column_keywords={"BITFLAG": kwrds["BITFLAG"]})
```
Note that the above example does not edit the keywords - it just attempts to rewrite the existing keywords but it still falls over. This seems to happen whenever I attempt to write to a keyword which already exists and contains an integer value, or when I attempt to change the type of the value in an existing keyword.
