=== ISSUE #360 | Supplying a ROWID for an empty table should throw a better error message as ROWID implies a table update | labels:  | comments: [{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"Thanks for the reproducer @miguelcarcamov. \n\nThere is a test case for this behaviour so my initial strategy would be to figure out how the example above differs from that:\n\nhttps://github.com/ratt-ru/dask-ms/blob/f3d32a711b2dfdaafd724977fb7800377faf0785/daskms/tests/test_ms_creation.py#L176-L185\n\nMaybe `chunks='auto'` is causing the issue. I'll start taking a closer look tomorrow. \n\n\n","createdAt":"2025-10-27T15:27:42Z","id":"IC_kwDOBh8_I87Nv6rp","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/360#issuecomment-3451890409","viewerDidAuthor":false},{"author":{"login":"miguelcarcamov"},"authorAssociation":"NONE","body":"Thank you for the information. I will keep checking what is going on.\n\nGot this warning when I activated the logs\n\n`WARNING:daskms.writes:Unusual ROWID layer arange-978dd7ae7b264601da0ace2b67530ec2. This is probably OK but could foreshadow incorrect behaviour.`\n\nnot sure if that would help","createdAt":"2025-10-27T16:53:31Z","id":"IC_kwDOBh8_I87NxrN9","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/360#issuecomment-3452351357","viewerDidAuthor":false},{"author":{"login":"miguelcarcamov"},"authorAssociation":"NONE","body":"I took off the ROWID coords in the construction of the xarray datasets and it worked. It seems that dask-ms has a bug when ROWID coords are present :).","createdAt":"2025-10-27T17:07:56Z","id":"IC_kwDOBh8_I87Nx6oo","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/360#issuecomment-3452414504","viewerDidAuthor":false},{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"> I took off the ROWID coords in the construction of the xarray datasets and it worked. It seems that dask-ms has a bug when ROWID coords are present :).\n\nAh! Thanks for figuring that out. That is buggy, could probably throw a nicer error message although this reminded me that it is intended behaviour!\n\nhttps://dask-ms.readthedocs.io/en/latest/tutorial/writes.html#updating-appending-rows\n\n> The presence of ROWID coordinates on each of the datasets provided to [xds_to_table()](https://dask-ms.readthedocs.io/en/latest/api.html#daskms.xds_to_table) governs whether the function will update or append rows to a table.\n\nI'll update the title of the issue","createdAt":"2025-10-27T17:17:30Z","id":"IC_kwDOBh8_I87NyE5s","includesCreatedEdit":true,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/360#issuecomment-3452456556","viewerDidAuthor":false}] | created: 2025-10-24T23:13:28Z ===
When attempting to write subtables (ANTENNA, FIELD, SPECTRAL_WINDOW, etc.) to an existing Measurement Set using xds_to_table(), I encounter a `RuntimeError: TableProxy::TableProxy::putColumn: no such row error`. This occurs even with minimal test cases and appears to be related to row allocation in subtable creation.


Minimal code here:

```python
import dask.array as da
import xarray as xr
from daskms import xds_to_table
import dask


# First, create a minimal main table to establish MS structure
minimal_main = xr.Dataset({
    'DATA': (['row', 'chan', 'corr'], da.zeros((1, 1, 1), chunks='auto')),
    'FLAG': (['row', 'chan', 'corr'], da.zeros((1, 1, 1), dtype=bool, chunks='auto')),
    'ANTENNA1': (['row'], da.zeros(1, dtype='int32', chunks='auto')),
    'ANTENNA2': (['row'], da.zeros(1, dtype='int32', chunks='auto')),
}, coords={'ROWID': (['row'], da.arange(1, chunks='auto'))})

print('Creating MS structure with minimal main table...')
try:
    writes = xds_to_table(minimal_main, 'test_minimal_antenna.ms')
    dask.compute(writes)
    print('MS structure created successfully')

    # Now try to write ANTENNA table
    print('Writing ANTENNA table to existing MS...')
    n_antenna = 3
    minimal_antenna = xr.Dataset({
        'NAME': (['row'], da.from_array(['ANT1', 'ANT2', 'ANT3'], chunks='auto')),
        'POSITION': (['row', 'xyz'], da.zeros((n_antenna, 3), chunks='auto')),
        'DISH_DIAMETER': (['row'], da.ones(n_antenna, chunks='auto')),
        'FLAG_ROW': (['row'], da.zeros(n_antenna, dtype=bool, chunks='auto')),
    }, coords={'ROWID': (['row'], da.arange(n_antenna, chunks='auto'))})

    writes = xds_to_table(minimal_antenna, 'test_minimal_antenna.ms::ANTENNA', columns=["NAME"])
    dask.compute(writes)
    print('ANTENNA table write successful')

except Exception as e:
    print(f'Test failed: {e}')
    import traceback
    traceback.print_exc()
```

Expected behavior: The subtable should be created successfully with the specified number of rows (3 in this case) and data should be written without errors.

Actual behavior:
```python
File "/home/user/.local/share/mamba/envs/env/lib/python3.12/site-packages/daskms/writes.py", line 230, in putter_wrapper
    ).result()
File "/home/user/.local/share/mamba/envs/env/lib/python3.12/site-packages/daskms/writes.py", line 41, in ndarray_putcol
    putcol(column, data[rr : rr + rl], startrow=rs, nrow=rl)
File "/home/user/.local/share/mamba/envs/env/lib/python3.12/site-packages/casacore/tables/table.py", line 1152, in table.putcol
    self._putcol(columnname, startrow, nrow, rowincr, value)
RuntimeError: TableProxy::TableProxy::putColumn: no such row
```

Additional Information

Main table writing works perfectly - The issue is specifically with subtable creation
Occurs with any subtable - ANTENNA, FIELD, SPECTRAL_WINDOW, etc. all fail with the same error
Data structure is correct - The xarray datasets have proper dimensions and dtypes
MS structure exists - The main table write creates the MS directory structure successfully
Minimal test case fails - Even with the simplest possible subtable, the error occurs

Workaround
Currently, I can only write main tables successfully. Subtable writing is completely blocked by this issue.

Questions
1. Is this a known issue with the current version of dask-ms?
2. Are there any specific requirements for subtable creation that I might be missing?
3. Is there a different approach I should use for creating subtables?

Additional Context:
This issue is preventing me from implementing a complete MS creation workflow where I need to write both main tables and subtables. The main table functionality works perfectly, but subtable creation appears to have a fundamental issue with row allocation in the casacore table system.


Python 3.12.11
dask-ms version 0.2.21
OS: Manjaro OS
