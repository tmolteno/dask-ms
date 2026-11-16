=== ISSUE #32 | casacore exception thrown at end of test suite if temporary MS is deleted at the end of the session | labels: bug,wontfix | comments: [{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"Solve this for the moment by not deleting the temporary MS in https://github.com/ska-sa/xarray-ms/pull/31/commits/262625ec4a82c62bbf12c5c3927a4d19e028b173","createdAt":"2019-05-21T14:31:43Z","id":"MDEyOklzc3VlQ29tbWVudDQ5NDQxNjk0NA==","includesCreatedEdit":true,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/32#issuecomment-494416944","viewerDidAuthor":false}] | created: 2019-05-21T14:30:17Z ===
* xarray-ms version: 1.4
* Python version: 3.6
* Operating System: Ubuntu 18.04

### Description

The ms fixture deletes the measurement set directory once the session closes.

```python
@pytest.fixture(scope="session")
def ms(tmp_path_factory):
    msdir = tmp_path_factory.mktemp("msdir", numbered=False)
    fn = os.path.join(str(msdir), "test.ms")
   ...
    yield fn

    # Remove the temporary directory
    shutil.rmtree(str(msdir))
```

This seems to interact badly with casacore on python3. It may be hanging on to some references internally.

```traceback
$ py.test -s -v xarrayms/
=============================================================================================== test session starts ===============================================================================================
platform linux -- Python 3.6.7, pytest-4.5.0, py-1.8.0, pluggy-0.11.0 -- /home/sperkins/venv/xms-py3/bin/python3.6
cachedir: .pytest_cache
rootdir: /home/sperkins/work/ska/code/xarray-ms, inifile: setup.cfg
collected 47 items

xarrayms/tests/test_ms.py::test_ms_read[index_cols=['ANTENNA2', 'ANTENNA1', 'TIME']-
...
xarrayms/tests/test_table_executor.py::test_table_wrapper_locks[lockseq6] PASSED

================================================================================================ warnings summary =================================================================================================
/home/sperkins/venv/xms-py3/lib/python3.6/site-packages/_pytest/mark/structures.py:324
  /home/sperkins/venv/xms-py3/lib/python3.6/site-packages/_pytest/mark/structures.py:324: PytestUnknownMarkWarning: Unknown pytest.mark.slow - is this a typo?  You can register custom marks to avoid this warning - for details, see https://docs.pytest.org/en/latest/mark.html
    PytestUnknownMarkWarning,

-- Docs: https://docs.pytest.org/en/latest/warnings.html
================================================================================ 45 passed, 2 xfailed, 1 warnings in 20.64 seconds ================================================================================
terminate called after throwing an instance of 'casacore::AipsError'
  what():  RegularFile: /tmp/pytest-of-sperkins/pytest-3/msdir/test.ms/table.dat does not exist and cannot be created
Aborted (core dumped)
```
