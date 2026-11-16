=== ISSUE #82 | Tutorial needs updating | labels: bug | comments: [{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"Thanks for the report @marcinglowacki. \r\n\r\nRegarding your first issue, dask arrays are required on the write dataset. dask-ms is falling over because a numpy array has been provided.\r\n\r\nRegarding your second issue, xarray is an optional dask-ms dependency. dask-ms has its own \"poor man's Dataset\" for use in cases where xarray is not installed. This is a case where the interaction between their internal types conflicted.\r\n\r\nI've merged a fix in https://github.com/ska-sa/dask-ms/pull/83 into master. Could you confirmm whether this fixes your second issue (and complains on the first).","createdAt":"2019-12-12T11:56:23Z","id":"MDEyOklzc3VlQ29tbWVudDU2NDk3NjkzOA==","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/82#issuecomment-564976938","viewerDidAuthor":false}] | created: 2019-12-12T10:31:11Z ===
* dask-ms version: 0.2.3
* Python version: 3.6.8
* Operating System: Ubuntu

### Description

I am trying to write to a new .ms file via xds_to_table, but encounter errors regardless of the file name or the column_keywords. I also am unable to run the examples given on the https://dask-ms.readthedocs.io/en/latest/tutorial/writes.html page involving example_ms.

### What I Did

```
from daskms import xds_from_table, xds_from_ms, xds_to_table

import numpy as np
import datashader as ds
import datashader.transfer_functions as tf
import dask.array as da

xds = xds_from_ms("/data/scratchtmp/marcin/laduma_reduction/reflagged/1539630057_sdp_l0.full_1284.full_pol.ms/")

for i in range(3):
    xds[i]['FLAG'] = False#da.zeros((xds[i].FLAG.values.shape),dtype=bool)

writes = xds_to_table(xds, '/data/scratchtmp/marcin/laduma_reduction/reflagged/TEST.MS','ALL')
```

This gives:

```
---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
<ipython-input-33-c4f9a7e71f2d> in <module>()
----> 1 writes = xds_to_table(xds, '/data/scratchtmp/marcin/laduma_reduction/reflagged/test.ms','ALL')

/anaconda3/lib/python3.6/site-packages/daskms/dask_ms.py in xds_to_table(xds, table_name, columns, descriptor, table_keywords, column_keywords)
    108                             descriptor=descriptor,
    109                             table_keywords=table_keywords,
--> 110                             column_keywords=column_keywords)
    111
    112     # No xarray available assume dask datasets

/anaconda3/lib/python3.6/site-packages/daskms/writes.py in write_datasets(table, datasets, columns, descriptor, table_keywords, column_keywords)
    557
    558     if not table_exists(table):
--> 559         table_proxy = _create_table(table, datasets, columns, descriptor)
    560     else:
    561         table_proxy = _updated_table(table, datasets, columns, descriptor)

/anaconda3/lib/python3.6/site-packages/daskms/writes.py in _create_table(table_name, datasets, columns, descriptor)
    179 def _create_table(table_name, datasets, columns, descriptor):
    180     builder = descriptor_builder(table_name, descriptor)
--> 181     table_desc, dminfo = builder.execute(datasets)
    182
    183     root, table, subtable = table_path_split(table_name)

/anaconda3/lib/python3.6/site-packages/daskms/descriptors/builder.py in execute(self, datasets)
     72         default_desc = self.default_descriptor()
     73         variables = self.dataset_variables(datasets)
---> 74         table_desc = self.descriptor(variables, default_desc)
     75         dminfo = self.dminfo(table_desc)
     76

/anaconda3/lib/python3.6/site-packages/daskms/descriptors/ms.py in descriptor(self, variables, default_desc)
    101
    102         if self.fixed:
--> 103             ms_dims = self.infer_ms_dims(variables)
    104             desc = self.fix_columns(variables, desc, ms_dims)
    105

/anaconda3/lib/python3.6/site-packages/daskms/descriptors/ms.py in infer_ms_dims(variables)
    110
    111         # Create a dictionary of all variables in all datasets
--> 112         expanded_vars = {v.data.name: v for k, lv in variables.items()
    113                          for v in lv}
    114

/anaconda3/lib/python3.6/site-packages/daskms/descriptors/ms.py in <dictcomp>(.0)
    111         # Create a dictionary of all variables in all datasets
    112         expanded_vars = {v.data.name: v for k, lv in variables.items()
--> 113                          for v in lv}
    114
    115         # Now try find consistent dimension sizes across all variables

AttributeError: 'numpy.ndarray' object has no attribute 'name'
```

If trying to write to the same .ms file path:
```
writes = xds_to_table(xds, '/data/scratchtmp/marcin/laduma_reduction/reflagged/1539630057_sdp_l0.full_1284.full_pol.ms/','ALL')
```
I get instead:

```
---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
<ipython-input-34-1fab1502fc51> in <module>()
----> 1 writes = xds_to_table(xds, '/data/scratchtmp/marcin/laduma_reduction/reflagged/1539630057_sdp_l0.full_1284.full_pol.ms/','ALL')

/anaconda3/lib/python3.6/site-packages/daskms/dask_ms.py in xds_to_table(xds, table_name, columns, descriptor, table_keywords, column_keywords)
    108                             descriptor=descriptor,
    109                             table_keywords=table_keywords,
--> 110                             column_keywords=column_keywords)
    111
    112     # No xarray available assume dask datasets

/anaconda3/lib/python3.6/site-packages/daskms/writes.py in write_datasets(table, datasets, columns, descriptor, table_keywords, column_keywords)
    564                            descriptor=descriptor,
    565                            table_keywords=table_keywords,
--> 566                            column_keywords=column_keywords)

/anaconda3/lib/python3.6/site-packages/daskms/writes.py in _write_datasets(table, table_proxy, datasets, columns, descriptor, table_keywords, column_keywords)
    479             # there is more than one chunk in any of the non-row columns.
    480             # In that case, we can putcol, otherwise putcolslice is required
--> 481             if not all(len(c) == 1 for c in array.chunks[1:]):
    482                 # Add extent arrays
    483                 for d, c in zip(full_dims[1:], array.chunks[1:]):

AttributeError: 'numpy.ndarray' object has no attribute 'chunks'
```

As for the tutorial page, I get errors for Dataset commands, e.g. running
```
import dask
from daskms import xds_from_ms, Dataset
from daskms.example_data import example_ms

# Create example Measurement Set and read datasets
ms = example_ms()
datasets = xds_from_ms(ms)
# Add last Dataset to table using variables only (no ROWID coordinate)
new_ds = Dataset(datasets[-1].data_vars)
datasets.append(new_ds)

# Write datasets back to Measurement Set
writes = xds_to_table(datasets, ms, "ALL")
dask.compute(writes)
```
gives
```
---------------------------------------------------------------------------
ValueError                                Traceback (most recent call last)
<ipython-input-9-1c7fed1cd01e> in <module>()
      7 datasets = xds_from_ms(ms)
      8 # Add last Dataset to table using variables only (no ROWID coordinate)
----> 9 new_ds = Dataset(datasets[-1].data_vars)
     10 datasets.append(new_ds)
     11

/anaconda3/lib/python3.6/site-packages/daskms/dataset.py in __init__(self, data_vars, coords, attrs)
    230         """
    231         self._data_vars = {k: _convert_to_variable(k, v)
--> 232                            for k, v in data_vars.items()}
    233
    234         if coords is not None:

/anaconda3/lib/python3.6/site-packages/daskms/dataset.py in <dictcomp>(.0)
    230         """
    231         self._data_vars = {k: _convert_to_variable(k, v)
--> 232                            for k, v in data_vars.items()}
    233
    234         if coords is not None:

/anaconda3/lib/python3.6/site-packages/daskms/dataset.py in _convert_to_variable(k, v)
    201         raise ValueError("'%s' must be a size 2 to 5 tuple of the form"
    202                          "(dims, array[, attrs[, encoding[, fastpath]]]) "
--> 203                          "tuple. Got '%s' instead," % (k, type(v)))
    204
    205     return as_variable(v)

ValueError: 'ANTENNA1' must be a size 2 to 5 tuple of the form(dims, array[, attrs[, encoding[, fastpath]]]) tuple. Got '<class 'xarray.core.dataarray.DataArray'>' instead,
```
