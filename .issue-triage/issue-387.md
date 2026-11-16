=== ISSUE #387 | Move daskms.experimental.zarr to zarr-python 3 | labels: enhancement,dependencies | comments: [] | created: 2026-09-30T09:35:15Z ===
dask-ms pins `zarr >=2.12.0, <3.0.0` (and `numcodecs <0.16.0`). The zarr 2 pin
now also holds xarray back. xarray 2026.9.0 requires `zarr >=3` for zarr I/O,
so pip pairs it with zarr 2.18.7 and xarray's zarr backend fails:

```
TypeError: open_group() got an unexpected keyword argument 'zarr_format'
```

This fails `test_xarray_to_zarr` and `test_xarray_reading_daskms_written_dataset`
on every supported Python version. #384 works around it by capping
`xarray <2026.9.0` in the `xarray`, `katdal` and `complete` extras. `master`
will hit the same failure on its next CI run without that cap.

The lasting fix is to move `daskms.experimental.zarr` to zarr-python 3.

## Writing zarr v2 with zarr-python 3

zarr-python 3 still writes the v2 format when given `zarr_format=2`. Tested
by writing with zarr 3.4.0 and reading back with zarr 2.18.7:

| feature | zarr 3 writing v2 | zarr 2 reads it |
|---|---|---|
| numeric and bool arrays | yes | yes |
| group attributes (incl. `__daskms_partition_schema__`) | yes | yes |
| consolidated metadata | yes | yes |
| fsspec mapper as the store | yes | — |
| strings as `dtype=zarr.dtype.VariableLengthUTF8()` | yes | yes (object array, `VLenUTF8` filter) |
| strings as `dtype=object` + `numcodecs.JSON()` (what dask-ms writes now) | **no** | — |

zarr 3 rejects a bare `object` dtype as ambiguous and has no JSON object codec
in its v2 path.

## Changes needed in `daskms/experimental/zarr/__init__.py`

- Write string columns as `VariableLengthUTF8` in place of
  `dtype=object, object_codec=numcodecs.JSON()`.
- `require_dataset` → `require_array`.
- `zarr.convenience.consolidate_metadata` → `zarr.consolidate_metadata`. The
  `zarr.convenience` module no longer exists.
- Opening a missing store now raises `FileNotFoundError` rather than
  `zarr.errors.GroupNotFoundError`, so the "open, or create if missing"
  fallback in `xds_to_zarr` needs to catch it.
- Drop the `zarr <3`, `numcodecs <0.16` and `xarray <2026.9.0` caps.

## Compatibility with existing stores

zarr 3 cannot read the JSON-encoded object arrays that current dask-ms writes
for string columns:

```
ValueError: No Zarr data type found that matches {'name': '|O', 'object_codec_id': 'json2'}
```

- **With consolidated metadata** (the `xds_to_zarr` default), the error comes
  from parsing `.zmetadata`, so the whole store fails to open, not just the
  string columns.
- **Without consolidation**, the group opens and numeric columns read, but the
  string columns still fail.

Stores written after the move would remain readable with zarr 2, so the
format change is backward compatible; reading older stores is what breaks.

As zarr support is experimental, a full compatibility path (rewriting the
`json2` dtype in the metadata) is probably not warranted. A cheap mitigation
would be to catch this error in `xds_from_zarr` and re-raise with a message
saying the store was written by an older dask-ms with zarr 2 and needs
`zarr <3` to read.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
