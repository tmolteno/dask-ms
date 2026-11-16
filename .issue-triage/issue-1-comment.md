## Review + fix — 2026-11-15

**Reproduced exactly as filed** (locked pair: xarray 2026.9.0 + zarr 2.18.7, py3.13): `2 failed, 29 passed`, `TypeError: open_group() got an unexpected keyword argument 'zarr_format'` at `xarray/backends/zarr.py:1951`. The pin claims, the upstream `zarr >=3.1.3` comparison and the `importorskip` observation (`conftest.py:42`) all check out.

### Two corrections to the body

1. **"our own tests are already zarr-3-aware" is not true.** The quoted comments ("zarr 3.0.8 and earlier did not support this property", "replace AssertionError after zarr-developers/zarr-python#2821") exist nowhere in this repo (repo-wide grep) nor in upstream's `test_zarr.py`. The zarr-3-awareness lives in upstream's *writer* code, not in anyone's tests.

2. **The suggested pin bump is necessary but not sufficient.** Probed against zarr 3.4.2: bumping the extras alone would fail differently and harder, because `daskms/experimental/zarr/__init__.py` was zarr-2-only:

   * `import zarr.convenience as zc` (old line 32) → `ModuleNotFoundError` — module removed in zarr 3 → `xds_to_zarr` dies before writing
   * `ds_group.require_dataset(..., object_codec=...)` (old line 109) → `Group.require_dataset` does not exist in zarr 3 (it is `require_array`)
   * `dtype=object` + `numcodecs.JSON()` (old lines 73–75) → zarr 3 rejects a bare object dtype and has no `json2` codec on the v2 path → needs `VariableLengthUTF8`
   * `test_zarr.py`'s `.zmetadata` assertions only hold if groups are opened with `zarr_format=2`
   * zarr 3.4.2 requires `numcodecs >=0.16.6, !=0.17.0`, so dropping the `<0.16.0` cap (as the issue already recommends) is mandatory
   * and CI never saw any of it: the two `--deselect`s in `.github/workflows/ci.yml` hid exactly these failures

### Fix implemented (local branch `zarr3-migration`, not yet pushed)

* Ported upstream's completed zarr-3 migration (ratt-ru/dask-ms#387, shipped in their 0.3.0): `daskms/experimental/zarr/{__init__.py,tests/test_zarr.py}` replaced wholesale — `VariableLengthUTF8` string columns, `require_array`, a `zarr_store()` helper on `LocalStore`/`FsspecStore`, `zarr_format=2` group opens (stores stay v2-format), `FileNotFoundError` fallback, `zarr.consolidate_metadata`, `use_consolidated` reads, `map_blocks`-based read/write, and the `json2` old-store re-raise *"contains string columns written by an older dask-ms with zarr-python 2"* with a regression test
* `daskms/tests/test_fsspec_store.py::test_local_store` — same API fix
* `pyproject.toml`: `zarr >=3.1.3` in all three extras (`zarr`, `katdal`, `complete`), `numcodecs <0.16.0` dropped
* `uv.lock` re-locked: zarr 3.4.2 (3.1.6 on py3.11), numcodecs 0.17.1, +msgspec, +google-crc32c, −asciitree, −fasteners
* CI `--deselect`s removed; `HISTORY.rst` entry added

### Verification (py3.13, locked env)

| suite | before | after |
|---|---|---|
| `test_zarr.py` | 2 failed, 29 passed | **33 passed, 1 skipped** |
| full `pytest dask-ms/` | (deselected in CI) | **390 passed, 0 failed** |
| `--applications` `test_convert_application[zarr]` | blocked by `~/.cache` in sandbox | **passed** |

The one skip is `test_fasteners`: `fasteners` was a zarr 2 dependency that zarr 3 dropped, so the test's `importorskip` skips — upstream behaves the same way.

### Compatibility

Stores written after this change are still **zarr v2 format**, so zarr 2 readers and older dask-ms releases keep working. Reading a *pre-existing* store containing string columns now raises the clear "older dask-ms with zarr-python 2" error instead of an opaque zarr `ValueError` (upstream's #387 mitigation, ported with its regression test covering both consolidated and unconsolidated stores).
