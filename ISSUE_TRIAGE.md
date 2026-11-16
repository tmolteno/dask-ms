# GitHub Issue Triage — dask-ms

Date: 2026-11-02 · Scope: all **43 open issues** on the upstream tracker
([ratt-ru/dask-ms](https://github.com/ratt-ru/dask-ms/issues)), triaged against
this checkout (`tmolteno/dask-ms`, casacure-backend fork).

> Issues were disabled on `tmolteno/dask-ms` until now, so its own tracker is
> currently empty; every issue below lives upstream.

**Method.** All 43 bodies and their comment threads were fetched and read;
every verdict was checked against source in this checkout and, where the
dependency pins mattered, against the fork's *remote* `master`
(`9f40908` — the local worktree began 3 commits behind at `a0d78e7` and was
fast-forwarded during this session; those 3 commits are packaging/CI only: the
`dask-ms-2` rename, the trusted-publish workflow, and the CI `--deselect`).
Six issues were reproduced for real in throwaway
venvs (since deleted): **#387, #374, #347, #360, #314, #346**. The fork
publishes as **`dask-ms-2`** precisely because upstream is moving to `arcae`
while this line depends on `casacure`.

## Update — 2026-11-15 (delta since the triage above)

Scope: everything that moved on both trackers in the 13 days since, plus a
verification pass on the fork's first filed issue.

### Tracker deltas

| | 2026-11-02 | 2026-11-15 |
|---|---|---|
| Upstream open issues | 43 | **44** (only +#393; the 43 below had *zero* activity — no comments, no closures — so every verdict still stands) |
| Fork open issues | 0 | **1** — [#1](https://github.com/tmolteno/dask-ms/issues/1), filed today |
| Upstream release state | `0.3.0-dev` awaiting merge, CI red | **0.3.0 released 2026-11-13**, master CI green |

* **Upstream 0.3.0 is out** — strategic fact 1 above is now history. PR
  [#390](https://github.com/ratt-ru/dask-ms/pull/390) merged 2026-11-13T09:37Z
  (`2f4599f`), then "Bump to 0.3.0" (`8374058`); every master CI run since is
  green. The "upstream CI is red today" observation under #387 is stale.
  The arcae migration is therefore now the *released* upstream line, widening
  the fork's divergence — but it also means upstream's zarr-3 work is sitting
  on `master` ready to port (see #1 below).
* **#387 (P1 in the table below) — implemented upstream, shipped in 0.3.0.**
  Every substitution in the issue body is now in upstream's
  `daskms/experimental/zarr/__init__.py` (saved as
  `.issue-triage/upstream-0.3.0/experimental-zarr-init.py`): `VariableLengthUTF8`
  for strings (`:95`), `require_array` (`:131`), `FileNotFoundError` fallback
  (`:159`), `zarr.consolidate_metadata` (`:377`), `zarr_format=2` group opens;
  and upstream `pyproject.toml` now carries `zarr >=3.1.3` in the `katdal`,
  `zarr` and `complete` extras with no `numcodecs`/`xarray` caps. The `json2`
  old-store re-raise mitigation proposed at the end of #387 *is* implemented
  too — `xds_from_zarr` catches the `json2` `ValueError` and re-raises "written
  by an older dask-ms with zarr-python 2", covered by their new
  `test_zarr2_json_string_array`. So #387 is **complete** upstream and
  closeable as done.
* **#393 (new, 2026-11-13, informational)** — Python 3.15 blocked on missing
  cp315 wheels: python-casacore (tests), numcodecs (`zarr` extra) and h5py
  (`katdal` extra). sjperkins' follow-up comment retried the source builds on
  3.15.2 (pyyaml and cityhash *do* build) and concluded: stay at
  `requires-python <3.15`, no 3.15 CI. The fork already matches
  (`requires-python = '>=3.11,<3.15'`, `pyproject.toml:9`) → **no fork action**;
  close or use as a tracking issue upstream.

### Fork issue #1 — reproduced and sound, but two claims need correcting

Filed today against this fork; verified verbatim in `.venv`
(xarray 2026.9.0 + zarr 2.18.7 from the lock):

```console
$ .venv/bin/pytest -q daskms/experimental/zarr/tests/test_zarr.py
2 failed, 29 passed, 4 warnings in 31.51s
E   TypeError: open_group() got an unexpected keyword argument 'zarr_format'
  .venv/.../xarray/backends/zarr.py:1951: TypeError
```

Confirmed: the three extras pin `zarr (>=2.12.0, <3.0.0)` + `numcodecs <0.16.0`
(`pyproject.toml:29-30,37,43-45`) against the locked pair; upstream `master`
carries `zarr >=3.1.3` with no numcodecs bound; the lock's `numcodecs` is
0.15.2. Two claims in the body do **not** hold:

1. *"our own tests are already zarr-3-aware"* — **false**. The quoted comments
   ("zarr 3.0.8 and earlier did not support this property", "replace
   AssertionError after zarr-developers/zarr-python#2821") appear nowhere in
   this checkout (repo-wide grep) nor in upstream's `test_zarr.py` (fetched
   2026-11-15, saved as `.issue-triage/upstream-0.3.0/experimental-zarr-test_zarr.py`).
2. *the fix is the pin bump* — necessary, **not sufficient**. Verified against
   zarr 3.4.2: `daskms/experimental/zarr/__init__.py` is zarr-2-only, so a
   bump-only lock would fail differently and harder —
   * `:32` `import zarr.convenience as zc` → `ModuleNotFoundError` (module
     removed in zarr 3) → `xds_to_zarr` dies before writing;
   * `:109` `ds_group.require_dataset(..., object_codec=…)` →
     `Group.require_dataset` does not exist in zarr 3 (it is `require_array`);
   * `:73-75,:109-117` `dtype=object` + `numcodecs.JSON()` → zarr 3 rejects a
     bare object dtype and has no json2 codec on the v2 path; needs
     `VariableLengthUTF8`;
   * `test_zarr.py:98-99` assert `.zmetadata` → only survives if groups are
     opened with `zarr_format=2` (upstream does exactly this at `:158/:161`
     of its migrated file);
   * zarr 3.4.2 requires `numcodecs !=0.17.0, >=0.16.6` → the `<0.16.0` cap
     must go (the issue does say to drop it ✓);
   * old stores written with the `json2` codec stay unreadable under zarr 3
     (whole store fails when consolidated) → keep #387's proposed targeted
     re-raise in `xds_from_zarr` in scope.

**Real fix = upstream #387's plan, already executed upstream**: port
`.issue-triage/upstream-0.3.0/experimental-zarr-init.py`, bump the three
extras, re-lock, and delete the two CI `--deselect`s at
`.github/workflows/ci.yml:62-63` — they are why CI has never seen this
failure. Effort **M**, not the S a pin bump implies. As filed, the issue
understates the work and contains one fabricated detail; it should be
commented on or edited before anyone picks it up.

**Outcome — completed the same day.** Corrections posted as a
[comment on #1](https://github.com/tmolteno/dask-ms/issues/1#issuecomment-6892163756);
the port landed as commit `54c7449` on local branch `zarr3-migration`
(not yet pushed): both zarr files replaced with upstream's 0.3.0 versions —
including the `json2` re-raise and its `test_zarr2_json_string_array`,
which corrected an error in the first draft of this update (the mitigation
*is* implemented upstream) — `test_fsspec_store.py::test_local_store` moved
to `require_array`, extras bumped, `uv.lock` re-locked (zarr 3.4.2 / 3.1.6
on py3.11, numcodecs 0.17.1, +msgspec/+google-crc32c,
−asciitree/−fasteners), CI deselects removed, `HISTORY.rst` entry added.
Verification: `test_zarr.py` **33 passed / 1 skipped** (`test_fasteners` —
`fasteners` departed with zarr 2, skipped identically upstream), full suite
**390 passed / 0 failed**, `--applications` `test_convert_application[zarr]`
**passed**.

### Recommended actions

1. ~~**Fork #1**: comment/edit the two corrections above, then do the port
   (upstream file + extras + re-lock + drop deselects).~~ **Done 2026-11-15** —
   comment + port + verification above; commit `54c7449` on branch
   `zarr3-migration`, awaiting push/release.
2. **Upstream #387**: ping to close — fully implemented in 0.3.0, json2
   mitigation included.
3. **Upstream #380 / #295 / #293 / #277 / #213**: close recommendations from
   the table below are still unacted; if the fork account has no triage rights
   on `ratt-ru`, re-post as comments instead.
4. **Upstream #393**: no action needed from the fork; matches our pin already.
5. Consider a fork issue tracking the 0.3.0 divergence (arcae line released;
   what, if anything, the casacure fork rebases onto).

## Snapshot

| | |
|---|---|
| Open issues (upstream) | 43 |
| Open issues (this fork) | 0 |
| Touched in the last 12 months | 5 (#387, #380, #374, #373, #371) |
| Untouched for 3+ years | **33** of 43 |
| Labeled `bug` | 4 (#380, #314, #82, #32-wontfix) |
| Labeled `enhancement` | 14 |

Age distribution of the 43 open issues (by creation year):

| Created | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 |
|---|---|---|---|---|---|---|---|---|---|
| Count | 1 | 5 | 7 | 3 | 13 | 5 | 1 | 3 | 5 |

## Two strategic facts that frame everything

**1. Upstream replaced python-casacore with `arcae` on the `0.3.0-dev` branch
(PR #384, merged 2026-10-01 into `0.3.0-dev`, awaiting release via open PR #390
→ `master`).**
That PR deletes `daskms/table_proxy.py` and `daskms/table_executor.py` — the
`TableProxy`/`Executor` machinery — and replaces it with `daskms/casa_table.py`
(`CasaTable`, a `Multiton` over `arcae.Table`), because arcae is thread-safe and
releases the GIL. It also removes the row-run splitting and post-read resorting
that python-casacore's API forced on reads/writes. On that branch `reads.py` /
`writes.py` call arcae directly: `lazy_import("casacore.tables")` is gone.

`master` (5bc1071) is unchanged — it still has `table_proxy.py` and
`from daskms.table_proxy import TableProxy` — and this fork is based on it, so
today's fork is self-consistent. The risk is PR #390: once it merges, the
sys.modules alias in `daskms/casacure_backend.py` becomes a **no-op**, because
nothing imports `casacore.tables` any more.

This fork's newest commit, `a0d78e7` *"perf: flush once per column write, not
per proxied chunk call"*, optimises exactly the layer `0.3.0-dev` deletes, and
the fork's casacure strategy depends on python-casacore's API shape surviving.
Upstream and this fork are now on divergent backend architectures:

| | upstream (ratt-ru) | this fork (tmolteno) |
|---|---|---|
| Backend | `arcae` (ratt-ru/arcae) | `casacure` (tmolteno/casacure, Rust) |
| Selection | hard replacement in 0.3.0 | `DASK_MS_BACKEND=casacure` |
| Table handle | `CasaTable` multiton, n instances | `TableProxy` + per-table executor (kept) |
| Threading | parallel under shared locks | serialised onto one thread per table |

Any upstream issue whose fix lands in `table_proxy.py`/`table_executor.py` or
assumes arcae does not apply to this fork as written.

**2. The zarr 2 pin is actively breaking CI (issue #387).**
`pyproject.toml` still pins `zarr >=2.12.0, <3.0.0` and `numcodecs <0.16.0`, and
`daskms/experimental/zarr/__init__.py` still uses `zarr.convenience`,
`require_dataset` and `object_codec=...`. xarray 2026.9.0 requires zarr ≥3 for
its zarr backend, so the combination fails `test_xarray_to_zarr` and
`test_xarray_reading_daskms_written_dataset`. This checkout also lacks the
`xarray <2026.9.0` stop-gap cap that upstream PR #384 carries.

## Findings verified directly in this checkout

- **#380 — pyarrow test cases segfault flakily: already fixed.** PR #385
  (merged 2026-09-28) serialises reads on a shared `pq.ParquetFile`; the guard
  is present at `daskms/experimental/arrow/reads.py:103`. Safe to close.
- **#387 — move to zarr-python 3: applies verbatim, and is the highest-value
  open issue.** The issue body is effectively an implementation plan (exact API
  substitutions, back-compat analysis for old stores); all named code sites
  still exist unchanged here.
- **#373 — katdal datasets not pickleable: not ours.** Root cause is upstream
  katdal (ska-sa/katdal#392); nothing to fix in dask-ms.
- **casacure has no CI, no tests and no README mention.** `grep` finds
  `DASK_MS_BACKEND` only in `daskms/casacure_backend.py` and
  `daskms/__init__.py`; `.github/workflows/ci.yml` has no casacure job. The
  fork now publishes packages, so the shipped backend is untested in CI.

---

*(Per-issue triage below — groups A–D.)*

## Group B — experimental formats & docs (#285 … #213)

### #285 — Experimental clean-up release (2023-08-29, 6 comments) · actionable-enhancement · L
Umbrella tracker: drop the vendored `daskms/dataset.py` `Dataset` for xarray as a
core dep, use `open_zarr`/`to_zarr`, unify `__daskms_metadata__`, #273
consolidation, swap python-casacore for arcae, fold in fragments (#282).
Undone: xarray still an optional extra (`pyproject.toml:10,29`), vendored
`Dataset` still present (`daskms/dataset.py:238+`), zarr keeps its private
`__daskms_zarr_attr__`, `DASKMS_METADATA` only consumed by arrow
(`daskms/constants.py:2`), no `xds_to_ms`. Only the fragments bullet landed
(PR #282, 2023-09-19). **Flag:** the "swap to arcae" bullet is the exact move
this fork declines; treat as a coordination issue, not a to-do.

### #283 — `xds_to_zarr` fails on large chunks (2023-08-14, 0 comments) · actionable-enhancement · S
`ValueError: ... exceed zarr's 2GiB chunk limit` still raised
(`daskms/experimental/zarr/__init__.py:98-107`, asserted by
`zarr/tests/test_zarr.py:395`); `maybe_rechunk` (:171) only reconciles against
*existing* on-disk chunks. `rechunk_by_size` (`experimental/utils.py:126`,
PR #284) landed days later and the error message points at it — but nothing
auto-applies it for a new store, so the reporter's exact call still fails.

### #277 — chi² gradient parallelisation (2023-04-01, 4 comments) · stale-likely-closable · S
User's own dask/numba code OOMs; all problematic code is in their notebook.
Maintainers already answered (`blockwise(..., concatenate=True)`; codex-africanus
wgridder/`im2residim`). No in-repo change, no follow-up since 2023-04-19.

### #273 — Consolidate MS support into xds_{from,to}_ms (2022-12-05, 1 comment) · actionable-enhancement · M
`daskms/dask_ms.py` still has `xds_to_table` (:18), `xds_from_table` (:128),
`xds_from_ms` (:297) and **no `xds_to_ms`**. `xds_from_ms` hardcodes
`_DEFAULT_GROUP_COLUMNS = ["FIELD_ID","DATA_DESC_ID"]`, `table_schema="MS"`,
so `foo.ms::SPECTRAL_WINDOW` through it still fails; subtables need
`xds_from_table` (documented `dask_ms.py:146-177`); no test opens a subtable
via `xds_from_ms`. Only the storage-dispatch layer (`dask_ms.py:391,416`) was
added. **Flag:** any refactor runs through `table_proxy.py`'s
`lazy_import("casacore.tables")` → must hold for casacure too.

### #272 — read, join, write MS (2022-12-01, 6 comments) · stale-likely-closable · S
Single-colon `x.ms:SPECTRAL_WINDOW` fails by design (`table.py:24` maps `::` →
`os.sep`; `utils.py:130 table_path_split`); the traceback `KeyError` is a
normal WeakValueDictionary cache miss (`table_proxy.py:159-169`). Thread
resolved to "`::` + `xds_from_table(..., group_cols='__row__')`", which
`dask_ms.py:146-177` still documents; SPW-merging answered as bookkeeping/xova.

### #235 — Dataset versioning (2022-07-20, 20 comments) · actionable-enhancement · L
Nothing implements it: `daskms/constants.py` has only `DASKMS_METADATA` /
`DASKMS_PARTITION_KEY`; zarr attrs are `__daskms_zarr_attr__`
(`experimental/zarr/__init__.py:39`); "parent dataset" appears only as a
fragment target (`experimental/fragments/__init__.py:207`). Design converged on
"metadata links to all previous datasets" but dormant since 2022-08-09.

### #227 — Document experimental Zarr and Arrow formats (2022-07-04, 0 comments) · actionable-enhancement · S
Implementations ship (`experimental/zarr/__init__.py`,
`experimental/arrow/`, dispatch `dask_ms.py:366-439`) but
`grep -ril "zarr" docs README.rst` and `arrow|parquet` return **nothing**;
`docs/api.rst` lists only the three core functions; `docs/usage.rst` is 7 lines.

### #226 — Document dask-ms convert (2022-07-04, 0 comments) · actionable-enhancement · S
The CLI exists and is actively maintained: `apps/entrypoint.py` (click group),
`apps/convert.py` (`-f {ms,casa,zarr,parquet}`, `--chunks`, `--taql-where`,
`--exclude`, `--force`), registered at `pyproject.toml:17-19`; HISTORY.rst
cites many convert PRs — but no doc page mentions it at all.

### #225 — Document performance tuning (2022-07-04, 0 comments) · actionable-enhancement · M
All the knobs are real and undocumented: row chunks (`reads.py`
`_DEFAULT_ROW_CHUNKS`, `DatasetFactory.chunks`), ordering/TAQL
(`ordering.py`, `query.py`), storage managers (`descriptors/`). Only perf
prose is one sentence at `docs/tutorial/reads.rst:306`. **Flag:** must now
document *two* backends, and `docs/installation.rst` still presents
python-casacore as a hard dependency — stale after the optional-backend change.

### #223 — Reintroduce warnings for non-contiguous data (2022-06-28, 0 comments) · actionable-enhancement · S
`row_run_factory` (`ordering.py:18`) computes `start_and_len` with no warning;
the put loop (`writes.py:42,62,84,103`) never logs fragmentation — the only
warnings there (`writes.py:528,544,555`) are about ROWID *graph shape*.
History confirms the regression: `test_fragmented_ms`/`test_unfragmented_ms`
were dropped in `4e38390` ("0.2.0 alpha2 Fixes (#48)") and never restored.
**Flag:** insertion point sits next to this fork's `a0d78e7` flush change and
runs per chunk per column inside the shared executor — use rate-limited
`logging`.

### #213 — Map correlations to Stokes without looping (2022-04-30, 4 comments) · stale-likely-closable · S
Usage question; the loop is in the user's pyralysis code. Answered with
`africanus.model.coherency.dask.convert`; no repo action, silent since 2022-07-04.

## Group C — storage API, docs & casacore (#212 … #109)

### #212 — Quiet mode for `xds_from_storage*` (2022-04-26, 0 comments, `enhancement`) · actionable-enhancement · S
Warnings fire exactly as described: `utils.py:254-268` (`filter_kwargs` warns on
every ignored kwarg), `arrow/reads.py:154-158` (once per partition),
`arrow/reads.py:227-231`, `arrow/writes.py:110-113`,
`experimental/zarr/__init__.py:311-314,420-423`. `grep quiet|silence` → zero
hits; the warnings were added deliberately by PR #192. Any new flag must be
registered in `filter_kwargs`' known-args or it warns about itself.

### #211 — Rechunk non-row axes in `xds_from_parquet` (2022-04-26, 0 comments) · actionable-enhancement · M
`partition_chunking()` still says "We only handle row chunking at present, warn
the user" (`arrow/reads.py:136-158`) and drops non-row dims;
`xds_from_parquet` (`arrow/reads.py:215+`) validates `chunks` but never applies
them. The limitation is codified in the test suite: `arrow/tests/test_parquet.py:78-96`
parametrises `{"row": 2, "chan": 4}` and *asserts* the UserWarning — a fix
needs a test rewrite.

### #206 — Make xarray a default dependency (2022-04-21, 2 comments) · actionable-enhancement · L
Not done: `pyproject.toml:10` core deps exclude xarray; `:29` keeps it an extra;
`daskms/dataset.py:15-18,104-106,238+` still carries the dual implementation
("Exists to allow xarray to be an optional dask-ms dependency"); tests still
skip on availability (`tests/test_storage.py:13`, `tests/test_dataset.py:31-35,546`).
JSKenyon notes downstream `xova` relies on `daskms.Dataset` — an unexecuted
deliberate breaking change.

### #193 — Document `xds_from_storage_*` (2022-03-30, 0 comments) · actionable-enhancement · S
`dask_ms.py:366` `xds_from_storage_table`, `:391` `xds_from_storage_ms`,
`:416` `xds_to_storage_table` have **no docstrings**; `docs/api.rst:7-14`
autodocs only the three core functions; `grep -rn "storage" docs/` → nothing.
Still entirely undocumented 3+ years on.

### #190 — File locks in a distributed environment (2022-03-24, 0 comments) · needs-repro · L
Design sketch only — no failure reported. Locking model unchanged:
`table_executor.py:39-51` (one `ThreadPoolExecutor(1)` per table key — the
block the issue cites), `table_proxy.py:98-125,177-239` READ/WRITE lock
runners; `CASACORE_TO_CASA_RS.md:19-24,70-75` notes `reads.py`/`writes.py`
bypass the proxy and do their own `lock/unlock/flush`. Needs a demonstrated
cross-node lock failure first.

### #167 — Transform to a gridded dataset (2021-10-03, 5 comments) · stale-likely-closable · S
No gridding code anywhere (grep grid/ducc/wgridder → nothing); the snippet uses
the reporter's own modules. Answered: gridding lives in ducc0 wgridder /
pfb-clean — out of scope.

### #159 — Baseline information (2021-05-05, 19 comments) · already-fixed · S
Baseline grouping always worked: `reads.py:520-537` runs `group_ordering_taql`
for *any* group column. The crash that derailed the thread ("Data manager name
StandardStMan is already used .../POINTING") is now guarded by
`writes.py:335-347` (`odminfo` NAME check), added in `482873a` "Support
optional CASA columns (#270)" on 2022-11-11 — which also closed #271. Verify
the round trip and close.

### #153 — `xds_from_table` on 'record' columns (2021-03-19, 3 comments) · actionable-bug · S
`_TABLE_TO_PY` (`columns.py:15-36`) has no `record` entry; `infer_dtype` raises
(`columns.py:69-80`) from a call site outside any try (`:147`), and
`reads.py:230-237` only catches `ColumnMetadataError` — so the ValueError
escapes and the whole read dies instead of taking the existing "Ignoring
'<col>'" path (the pattern from `8609796` "Ignore non-existent table columns
(#290)"). Maintainer kept it open deliberately. **Flag:** fix belongs in
backend-agnostic `columns.py`, so it hardens casacure too — but what casacure
returns as `valueType` for records is unverified.

### #121 — Improve multi-ms functionality (2020-08-12, 8 comments) · stale-likely-closable · S to close
`grep -rni "multi-ms|mms"` → zero hits; reads always go through one
`TableProxy`. Maintainer workaround documented (`list(chain(map(xds_from_ms,
mms)))`), blockers are upstream (casacore#1038 thread-safety, python-casacore#209
GIL), silent since 2021. **Flag:** a GIL-releasing Rust backend changes that
calculus — a possible differentiator for this fork.

### #111 — Track column slicing improvements in casacore (2020-04-24, 0 comments, `enhancement`) · upstream-not-us · S/L
dask-ms's side is a deliberate workaround: `reads.py:251-254` still calls
`table_proxy.setmaxcachesize(column, 1).result()` with comments citing
`ratt-ru/dask-ms#92` and `casacore/casacore#1018`; `CASACORE_TO_CASA_RS.md:114`
records it as "ideally a no-op in the replacement". **Flag:** under
`DASK_MS_BACKEND=casacure` this executes on every multi-chunk column read, so
`casacure.tables.Table.setmaxcachesize` must exist (or be `hasattr`-guarded)
or sliced reads AttributeError — untested here; worth a targeted casacure test.

### #109 — Best way to apply a channel slice (2020-04-24, 7 comments, `question`) · stale-likely-closable · S
Answered in 2020 with a recipe that still matches the code: multi-chunk
non-row dims take the `getcolslice` path (`reads.py:58-68,155,244-254`,
inclusive ends from `columns.py:280-297`). No helper from the referenced PR #86
ever landed; residue is a docs example (overlaps #193).

## Group D — old guard: profiling, writes, docs (#106 … #19)

### #106 — Add dask-ms profiling functionality (2020-04-21, 2 comments) · actionable-enhancement · M
No instrumentation exists: `grep DASK_MS_PROFILE|profil|perf_counter|atexit`
under `daskms/` → zero hits, while the code it wants to instrument is intact
(`table_proxy.py:48-60` method table, `:387 submit`; `table_executor.py:42`
single-thread pool). The config hook it deferred to now exists —
`daskms/config.py` `Config("dask-ms")`, currently only read by `fsspec_store.py`.

### #101 — Leverage complete MS table/subtable definitions (2020-04-16, 0 comments) · already-fixed · S
Already the default: `descriptors/ms.py:35-36` calls `ct.complete_ms_desc()` /
`ct.required_ms_desc()`, `ms_subtable.py:22-23` likewise, and `descriptor()`
(`ms.py:111-115`) copies from the complete desc before falling back to
`variable_descriptor`. Introduced by `482873a` (#270). **Flag:** under
casacure, casacure must also export both functions (unverified — casacure isn't
installed here) or MS creation breaks.

### #99 — Documentation improvements (2020-04-15, 0 comments) · actionable-enhancement · S
Live typo `docs/tutorial/writes.rst:27` ("overwriting data **or or**
performing"); API docstrings for `xds_from_table`/`xds_from_ms` never mention
`attrs`, though `reads.py:417-424` stores group-col values there as Python
ints; the subtable-join ask is already satisfied (`docs/tutorial/reads.rst:235-266`).
No doctest/spellcheck target in `Makefile` or CI.

### #93 — Write fewer correlations than present in the MS (2020-03-10, 4 comments) · actionable-enhancement · L
Extents still come from the dataset's own chunks: `writes.py:646-676` builds
them via `dim_extents_array` (`columns.py:265`), a cumulative range anchored at
`start = 0` — so a 2-corr subset writes on-disk corr 0-1, never the selected
indices. `ndarray_putcolslice` (`writes.py:75`) already issues ranged
`putcolslice(column, data, blc, trc, ...)`; only row/chunk addressing exists.
Reads assign no coords (`reads.py:434-482`), so the issue's "paper trail"
doesn't exist. sjperkins' stated limitation ("ranged writes on `row` only")
still describes the code exactly.

### #82 — Tutorial needs updating (2019-12-12, 1 comment, `bug`) · already-fixed · S
Both tracebacks were fixed the same day by `fcb92c5` "Variable conversion and
check fixes (#83)": `dataset.py:92` converts `xr.DataArray`/`xr.Variable`, and
`writes.py:640-644` raises a clear `TypeError: ... is not a dask Array`. The
tutorial snippet and the poor-man's `Dataset` branch still exist
(`docs/tutorial/writes.rst:69-79`, `dataset.py:107+`), but no CI runs doctests.

### #77 — Support dask's process scheduler (2019-12-09, 0 comments, `enhancement`) · actionable-enhancement · M
`table_executor.py:39-50` always builds `cf.ThreadPoolExecutor(1)`; no
`DummyExecutor` anywhere. Partial mitigation landed: `Executor.__reduce__`
(`:49-50`, `0de0e37` #88) pickles only the key. No opt-out knob —
`DatasetFactory.__init__` rejects unknown kwargs (`reads.py:323-324`) and
donfig has no executor setting. Tests cover spawn-child reads and a `LazyProxy`
graph (`tests/test_patterns.py:111`) but never an xds graph in-parent.
**Flag:** fork safety is backend-specific — test both `DASK_MS_BACKEND` values.

### #69 — Incorrect data type for column keyword (2019-11-05, 4 comments, `enhancement`) · actionable-bug · S
Still unfixed: `_put_keywords` (`writes.py:723-739`) passes `v` verbatim to
`table.putcolkeyword(column, k, v)` — no Python→numpy cast, exactly the
`TableRecordRep::defineDataField - incorrect data type` gap. Read side returns
raw dicts (`reads.py:294`). Tests only round-trip a *string* keyword
(`tests/test_dataset_keywords.py`), never an int. Backend-neutral fix site.

### #67 — `force_write` option for xds_to_table (2019-10-21, 0 comments, `enhancement`) · actionable-enhancement · M
`_updated_table` (`writes.py:311-349`) only adds *missing* columns; there is no
dtype/shape consistency check anywhere (only the ROWID chunk check,
`writes.py:663-665`), `removecol` appears nowhere (`removecolkeyword` only,
`writes.py:735`), and `xds_to_table`'s signature (`dask_ms.py:18-26`) has no
force flag. **Flag:** the drop/re-add path needs `removecol`/`addcols` in
casacure too.

### #32 — casacore exception if temp MS deleted (2019-05-21, 1 comment, `bug`+`wontfix`) · stale-likely-closable · S
The workaround *is* the current code: `conftest.py:141-143` has teardown
commented out (`# shutil.rmtree(str(msdir))`, with an inline link to the
issue), same for `big_ms` (:85) and `spw_table` (:185); the fixture is no
longer session-scoped. Labelled `wontfix`, no reproduction path remains.

### #19 — Set MS index arrays to be coordinates (2018-09-01, 1 comment) · actionable-enhancement · M
Only `ROWID` is promoted (`reads.py:355-363`, `:407-413`); index columns stay
data_vars; `set_coords` appears nowhere, including the poor-man's `Dataset`
(`dataset.py:238+`). Writes read `ds.data_vars[column]` (`writes.py:632`), so
promoting index cols to coords breaks writing them unless the write path also
consults coords (contrast `experimental/utils.py:72-94`, which already does).
The read/write asymmetry is the design work — and it interacts with open PR
#292 "dimension ID coordinates (ROWID++)" (open since 2023-09-28).

## Group A — recent issues (#387 … #293)

### #387 — Move daskms.experimental.zarr to zarr-python 3 (2026-09-30, 0 comments, `enhancement`+`dependencies`) · actionable-enhancement · M · **top priority**
The body is effectively a ready implementation plan: zarr 3 writing v2 via
`zarr_format=2`, the compatibility matrix for each dtype class, and the exact
substitutions — `require_dataset`→`require_array` (`zarr/__init__.py:109`),
`zarr.convenience.consolidate_metadata`→`zarr.consolidate_metadata` (`:32`,
`:360`), `GroupNotFoundError`→`FileNotFoundError` (`:133`), string columns via
`VariableLengthUTF8` instead of `dtype=object` + `numcodecs.JSON()` (`:75,:115`),
and dropping the `zarr<3`/`numcodecs<0.16` caps — which also sit in the
`katdal` and `complete` extras (`pyproject.toml:26-27,33,39-40`).
**Reproduced both halves:** py3.13 + zarr 2.18.7 + xarray 2026.9.0 yields the
exact CI error `TypeError: open_group() got an unexpected keyword argument
'zarr_format'`; under zarr 3.4.2, `xds_to_zarr` dies with `ImportError: No
module named 'zarr.convenience'`. Upstream `master` CI is red today (run
41378344930, "Test dask-ms complete" failed on 3.11/3.12/3.13), and your fork's
remote `master` works around it with `--deselect` (`9f40908`). Old stores
written with the `json2` object codec can't be read by zarr 3 — the issue
proposes a targeted re-raise in `xds_from_zarr` as a cheap mitigation.

### #380 — pyarrow test cases segfault flakily (2026-07-20, 0 comments, `bug`) · already-fixed · S
Root cause (apache/arrow#50471, concurrent reads on a shared
`pq.ParquetFile`) was fixed by PR #385 (merged 2026-09-28): `read_column` holds
`self.lock` around `file_.read(...)` (`daskms/experimental/arrow/reads.py:100-105`).
The `8ff2628` commit message records 19/30 local crashes on pyarrow 25.0.1
before the lock and 30/30 after; HISTORY.rst carries the entry. No pyarrow pin
needed. **Close it.**

### #374 — Fusion failure when using clone (2026-06-11, 3 comments) · actionable-bug · M
**Reproduced** on dask 2026.8.0 against the fixture SPECTRAL_WINDOW table:
`ValueError: Cannot fuse tasks with multiple outputs {('read~CHAN_FREQ~SPECTRAL_WINDOW-…',0,0),
'finalize-hlgfinalizecompute-…'}` — for both `clone(x)[0]` (the reported case)
and plain `clone(x)`; `optimize_graph=False` hides it. Mechanism:
`reads.py:275-284` flattens every column read into one materialised layer via
`inlined_array`, `optimisation.py:215-349` does the `inline`/`cull`/
`Task.substitute` work, and the `finalize-*` keys come from
`daskms/dataset.py:218/493`. The raise itself is in dask's
`fuse_linear_task_spec`, so a small upstream dask issue may be needed too.
No `clone` or `optimize_graph` usage exists anywhere in the test suite — no
regression coverage. Backend-independent (pure graph), but reproduce under
casacure as well since the fixture goes through `TableProxy`.

### #373 — katdal-backed datasets not pickleable (2026-05-18, 0 comments) · upstream-not-us · S
Root cause is katdal (ska-sa/katdal#392): `experimental/katdal/msv2_facade.py:161-163`
builds arrays straight from katdal's `DaskLazyIndexer`, and there is no
`__reduce__`/pickle support anywhere under `daskms/experimental/katdal/`.
dask-ms could only add a picklable facade, which is a katdal API question.
Note the `katdal` extra also carries the `zarr<3` pins — collateral for #387.

### #371 — Remove low-level graph construction (2026-04-21, 0 comments, `enhancement`) · actionable-enhancement · L
Tracks replacing hand-built dict graphs after #368 fixed the acute breakage —
and the issue's premise verifies verbatim. Raw tuple layers are still built in
`ordering.py:86-93,136-138`, `writes.py:467-477`, `columns.py:294-311`, all
recombined with `HighLevelGraph.from_collections`; `optimisation.py:26-28`
says outright that "dask-ms itself still builds some graph layers with
old-style (func, *args) tuples" and maintains **dual** legacy/new-dask code
paths (`:240-265`, `:279-343`) — that shim is the maintenance cost the issue
argues against. Consumers: `writes.py:566,695`, `ordering.py:95`,
`experimental/zarr/__init__.py:261,508`, `experimental/arrow/writes.py:163`.
`da.blockwise` is already used for the real read/write work (`reads.py:275`,
`writes.py:682`); `daskms/expressions.py` is unrelated (ast→dask.array visitor,
test-only). #374 is the user-visible bug this would eliminate.

### #360 — ROWID on an empty table should throw a better error (2025-10-24, 4 comments) · actionable-bug · S
**Reproduced:** with a `ROWID` coordinate → `RuntimeError:
TableProxy::TableProxy::putColumn: no such row`; the identical dataset without
ROWID → succeeds (3 rows written). Semantics are documented (ROWID ⇒ update,
not append), maintainer confirmed "That is buggy, could probably throw a nicer
error message". Code: ROWID routes to the update branch
(`writes.py:585-619`), the only shape check compares ROWID to the data array
(`:654-665`), and `table.nrows()` is consulted only in `_add_row_wrapper`
(`:353`) — a `nrows()` pre-check in `_write_datasets` would turn the crash into
a clear `ValueError`. The maintainer's cited test writes subtables *without*
ROWID (`tests/test_ms_creation.py:165-200,307`), which is why it passes.
**Flag:** the message comes from the backend, so casacure would say something
else — validate in dask-ms and test both backends.

### #347 — per-xds chunking ignored when grouping by `__row__` (2025-06-03, 3 comments) · actionable-bug · S
**Reproduced** on a 3-SPW table (8/16/32 channels) with
`chunks=[{'chan':(4,4)},{'chan':(4,)*4},{'chan':(4,)*8}]`:
`ValueError: Chunks do not add up to shape. Got chunks=((4, 4),), shape=(16,)`
— `chunks[0]` is applied to SPW 1. Cause: `_single_dataset` always passes
`self.chunks[0]` (`reads.py:351,365`) and the `__row__` branch does
`dict(self.chunks[0], row=1)` (`:499`); only `_group_datasets` indexes per
group (`:388-390`). With auto dims the requests are silently ignored instead of
erroring. Fix: index `self.chunks[r]` in the `__row__` path plus a
heterogeneous-SPW test (reproducer left at `.issue-triage/repro_347.py`).
Note the second half of the thread — whether heterogeneous SPWs should be
supported at all — is still an open design question.

### #346 — w-projection / w-ordering pre-processing (2025-05-31, 3 comments) · actionable-enhancement · L (S for the sub-fix)
**Confirmed the maintainer's suggestion fails as written:**
`index_cols=["UVW[:,2]"]` → TaQL `parse error at or near position 75 ','`,
while `index_cols=["UVW[2]"]` **works** on a table with a `UVW` column. So a
documented/expression-tolerant path through `query.py:11-15` (index_cols spliced
verbatim into TaQL `ORDERBY`) and `ordering.py:64-79,102-117,165-191`
(`GROUP_<col>` aggregations that assume plain column names) is a real, small
win. The true distributed W-sort stays L (dask/dask#4368). **Flag:** ordering
runs via `ct.taql` (`table_proxy.py:182`), so any expression feature must be
verified against casacure's TaQL.

### #314 — Capitalised ROWID breaks xarray lowercase coordinate convention (2024-03-20, 0 comments, `bug`) · actionable-bug · S
The behaviour is *codified* in a test comment: `experimental/zarr/tests/test_zarr.py:444-447`
"capitalised ROWID breaks the lowercase xarray coordinate naming convention so
xarray treats it as a data variable". The store writes `ROWID` uppercase
(`zarr/__init__.py:109,124`) with `coordinate: True` living only in dask-ms's
private `__daskms_zarr_attr__` (`:120-126`, read back `:486/:510`), and
`reads.py:356-360` matches. Cheapest fix is emitting a CF `coordinates`
attribute; renaming to `rowid` would break existing stores and the write path
(`writes.py:586,592` keys on `"ROWID" in ds.data_vars` for update-vs-append).
Best coordinated with open PR #292 "dimension ID coordinates (ROWID++)".

### #295 — Test suite fails on pyarrow 13.0.0 due to pandas (2023-10-27, 0 comments) · already-fixed · S
The surprise transitive dependency is now a declared one:
`pyproject.toml:22` `arrow = ['pandas >=2.1.2', 'pyarrow >=14.0.1']`, bumped by
`1268329` "Upgrade to pyarrow 14.0.1 (#299)" (`HISTORY.rst:120`), and CI
installs `.[arrow,testing,zarr]` so pandas is always present. apache/arrow#38428
is long resolved; `daskms/tests/` imports no pandas directly. **Close it.**

### #293 — Performance issue in bispectrum method and gradient (2023-10-04, 12 comments, last 2025-05-19) · stale-likely-closable · S
Support thread, not a defect: the failure is in the reporter's own
`da.blockwise(..., adjust_chunks={'t': (utime_size,)})` contract, and both
maintainers supplied working rewrites across 12 comments (NaN chunk sizes;
`concatenate=True` / list handling for reductions). dask-ms is only used to
*read* the MS; chunking flows normally through `reads.py:302-315` /
`columns.py:254-259`. Close with links, or fold into a docs FAQ.

### #293 — Performance issue in bispectrum method and gradient (2023-10-04, 12 comments, last 2025-05-19) · stale-likely-closable · S
Support thread, not a defect: maintainers established the code saturates all
cores as intended and is a slow-by-nature DFT (pointing at crystalball /
codex-africanus), and the 2025 follow-up about `adjust_chunks={'t': …}`
forcing a single chunk was answered (`concatenate=True` / list-handling in
`blockwise`). All code in the thread is the reporter's own. Close with links,
or convert to a docs FAQ entry.

## Master table — all 43 open upstream issues

| # | Title (short) | Verdict | Effort |
|---|---|---|---|
| 387 | zarr-python 3 migration | actionable-enhancement **(P1)** | M |
| 380 | pyarrow segfault flakily | already-fixed → close | S |
| 374 | Fusion failure with clone | actionable-bug | M |
| 373 | katdal datasets not pickleable | upstream-not-us | S |
| 371 | Remove low-level graph construction | actionable-enhancement | L |
| 360 | ROWID on empty table error message | actionable-bug | S |
| 347 | chunking ignored with `group_cols="__row__"` | actionable-bug **(reproduced)** | S |
| 346 | w-projection / w-ordering | actionable-enhancement | L (S sub-fix) |
| 314 | Capitalised ROWID vs xarray convention | actionable-bug | S |
| 295 | pyarrow 13 pandas dep | already-fixed → close | S |
| 293 | Bispectrum perf question | stale-likely-closable → close | S |
| 285 | Experimental clean-up release | actionable-enhancement | L |
| 283 | `xds_to_zarr` large chunks | actionable-enhancement | S |
| 277 | chi² gradient parallelisation | stale-likely-closable → close | S |
| 273 | Consolidate into `xds_{from,to}_ms` | actionable-enhancement | M |
| 272 | read, join, write MS | stale-likely-closable → close | S |
| 235 | Dataset versioning | actionable-enhancement | L |
| 227 | Document Zarr/Arrow formats | actionable-enhancement | S |
| 226 | Document `dask-ms convert` | actionable-enhancement | S |
| 225 | Document performance tuning | actionable-enhancement | M |
| 223 | Fragmented-row warnings | actionable-enhancement | S |
| 213 | Correlations→Stokes question | stale-likely-closable → close | S |
| 212 | Quiet mode for storage reads | actionable-enhancement | S |
| 211 | Non-row rechunk for parquet | actionable-enhancement | M |
| 206 | xarray as default dependency | actionable-enhancement | L |
| 193 | Document `xds_from_storage_*` | actionable-enhancement | S |
| 190 | Distributed file locks | needs-repro | L |
| 167 | Gridded dataset question | stale-likely-closable → close | S |
| 159 | Baseline information | already-fixed → close | S |
| 153 | `record` column crashes reads | actionable-bug | S |
| 121 | Multi-MS functionality | stale-likely-closable → close | S |
| 111 | casacore column slicing tracking | upstream-not-us | S |
| 109 | Channel slice question | stale-likely-closable → close | S |
| 106 | Profiling functionality | actionable-enhancement | M |
| 101 | Complete MS definitions | already-fixed → close | S |
| 99 | Documentation improvements | actionable-enhancement | S |
| 93 | Write fewer correlations | actionable-enhancement | L |
| 82 | Tutorial needs updating | already-fixed → close | S |
| 77 | Process scheduler support | actionable-enhancement | M |
| 69 | Column keyword dtype | actionable-bug | S |
| 67 | `force_write` option | actionable-enhancement | M |
| 32 | Temp MS deletion exception | stale-likely-closable → close | S |
| 19 | Index arrays as coordinates | actionable-enhancement | M |

Verdict totals across the 43:

| Verdict | Count |
|---|---|
| actionable-enhancement | 21 (7 S · 8 M · 6 L) |
| actionable-bug | 6 (5 S · 1 M): #374, #360, #347, #314, #153, #69 |
| already-fixed → close | 5 (#380, #295, #159, #101, #82) |
| stale-likely-closable → close | 8 (#293, #277, #272, #213, #167, #121, #109, #32) |
| upstream-not-us | 2 (#373, #111) |
| needs-repro | 1 (#190) |

So: **12 quick wins (S)**, 15 actionable at M/L, **13 closable today**, 3 not
actionable at all. Six issues were **reproduced in throwaway venvs during this
triage**: #387, #374, #347, #360, #314, #346 (reproducer for #347 kept at
`.issue-triage/repro_347.py`).

## Recommended actions

### Upstream (ratt-ru/dask-ms) — triage hygiene
1. **Close 13 issues** with evidence: `#380` (fixed by #385), `#101`, `#82`,
   `#159` (all fixed in-tree), `#32` (wontfix + workaround committed), and
   `#295`, `#277`, `#272`, `#213`, `#167`, `#121`, `#109`, `#293` (answered or
   obsolete). That cuts the open list from 43 → 30 and removes 13 years of
   noise from the default view.
2. **#387 is the one that matters now** — it is red on upstream `master` CI
   today. Your `--deselect` stop-gap is fine locally, but the migration itself
   (zarr 3, `zarr_format=2` output) is fully specified in the issue body.

### This fork (`tmolteno/dask-ms`, published as `dask-ms-2`) — issues worth filing in your new tracker
1. **casacure has zero CI, zero tests, zero docs.** No `DASK_MS_BACKEND`
   reference outside `casacure_backend.py`/`__init__.py`, no casacure job in
   `ci.yml`, no README mention — while the package is published to PyPI
   (v0.2.33, trusted publishing). Highest-value local issue: a CI matrix leg
   running the suite under `DASK_MS_BACKEND=casacure`.
2. **Backend-API surface audit for casacure.** Concrete gaps found during
   triage that will `AttributeError` under casacure if unsupported:
   `setmaxcachesize` (called on every multi-chunk read, `reads.py:251-254`),
   `complete_ms_desc`/`required_ms_desc` (MS creation, `descriptors/ms.py:35-36`),
   `removecol`/`addcols` (any `force_write` work), `getcolkeyword` dtype
   fidelity (#69), and `valueType` for `record` columns (#153).
3. **Track divergence from upstream `0.3.0-dev`.** PR #390 would merge arcae
   into upstream `master`, deleting `table_proxy.py`/`table_executor.py` — the
   layer your `a0d78e7` perf commit optimises, and the layer casacure's
   `sys.modules` alias depends on. Decide deliberately: rebase onto 0.3.0 +
   re-target casacure, or maintain `dask-ms-2` as an independent line.
4. **Re-point or withdraw upstream PR #383** (open 6 weeks, unreviewed,
   no conflicts): after #384 its premise ("keep python-casacore's API, swap the
   implementation") no longer matches upstream direction.
5. **Fix `docs/installation.rst`** — it still presents python-casacore as a
   hard dependency, which is wrong for `dask-ms-2` (upstream issue #225 flags
   the same page).

### Small upstream PRs that are safe, backend-neutral wins
`#69` (numpy cast in `_put_keywords`), `#153` (graceful `record` skip in
`columns.py`), `#360` (clear ROWID/empty-table error), `#347` (index
`self.chunks[r]` in the `__row__` path), `#283` (auto-apply `rechunk_by_size`
for new stores), `#223` (fragmentation logging), and the docs cluster
`#193`/`#226`/`#227`/`#99`.

### Best next actions, ranked
1. **#387 (M)** — only issue proven to break CI on every Python version, and
   its body is a complete, pre-verified implementation plan.
2. **#347 (S)** — hard `ValueError` reproduced in minutes; one indexing change
   in `daskms/reads.py` + a heterogeneous-SPW test.
3. **#360 (S)** — cryptic backend crash reproduced; maintainer already agreed
   on the desired behaviour; a `nrows()` pre-check serves casacore *and*
   casacure.
4. **Close the 13** — pure triage hygiene, no code.
5. **File the five fork-local issues** — casacure CI, backend API surface
   audit, 0.3.0-dev divergence, PR #383 re-pointing, `installation.rst`.
