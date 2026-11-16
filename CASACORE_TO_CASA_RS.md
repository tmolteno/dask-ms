# CASACORE_TO_CASA_RS — Swapping the dask-ms backend from casacore to a Rust table system

## Goal

Replace `python-casacore` (the C++ casacore table system) as the I/O backend of
dask-ms with a Rust implementation of the CASA table format (e.g. a casa-rs /
casatables-style crate), keeping the dask/xarray user-facing API unchanged.

This document inventories every piece of casacore functionality dask-ms relies
on, identifies the swap points in the codebase, and sequences the work.

## Key architectural facts

- **Only `casacore.tables` is used.** There is no use of `casacore.measures`,
  `casacore.quanta`, `casacore.images`, or any other casacore module anywhere
  in the package. The Rust replacement only needs the *tables* subsystem.
- **The abstraction layer is thin.** `daskms/table_proxy.py` (`TableProxy`, a
  picklable multiton wrapping the table in a per-table single-threaded
  `Executor` from `daskms/table_executor.py`) defines the method/locking
  contract — but the hot I/O paths in `daskms/reads.py` and `daskms/writes.py`
  bypass the proxied methods, binding `getcolnp`/`putcol`/etc. directly off the
  raw table object and doing their own `lock`/`unlock`/`flush`. A replacement
  backend must therefore implement the raw-table object contract, not just the
  proxy method list.
- **Most of the codebase is backend-agnostic.** `columns.py`, `ordering.py`,
  `query.py`, `dataset.py`, `dataset_schema.py`, `table_schemas.py`, and the
  experimental zarr/arrow/katdal stores operate on dicts and numpy arrays.
  `daskms/fsspec_store.py:59-82` already dispatches between store types by
  on-disk signature (`table.dat` → `"casa"`, `.zgroup` → `"zarr"`,
  `.parquet` → `"parquet"`), and `daskms/dask_ms.py:366-437`
  (`xds_from_storage_*`/`xds_to_storage_*`) does backend dispatch — a Rust
  backend can be registered as another store type here.

## Swap points (files that would change)

| File | Role |
|---|---|
| `daskms/table_proxy.py` | Backend contract: factory, locking, proxied method list, pickling |
| `daskms/reads.py` | Direct bound-method read paths (`getcolnp`, `getcolslicenp`, `getcol`, `getcolslice`) |
| `daskms/writes.py` | Direct bound-method write paths (`putcol`, `putcolslice`, `putvarcol`), table/column creation, keyword writes |
| `daskms/descriptors/ms.py`, `ms_subtable.py`, `builder.py` | MS schema knowledge: `required_ms_desc`/`complete_ms_desc` equivalents, dminfo construction |
| `daskms/example_data.py` | Example MS creation via `default_ms` |
| `daskms/conftest.py` + `daskms/tests/` | Test fixtures: TaQL DDL table creation, casacore behaviour assertions |
| `daskms/apps/formats.py` | CASA-format introspection (subtable discovery via keywords) |

## Required casacore functionality, by area

### 1. CASA table on-disk format (the hard prerequisite)

The Rust backend must read and write the binary CASA table directory format:

- Directory table with `table.dat` (the on-disk identity check used by
  `fsspec_store.py` and `table.py:24`'s existence test).
- Storage managers actually exercised:
  - `StandardStMan` — default.
  - `IncrementalStMan` with `option |= 1` (Direct) for MS index columns
    (`descriptors/ms.py:106-109`).
  - `TiledColumnStMan` — one group per fixed column (`{column}_GROUP`,
    `option |= 4`), with `SPEC DEFAULTTILESHAPE` in **reversed (CASA) dim
    order** (`descriptors/ms.py:168-184, 225-264`).
- dminfo dict round-trip fidelity: `getdminfo()` must return
  `{"*N": {"NAME", "TYPE", "SPEC", "COLUMNS"}}` matching what was written
  (asserted in `tests/test_optional.py:271-275`), including the auto-suffixing
  behaviour (`_1` group) when `addcols` is called without dminfo
  (`tests/test_optional.py:362-374`).

Compatibility bar: a Rust-written MS must be readable by casacore/CASA and
vice versa, otherwise this is a new format, not a swap.

### 2. Table lifecycle and locking

| API | Used at |
|---|---|
| `table(path, ack=False, readonly=..., lockoptions=...)` (open *and* create) | `reads.py:328`, `writes.py:249,293,305`, `apps/formats.py:35` |
| `lock(write=)` / `unlock()` under `lockoptions="user"` | `table_proxy.py:98-239`, all runners in `reads.py`/`writes.py` |
| `flush()` after every write batch | `writes.py:46,69,88,110,127` |
| `close()` (from GC finalizer, any thread) | `table_proxy.py:248` |
| `iswritable()` reflecting open mode | `table_proxy.py:35,243,347-361` |
| `name()` | `table_proxy.py:360` |

The replacement must be safe to hold across dask's threaded executor and
survive pickling/multiprocess access (`table_proxy.py:370-375`,
`tests/test_ms_read_and_update.py:290-299`). Explicit user locking can be
treated as advisory internally, but the call surface must exist.

### 3. Column data access (hot path — performance-critical)

Reads:
- `getcolnp(column, buf, startrow=, nrow=)` — zero-copy-ish read into a
  preallocated numpy buffer (`reads.py:43,50`). This is the main read path.
- `getcolslicenp(column, buf, blc=, trc=, startrow=, nrow=)` (`reads.py:61,68`).
- `getcol` / `getcolslice` returning fresh arrays (`reads.py:81-124`).
- `getcell(column, row)` — exemplar-row shape/dtype inference for
  variable-shaped columns (`columns.py:185`).
- `getcellslice` / `getcell` for ordering group columns (`ordering.py:104,113`).
- `getvarcol(column)` → `{"rN": per-row-array}` (`table_proxy.py:44`).

Writes:
- `putcol(column, data, startrow=, nrow=)` (`writes.py:36-107`).
- `putcolslice(column, data, blc, trc, ...)` (`writes.py:78,85`).
- `putvarcol(column, {"rN": arr, ...}, ...)` for variable-shape data
  (`writes.py:122,126`).
- `addrows(n)` (`writes.py:358`, `table_proxy.py:37`).

Semantics that must match python-casacore exactly:
- **Inclusive slice ends** on `blc`/`trc` (`columns.py:280-281,297`).
- **Strings**: 1-D string columns as plain Python lists; multidimensional
  string columns as `{"shape": ..., "array": flat_list}` dicts on both read
  and write (`reads.py:90-93,120-124`, `writes.py:52-114`).
- **Object-dtype chunks must be converted to lists before `putcol`** — raw
  object arrays segfault python-casacore (`writes.py:158-163`,
  ratt-ru/dask-ms#42). A Rust binding should accept numpy object arrays
  directly and lift this wart out of dask-ms.
- `setmaxcachesize(column, 1)` — workaround for casacore's getcolslice
  caching bug (`reads.py:254`, casacore/casacore#1018). Ideally a no-op in
  the replacement.

### 4. Type system

The full `_TABLE_TO_PY` / `_PY_TO_TABLE` mapping in `daskms/columns.py:15-54`:

| CASA type | numpy |
|---|---|
| BOOL/BOOLEAN | bool |
| BYTE/UCHAR | uint8 |
| SHORT/SMALLINT | int16 |
| USHORT/USMALLINT | uint16 |
| INT/INTEGER | int32 |
| UINT/UINTEGER | uint32 |
| FLOAT | float32 |
| DOUBLE | float64 |
| FCOMPLEX/COMPLEX | complex64 |
| DCOMPLEX | complex128 |
| STRING | object |

Column descriptor semantics (`columns.py:103-262`): `ndim` = 0 scalar
(unsupported), `"row"`-only, positive fixed, `-1` unconstrained;
`option & 4` = FixedShape; shape from descriptor or inferred from an exemplar
`getcell`.

### 5. Table and column metadata

- `nrows()` (`table_proxy.py:31`, `ordering.py:82,196`).
- `colnames()` (`table_proxy.py:32`, `reads.py:294,344,379`).
- `getcoldesc(column)` — full column descriptor dict (`columns.py:142`,
  `reads.py:530`).
- `getdminfo()` (`writes.py:335`).
- Table keywords: `getkeywords()` / `putkeywords({...})` / `removekeyword()`
  (`reads.py:542`, `writes.py:298,727,729`); values may be nested records.
- Column keywords: `getcolkeywords(col)` / `putcolkeyword(col,k,v)` /
  `removecolkeyword(col,k)` (`reads.py:294`, `writes.py:735,737`); deletion
  sentinel `DELKW` in `writes.py:720`.
- Subtable linkage: main-table keyword string `"Table: <path>"`
  (`writes.py:298`), discovered by prefix + `table.dat` existence
  (`apps/formats.py:103-118`). `MS_VERSION` keyword distinguishes an MS from
  a generic CASA table (`apps/formats.py:40-46`).
- `table._getdesc(actual=True)` — a **private** python-casacore call used by
  `daskms/table.py:11` and `tests/test_dataset_keywords.py:29` (with
  empty `HCcoordnames`/`HCidnames` stripped from `_define_hypercolumn_`).
  The replacement needs a public table-description dict API so this private
  dependency can be dropped.

### 6. TaQL

dask-ms generates TaQL strings in `daskms/query.py` and executes them via
`ct.taql(query, style=..., tables=[...])` with `$N` table references and
per-table read/write locking (`table_proxy.py:181-199`).

Required dialect subset:

- `SELECT ... FROM $N [WHERE ...] [ORDERBY c1, c2] [GROUPBY ...]`
- Aggregate/grouping functions used for ordering (`ordering.py:64-190`):
  `ROWID()`, `GROWID()`, `GAGGR(c)`, `GCOUNT()`, `GROWID()[0]`.
- User `taql_where` passthrough into WHERE (`reads.py:317,489,494,520`) —
  arbitrary user expressions, so a real expression evaluator is needed.
- `SELECT UNIQUE col` and scalar subqueries
  (`SELECT [SELECT NAME FROM $2][ANTENNA1] AS NAME FROM $1`,
  `tests/test_table_proxy.py:28,61,173`).
- DDL: `CREATE TABLE path [FIELD_ID I4, ..., DATA C8 [NDIM=2, SHAPE=[16,4]],
  NAME S] LIMIT n` — used by all test fixtures (`conftest.py:45-56,95-107,
  163-171,219-227`).

Not used anywhere: `table.query()`, `table.rownumbers()`, `table.iter()`,
`table.sort()`, `makesequence`, `tableutil`. (Note: `tablefromascii` is used
in one test, `tests/test_table_proxy.py:222` — low priority.)

Note on ordering: dask-ms never assumes physical storage order — it always
materializes row order via `SELECT ROWID() ... ORDERBY` and converts to
(startrow, len) runs (`ordering.py:18-99`). The backend only needs correct
TaQL sort semantics, not a specific physical layout.

### 7. MS schema / descriptors

The Rust backend needs the canonical MS and subtable schema knowledge
currently supplied by casacore functions:

- `required_ms_desc()` / `required_ms_desc(subtable)` and
  `complete_ms_desc()` / `complete_ms_desc(subtable)` for the MS and all
  18 known subtables (`table_schemas.py:13-31`), asserted against in
  `test_ms_creation.py` and the descriptor builder tests. These return large
  but *static* descriptor dicts — they can be vendored as data in the Rust
  crate (generated once from casacore) rather than derived.
- Column descriptor keys written by builders: `valueType, ndim, shape,
  option, _c_order, dataManagerType, dataManagerGroup, keywords, maxlen,
  comment` (`descriptors/builder.py:167-176`).
- Standard column keywords, e.g. `QuantumUnits`/`UNIT: Jy` on DATA columns
  (`descriptors/ms.py:44-59`).
- `default_ms(path, tabdesc=, dminfo=)` — create a full MS **including the
  whole subtable tree with keyword linkage** (`writes.py:273`,
  `example_data.py:42`); `default_ms_subtable(name, path, ...)`
  (`writes.py:288`). The `"MS::SUBTABLE"` path syntax is used in
  `example_data.py:14-18`.
- `maketabdesc(...)` — tests only.

## Suggested sequencing

1. **Core table format crate**: `table.dat` header, StandardStMan column
   storage, scalar + fixed-shape array columns, full type mapping (§1, §4).
   Prove byte-level interop: read a casacore-written MS, write a table that
   casacore reads.
2. **Python bindings matching the python-casacore surface**: `table`,
   `getcolnp`/`putcol` (+ slice/varcol variants), `nrows`, `colnames`,
   `getcoldesc`, keywords APIs, `addrows`, `addcols`, lifecycle/lock calls as
   no-ops or advisory (§2, §3, §5). At this point, point a fork of
   `table_proxy.py` at the new module and run the read-only tests.
3. **Tiled + Incremental storage managers and dminfo round-trips** (§1):
   needed for MS creation and `test_optional.py`.
4. **TaQL subset** (§6): ordering queries first (`ROWID`/`ORDERBY`), then
   `WHERE` passthrough, then grouping aggregates (`GROWID`/`GAGGR`/`GCOUNT`),
   then DDL for test fixtures. Alternatively, port dask-ms's ordering to a
   native sort API and restrict TaQL to user `taql_where` strings.
5. **MS descriptors + `default_ms` subtable tree creation** (§7): vendor the
   canonical descriptors as data; implement subtable keyword linkage.
6. **Integration**: register the Rust backend as a store type alongside
   zarr/parquet in `fsspec_store.py`/`dask_ms.py` dispatch, or as a
   drop-in `casacore.tables` replacement selected by config
   (`daskms/config.py`). Migrate fixtures off TaQL DDL where convenient.
7. **Compatibility gate**: full dask-ms test suite passing against the Rust
   backend, plus cross-implementation round-trip tests (Rust-write →
   casacore-read and casacore-write → Rust-read).

## Out of scope

- `casacore.measures`, `casacore.quanta`, `casacore.images`, synthesis —
  unused by dask-ms.
- Automatic locking modes beyond accepting the kwargs.
- Full TaQL — only the subset in §6 is exercised.
- The experimental zarr/arrow/katdal stores — already casacore-free and
  unaffected by the swap.
