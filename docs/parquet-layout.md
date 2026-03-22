# Parquet Layout Strategy for ZMP Archives

## Problem

A ZMP archive stores both small metadata (JSON text, ~100 bytes) and large
binary blobs (zarr chunks, 4KB–2MB+) in the same parquet file. The challenge
is optimizing for all access patterns simultaneously:

1. **Open time** — reading the footer to learn the file structure
2. **Single-row metadata lookup** — fetch a `text` column value by path, without reading blobs
3. **Single-row blob fetch** — fetch a `data` column value by path
4. **Path listing** — read all paths without reading blobs

The naive approach of one row group per row (to avoid decompressing
unrelated blobs) creates massive footers that dominate open time and
metadata lookups.

## Parquet Primitives

### Row Groups

The row group is the minimum I/O unit for most operations. Each row group
stores columns independently, so you can read just the `path` column
without touching the `data` column (column projection). However, reading
any column from a row group reads that column's entire chunk within the
row group.

**Footer overhead**: each row group adds ~100–200 bytes per column to the
footer. With 10 columns and 10,000 row groups, that's ~10–20 MB of footer,
which must be read on every file open.

### Column Statistics (min/max per row group)

Parquet stores min/max statistics for each column chunk in the footer.
If data is **sorted by path**, these statistics enable binary-search-like
row group elimination: for a lookup of path `P`, skip any row group where
`P < min` or `P > max`.

This is the primary mechanism for efficient single-row lookups. It works
with any parquet reader — no special features required.

### Page Indexes

Within a column chunk, data is divided into pages (~1MB default). Page
indexes store per-page min/max statistics and byte offsets, enabling
page-level skipping. PyArrow writes page indexes by default.

**Observed effect**: minimal for our workloads. Page-level skipping helps
within large row groups, but the dominant cost is already at the row group
level. Page size tuning (8KB, 64KB, 1MB) showed <10% difference.

### Bloom Filters

Parquet supports Split Block Bloom Filters per column chunk. These answer
"is value X definitely NOT in this column chunk?" in O(1) with no false
negatives.

**Current status**: PyArrow 23.x does not expose bloom filter writing via
`ParquetWriter`. When support arrives, bloom filters on the `path` column
would replace the manual statistics loop with a direct check, reducing
lookup from O(num_row_groups) to O(1) in the filter check phase.

**Expected impact**: at 500 row groups, the manual stats loop takes ~0.1ms.
Bloom filters would reduce this to ~0.01ms. The improvement is real but
not dramatic because the stats loop is already fast on sorted data (the
binary search exits early). Bloom filters will matter more for unsorted
data or very large files (10K+ row groups).

**Recommended**: enable bloom filters on the `path` column when PyArrow
adds support. The overhead is ~10 bits per path per row group (~1KB per
row group), negligible for any practical file.

### Column Projection

Reading `columns=["path"]` skips the `data` column entirely. This is
critical: a 5GB file with 200K blobs reads only ~5MB to list all paths.

### Per-Column Compression

Different columns can use different compression codecs:
- `data`: `NONE` (zarr chunks are pre-compressed; double-compression wastes CPU)
- `data_z`: `NONE` (same reason)
- All other columns: `ZSTD` (good ratio, fast decode for small metadata)

## Benchmarks

### Setup

- 200,000 entries with mixed blob sizes (95% × 4KB, 4% × 64KB, 1% × 2MB)
- Random (incompressible) blob data
- All paths sorted
- Manual stats-based lookup (iterates row groups, checks min/max, reads matching)
- MacOS ARM64, PyArrow 23.x, local SSD

### Results: Fixed Row Group Size

| RG size | RGs   | Open    | Meta    | Blob    | List     | File overhead |
|---------|-------|---------|---------|---------|----------|---------------|
| 50      | 4,001 | 19.4 ms | 65.9 ms | 0.1 ms  | 179.4 ms | 1.1%          |
| 100     | 2,001 | 9.4 ms  | 33.4 ms | 0.1 ms  | 124.4 ms | 0.5%          |
| 200     | 1,001 | 4.4 ms  | 15.7 ms | 0.4 ms  | 108.1 ms | 0.2%          |
| 500     | 401   | 1.2 ms  | 6.1 ms  | 1.6 ms  | 77.2 ms  | 0.1%          |
| 1,000   | 201   | 0.5 ms  | 2.9 ms  | 3.7 ms  | 72.3 ms  | <0.1%         |
| 2,000   | 101   | 0.3 ms  | 1.7 ms  | 16.9 ms | 69.9 ms  | <0.1%         |

**Manual stats-based blob lookup** ("Manual" column) is dramatically
faster than filter pushdown ("Meta" column) because it short-circuits
on the first match. Filter pushdown reads all row group metadata first.

**Observation**: blob fetch time is proportional to `RG_rows × avg_blob_size`
because reading any row from a row group reads the entire data column chunk
for that row group. At RG=2000 with 2MB blobs, a single row group can
contain ~20 × 2MB = 40MB of blob data.

### Results: Adaptive Row Group Sizing (10MB data budget)

| Metric       | Fixed RG=500 | Adaptive 10MB |
|-------------|-------------|---------------|
| Row groups  | 401         | 500           |
| Open        | 1.2 ms      | 1.7 ms        |
| Meta lookup | 6.1 ms      | 7.5 ms        |
| Small blob  | 1.6 ms      | 2.4 ms        |
| Big blob    | —           | 3.3 ms        |
| List paths  | 77.2 ms     | 82.0 ms       |

The adaptive strategy naturally handles mixed blob sizes: small chunks
pack into large row groups (~2000 rows of 4KB = 8MB), while areas with
large blobs get smaller row groups (~5 rows of 2MB = 10MB).

### Scaling

| Entries | Chunk size | RG=200 | Blob fetch |
|---------|-----------|--------|-----------|
| 1,000   | 4 KB      | 6 RGs  | 0.1 ms    |
| 10,000  | 4 KB      | 51 RGs | 0.1 ms    |
| 20,000  | 4 KB      | 101 RGs| 0.1 ms    |
| 20,000  | 64 KB     | 101 RGs| 0.8 ms    |
| 200,000 | mixed     | 500 RGs| 2.4 ms    |

## Recommended Layout

### Algorithm

When writing a ZMP file:

1. **Sort all rows by path** for optimal column statistics.

2. **Separate rows into three categories:**
   - Data rows (have `data` or `data_z` column populated)
   - Non-data rows (text-only: metadata, resolve references)
   - Archive row (path `""`: index + archive metadata)

3. **Write the archive row as the first row group.** This is always 1 row
   (path `""`), contains archive-level metadata, and sits at the start
   of the file for fast access.

4. **Write all non-data rows as the second row group.** These are small
   (metadata JSON, resolve dicts) and benefit from being colocated near
   the start of the file for fast scanning.

5. **Write data rows with adaptive row group sizing:**
   - Accumulate rows until the data column would exceed `TARGET_RG_DATA_BYTES`
     (default 10MB) or the row count exceeds `MAX_RG_ROWS` (default 2000).
   - Flush at that boundary.
   - This naturally adapts: many small blobs → large row groups; few large
     blobs → small row groups.

This metadata-first layout is optimal for HTTP range-request access:
the parquet footer is at the end (standard), metadata is at the start,
and blobs are only read on demand from the middle of the file.

### Parameters

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| `TARGET_RG_DATA_BYTES` | 10 MB | Keeps blob fetch under ~5ms even for large blobs |
| `MAX_RG_ROWS` | 2,000 | Caps row group scan time for small blobs |
| `data_page_size` | 1 MB | Default; page indexes provide within-RG skipping |
| `data` compression | NONE | Zarr chunks are pre-compressed |
| Other columns | ZSTD | Good ratio for small text/JSON data |
| `use_dictionary` | False for `path` | Paths are unique; dictionary wastes space |
| `write_page_index` | True | Enables page-level skipping |
| Bloom filters | When available | Enable on `path` column |

### Lookup Algorithm (reader side)

```python
# O(num_row_groups) but exits early on sorted data
pf = ParquetFile(path)
path_col_idx = schema.get_field_index("path")

for rg_idx in range(pf.metadata.num_row_groups):
    stats = pf.metadata.row_group(rg_idx).column(path_col_idx).statistics
    if stats and stats.has_min_max:
        if target < stats.min or target > stats.max:
            continue
    # Read only needed columns from matching row group
    rg = pf.read_row_group(rg_idx, columns=["path", "data"])
    result = rg.filter(pc.equal(rg.column("path"), target))
    if len(result) > 0:
        return result.column("data")[0].as_py()
```

With bloom filters (future):
```python
# O(1) check per row group — skip without reading stats
if not rg_has_value_in_bloom_filter(rg_idx, "path", target):
    continue
```

### What This Replaces

The current approach uses:
- 1 row group per data row (causes footer bloat)
- An index row with a JSON blob mapping paths to row numbers (a hack)
- Manual index parsing on file open

The new approach needs no index row for data access. The archive row
(`path == ""`) still exists for archive-level metadata, but it no longer
carries an index — sorted paths + column statistics replace it.
