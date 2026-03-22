"""Benchmark parquet configurations for zmanifest-style blob+metadata storage.

Tests various row group sizes, page sizes, bloom filters, and compression
settings across different access patterns:
  1. Open time (read footer + metadata)
  2. Single-row metadata lookup (path → text column, no data column)
  3. Single-row blob fetch (path → data column)
  4. Full scan of all paths
  5. File size

Generates compressible (zeros) and incompressible (random) blob data.
"""

import json
import os
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

WORKDIR = Path("/tmp/parquet_bench")
WORKDIR.mkdir(exist_ok=True)


def make_test_data(
    num_entries: int,
    chunk_size: int,
    compressible: bool = True,
) -> pa.Table:
    """Create a table mimicking a zmanifest with inline data."""
    paths = [f"group/array/c/{i}" for i in range(num_entries)]
    # Add some metadata entries
    meta_paths = ["zarr.json", "group/zarr.json", "group/array/zarr.json"]
    meta_texts = [
        json.dumps({"zarr_format": 3, "node_type": "group"}),
        json.dumps({"zarr_format": 3, "node_type": "group"}),
        json.dumps({
            "zarr_format": 3, "node_type": "array",
            "shape": [num_entries * 64], "data_type": "float64",
            "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [64]}},
            "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
            "fill_value": 0,
            "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
        }),
    ]

    all_paths = sorted(paths + meta_paths)

    texts = []
    datas = []
    sizes = []

    for p in all_paths:
        if p.endswith("zarr.json"):
            idx = meta_paths.index(p) if p in meta_paths else 0
            t = meta_texts[idx] if p in meta_paths else None
            texts.append(t)
            datas.append(None)
            sizes.append(len(t) if t else 0)
        else:
            texts.append(None)
            if compressible:
                blob = b"\x00" * chunk_size
            else:
                blob = np.random.bytes(chunk_size)
            datas.append(blob)
            sizes.append(chunk_size)

    return pa.table({
        "path": pa.array(all_paths, type=pa.string()),
        "size": pa.array(sizes, type=pa.int64()),
        "text": pa.array(texts, type=pa.string()),
        "data": pa.array(datas, type=pa.binary()),
        "addressing": pa.array(
            [["T"] if t else ["D"] for t in texts],
            type=pa.list_(pa.string()),
        ),
    })


def write_parquet(
    table: pa.Table,
    path: str,
    *,
    row_group_size: int,
    data_page_size: int = 1024 * 1024,
    use_dictionary: bool = False,
    data_compression: str = "NONE",
    meta_compression: str = "ZSTD",
) -> Path:
    """Write parquet with specific settings."""
    out = WORKDIR / path
    writer = pq.ParquetWriter(
        str(out),
        table.schema,
        compression={
            "path": meta_compression,
            "size": meta_compression,
            "text": meta_compression,
            "data": data_compression,
            "addressing": meta_compression,
        },
        use_dictionary=["addressing"] if not use_dictionary else True,
        data_page_size=data_page_size,
        write_page_index=True,
        write_batch_size=row_group_size,
    )

    # Write in row groups
    for start in range(0, len(table), row_group_size):
        end = min(start + row_group_size, len(table))
        writer.write_table(table.slice(start, end - start))
    writer.close()
    return out


def bench_open(path: Path, iterations: int = 50) -> float:
    """Benchmark file open time (footer read)."""
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        pf = pq.ParquetFile(str(path))
        _ = pf.metadata  # force footer read
        times.append(time.perf_counter() - t0)
    return np.median(times) * 1000  # ms


def bench_metadata_lookup(path: Path, target_path: str, iterations: int = 50) -> float:
    """Benchmark reading a metadata row (text column, no data column)."""
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        table = pq.read_table(
            str(path),
            columns=["path", "text"],
            filters=[("path", "=", target_path)],
        )
        _ = table.column("text")[0].as_py()
        times.append(time.perf_counter() - t0)
    return np.median(times) * 1000  # ms


def bench_blob_fetch(path: Path, target_path: str, iterations: int = 50) -> float:
    """Benchmark reading a data blob for a specific path."""
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        table = pq.read_table(
            str(path),
            columns=["path", "data"],
            filters=[("path", "=", target_path)],
        )
        _ = table.column("data")[0].as_py()
        times.append(time.perf_counter() - t0)
    return np.median(times) * 1000  # ms


def bench_manual_blob_fetch(path: Path, target_path: str, iterations: int = 50) -> float:
    """Benchmark blob fetch using manual row group iteration with statistics."""
    times = []
    pf = pq.ParquetFile(str(path))
    path_col_idx = pf.schema_arrow.get_field_index("path")

    for _ in range(iterations):
        t0 = time.perf_counter()
        found = False
        for rg_idx in range(pf.metadata.num_row_groups):
            col_meta = pf.metadata.row_group(rg_idx).column(path_col_idx)
            stats = col_meta.statistics
            if stats and stats.has_min_max:
                if not (stats.min <= target_path <= stats.max):
                    continue
            rg = pf.read_row_group(rg_idx, columns=["path", "data"])
            mask = pc.equal(rg.column("path"), target_path)
            filtered = rg.filter(mask)
            if len(filtered) > 0:
                _ = filtered.column("data")[0].as_py()
                found = True
                break
        assert found
        times.append(time.perf_counter() - t0)
    return np.median(times) * 1000  # ms


def bench_list_all_paths(path: Path, iterations: int = 20) -> float:
    """Benchmark reading all paths (no data column)."""
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        table = pq.read_table(str(path), columns=["path"])
        _ = table.column("path").to_pylist()
        times.append(time.perf_counter() - t0)
    return np.median(times) * 1000  # ms


def run_config(
    name: str,
    table: pa.Table,
    *,
    row_group_size: int,
    data_page_size: int = 1024 * 1024,
    data_compression: str = "NONE",
) -> dict:
    """Write and benchmark a single configuration."""
    fname = f"{name}.parquet"
    path = write_parquet(
        table, fname,
        row_group_size=row_group_size,
        data_page_size=data_page_size,
        data_compression=data_compression,
    )

    file_size = os.path.getsize(path)
    pf = pq.ParquetFile(str(path))

    # Pick targets: a metadata path and a chunk path in the middle
    all_paths = sorted(table.column("path").to_pylist())
    chunk_paths = [p for p in all_paths if not p.endswith("zarr.json")]
    mid_chunk = chunk_paths[len(chunk_paths) // 2]
    meta_target = "group/array/zarr.json"

    results = {
        "name": name,
        "file_size_mb": file_size / (1024 * 1024),
        "num_row_groups": pf.metadata.num_row_groups,
        "footer_size_kb": pf.metadata.serialized_size / 1024 if hasattr(pf.metadata, 'serialized_size') else -1,
        "open_ms": bench_open(path),
        "meta_lookup_ms": bench_metadata_lookup(path, meta_target),
        "blob_fetch_ms": bench_blob_fetch(path, mid_chunk),
        "blob_manual_ms": bench_manual_blob_fetch(path, mid_chunk),
        "list_paths_ms": bench_list_all_paths(path),
    }
    return results


def print_results(results: list[dict]) -> None:
    header = f"{'Config':<40} {'Size MB':>8} {'RGs':>5} {'Open':>7} {'Meta':>7} {'Blob':>7} {'Manual':>7} {'List':>7}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['name']:<40} {r['file_size_mb']:>8.2f} {r['num_row_groups']:>5} "
            f"{r['open_ms']:>7.2f} {r['meta_lookup_ms']:>7.2f} {r['blob_fetch_ms']:>7.2f} "
            f"{r['blob_manual_ms']:>7.2f} {r['list_paths_ms']:>7.2f}"
        )


def main():
    for data_label, compressible in [("compressible", True), ("random", False)]:
        for num_chunks, chunk_size in [(1000, 4096), (10000, 4096), (1000, 65536)]:
            print(f"\n{'='*80}")
            print(f"  {data_label} data | {num_chunks} chunks x {chunk_size} bytes")
            print(f"{'='*80}")

            table = make_test_data(num_chunks, chunk_size, compressible=compressible)
            tag = f"{data_label}_{num_chunks}x{chunk_size}"

            configs = [
                # Row group size sweep
                (f"{tag}_rg1", {"row_group_size": 1}),
                (f"{tag}_rg10", {"row_group_size": 10}),
                (f"{tag}_rg100", {"row_group_size": 100}),
                (f"{tag}_rg1000", {"row_group_size": 1000}),
                (f"{tag}_rg5000", {"row_group_size": 5000}),
                # Smaller page size
                (f"{tag}_rg1000_pg64k", {"row_group_size": 1000, "data_page_size": 64 * 1024}),
                (f"{tag}_rg1000_pg8k", {"row_group_size": 1000, "data_page_size": 8 * 1024}),
                # Data compression (for compressible data)
                (f"{tag}_rg1000_lz4", {"row_group_size": 1000, "data_compression": "LZ4"}),
            ]

            results = []
            for name, kwargs in configs:
                r = run_config(name, table, **kwargs)
                results.append(r)

            print()
            print_results(results)


if __name__ == "__main__":
    main()
