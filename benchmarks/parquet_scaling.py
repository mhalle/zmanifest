"""Scaling benchmark: 200K entries with mixed blob sizes.

Tests adaptive row group sizing (10MB data budget) against fixed sizes.
Measures open, metadata lookup, blob fetch (small + large), and path listing.
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


def make_mixed_data(num_chunks: int) -> pa.Table:
    """Create 200K-entry table: 95% x 4KB, 4% x 64KB, 1% x 2MB."""
    paths = sorted(
        [f"vol/c/{i}" for i in range(num_chunks)]
        + ["zarr.json", "vol/zarr.json"]
    )
    texts, datas, sizes = [], [], []
    for p in paths:
        if p.endswith("zarr.json"):
            texts.append('{"zarr_format":3}')
            datas.append(None)
            sizes.append(16)
        else:
            i = int(p.split("/")[-1])
            if i % 100 < 95:
                sz = 4096
            elif i % 100 < 99:
                sz = 65536
            else:
                sz = 2 * 1024 * 1024
            texts.append(None)
            datas.append(np.random.bytes(sz))
            sizes.append(sz)
    return pa.table({
        "path": pa.array(paths, type=pa.string()),
        "size": pa.array(sizes, type=pa.int64()),
        "text": pa.array(texts, type=pa.string()),
        "data": pa.array(datas, type=pa.binary()),
    })


def write_adaptive(table, path, target_bytes=10 * 1024 * 1024, max_rows=2000):
    """Write with adaptive row group sizing."""
    writer = pq.ParquetWriter(
        str(path), table.schema,
        compression={"path": "zstd", "size": "zstd", "text": "zstd", "data": "none"},
        use_dictionary=False,
        write_page_index=True,
        data_page_size=1024 * 1024,
    )

    sizes_col = table.column("size")
    rg_start = 0
    rg_data = 0
    rg_rows = 0

    for i in range(len(table)):
        sz = sizes_col[i].as_py()
        if table.column("text")[i].as_py() is not None:
            sz = 0  # text rows have no blob data
        rg_data += sz
        rg_rows += 1

        if rg_data >= target_bytes or rg_rows >= max_rows:
            writer.write_table(table.slice(rg_start, i - rg_start + 1))
            rg_start = i + 1
            rg_data = 0
            rg_rows = 0

    if rg_start < len(table):
        writer.write_table(table.slice(rg_start, len(table) - rg_start))
    writer.close()


def write_fixed(table, path, rg_size):
    """Write with fixed row group size."""
    writer = pq.ParquetWriter(
        str(path), table.schema,
        compression={"path": "zstd", "size": "zstd", "text": "zstd", "data": "none"},
        use_dictionary=False,
        write_page_index=True,
        data_page_size=1024 * 1024,
    )
    for s in range(0, len(table), rg_size):
        writer.write_table(table.slice(s, min(rg_size, len(table) - s)))
    writer.close()


def benchmark(fpath, mid_small, mid_big, iters=20):
    pf = pq.ParquetFile(str(fpath))
    nrg = pf.metadata.num_row_groups
    path_ci = pf.schema_arrow.get_field_index("path")

    def median_time(fn, n=iters):
        ts = [0.0] * n
        for j in range(n):
            t0 = time.perf_counter()
            fn()
            ts[j] = time.perf_counter() - t0
        return np.median(ts) * 1000

    def manual_lookup(target):
        for ri in range(nrg):
            st = pf.metadata.row_group(ri).column(path_ci).statistics
            if st and st.has_min_max and not (st.min <= target <= st.max):
                continue
            rgt = pf.read_row_group(ri, columns=["path", "data"])
            filt = rgt.filter(pc.equal(rgt.column("path"), target))
            if len(filt) > 0:
                _ = filt.column("data")[0].as_py()
                return

    t_open = median_time(lambda: pq.ParquetFile(str(fpath)))
    t_meta = median_time(lambda: pq.read_table(
        str(fpath), columns=["path", "text"],
        filters=[("path", "=", "vol/zarr.json")],
    ))
    t_small = median_time(lambda: manual_lookup(mid_small))
    t_big = median_time(lambda: manual_lookup(mid_big))
    t_list = median_time(
        lambda: pq.read_table(str(fpath), columns=["path"]).column("path").to_pylist(),
        n=5,
    )

    rg_sizes = [pf.metadata.row_group(i).num_rows for i in range(nrg)]
    return {
        "file_mb": os.path.getsize(fpath) / (1024 * 1024),
        "rgs": nrg,
        "rg_min": min(rg_sizes),
        "rg_max": max(rg_sizes),
        "rg_med": int(np.median(rg_sizes)),
        "open": t_open,
        "meta": t_meta,
        "small": t_small,
        "big": t_big,
        "list": t_list,
    }


def main():
    num = 200_000
    print(f"Generating {num} entries with mixed blob sizes...")
    table = make_mixed_data(num)

    total_data = sum(
        table.column("size")[i].as_py()
        for i in range(len(table))
        if table.column("text")[i].as_py() is None
    )
    print(f"Total data: {total_data / 1024 / 1024:.0f} MB")
    big_count = sum(
        1 for i in range(len(table))
        if table.column("size")[i].as_py() >= 2 * 1024 * 1024
    )
    print(f"2MB blobs: {big_count}")
    print()

    mid_small = f"vol/c/{num // 2}"
    mid_big = f"vol/c/{99}"

    configs = [
        ("fixed_rg100", lambda t, p: write_fixed(t, p, 100)),
        ("fixed_rg500", lambda t, p: write_fixed(t, p, 500)),
        ("fixed_rg1000", lambda t, p: write_fixed(t, p, 1000)),
        ("adaptive_10mb", lambda t, p: write_adaptive(t, p, 10 * 1024 * 1024)),
        ("adaptive_5mb", lambda t, p: write_adaptive(t, p, 5 * 1024 * 1024)),
        ("adaptive_20mb", lambda t, p: write_adaptive(t, p, 20 * 1024 * 1024)),
    ]

    header = (
        f"{'Config':<20} {'MB':>7} {'RGs':>5} {'RG rows':>12} "
        f"{'Open':>7} {'Meta':>7} {'Small':>7} {'Big':>7} {'List':>7}"
    )
    print(header)
    print("-" * len(header))

    for name, write_fn in configs:
        fpath = WORKDIR / f"scaling_{name}.parquet"
        write_fn(table, fpath)
        r = benchmark(fpath, mid_small, mid_big)
        rg_range = f"{r['rg_min']}-{r['rg_max']} (m{r['rg_med']})"
        print(
            f"{name:<20} {r['file_mb']:>7.0f} {r['rgs']:>5} {rg_range:>12} "
            f"{r['open']:>7.1f} {r['meta']:>7.1f} {r['small']:>7.1f} "
            f"{r['big']:>7.1f} {r['list']:>7.1f}"
        )


if __name__ == "__main__":
    main()
