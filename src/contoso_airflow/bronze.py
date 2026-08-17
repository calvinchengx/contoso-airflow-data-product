"""Landing → bronze. Parses, and nothing else.

The rule is the family's, and it is narrow on purpose:

    Bronze parses and nothing more. No dedupe, no conforming, no quarantine —
    those are silver's job, and doing them here would destroy the only copy of
    what the vendor actually sent.

WRITTEN BY DELTA-RS, not by dlt's destination and not by Spark. Phase 0 settled
that: dlt's Azure destinations cannot carry a bearer token and fall through to
`DefaultAzureCredential`; Spark is not available to an Airflow worker and the
Fabric notebook route needs an engine this platform does not have. delta-rs is
the writer this emulator is already witnessed against (`e2e/delta-rs`, 3-OS CI),
and it is the same library on both targets.

READS BACK FROM LANDING rather than from whatever the landing step held in
memory. That is not defensiveness: it makes the landing write load-bearing. A
landing step that silently wrote nothing would otherwise still produce a green
bronze, which is the exact shape of failure this product exists to refuse.
"""
from __future__ import annotations

import csv
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

from .io import onelake
from .target import Target


def _parse(blob: bytes, ext: str) -> list[dict]:
    """Bytes → rows, by the vendor's own dialect. Three vendors, three formats."""
    if ext == "jsonl":
        return [json.loads(line) for line in blob.splitlines() if line.strip()]
    if ext == "json":
        doc = json.loads(blob)
        # Web ships JSON arrays; a single object is still one row. Orders arrive
        # NESTED — the `lines` array rides along as data. Flattening is silver's
        # decision, and making it here would be reshaping.
        return doc if isinstance(doc, list) else [doc]
    if ext == "csv":
        return list(csv.DictReader(io.StringIO(blob.decode("utf-8-sig"))))
    if ext == "parquet":
        return pq.read_table(io.BytesIO(blob)).to_pylist()
    raise ValueError(f"no parser for .{ext} — bronze will not guess at a dialect")


def build_table(target: Target, workspace: str, lakehouse: str,
                manifest: list[dict], table: str) -> dict:
    """Read the landed parts back, parse them, write one bronze Delta table."""
    rows: list[dict] = []
    for part in manifest:
        rel = part["path"]
        ext = rel.rsplit(".", 1)[-1]
        rows += _parse(onelake.read(target, workspace, lakehouse, rel), ext)

    uri = f"az://{workspace}/{lakehouse}/Tables/{table}"
    opts = target.delta_storage_options()
    # `overwrite` because a bronze table is the landed day, not an accumulation:
    # re-running a day must not double it. Delta keeps the previous version, so
    # nothing is actually lost.
    write_deltalake(uri, pa.Table.from_pylist(rows), mode="overwrite",
                    storage_options=opts)

    # Confirm from a FRESH handle. The writer's own success is not evidence the
    # table is readable.
    dt = DeltaTable(uri, storage_options=opts)
    landed_rows = dt.to_pyarrow_table().num_rows
    if landed_rows != len(rows):
        raise ValueError(f"{table}: wrote {len(rows)} rows, delta-rs reads {landed_rows}")
    return {"table": table, "rows": landed_rows, "version": dt.version(),
            "parts": len(manifest)}
