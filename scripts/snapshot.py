"""The gold numbers this runtime produced, for compare_products.py.

The family's claim is that every platform builds the SAME product. Two green
pipelines do not establish that; the same aggregates and the same contract
names do. This writes the shape `compare_products.py` reads, so this runtime
can be held to the others.

Runs against whatever the environment points at, so it works from a task, a
witness, or a shell -- it reads the Warehouse over TDS exactly as gold's own
dbt profile does.
"""
from __future__ import annotations

import json
import pathlib
import struct
import sys

from contoso_airflow.target import Target

SQL_COPT_SS_ACCESS_TOKEN = 1256


def _attrs(token: str) -> dict:
    raw = token.encode("utf-16-le")
    return {SQL_COPT_SS_ACCESS_TOKEN: struct.pack("<i", len(raw)) + raw}


def main() -> int:
    import mssql_python

    if len(sys.argv) < 4:
        print("usage: snapshot.py <host> <port> <warehouse-id> [out.json]",
              file=sys.stderr)
        return 2
    host, port, warehouse = sys.argv[1], sys.argv[2], sys.argv[3]
    out = pathlib.Path(sys.argv[4] if len(sys.argv) > 4 else "product_snapshot.json")

    target = Target.from_env()
    conn = mssql_python.connect(
        f"Server={host},{port};Database={warehouse};"
        f"Encrypt=no;TrustServerCertificate=yes",
        attrs_before=_attrs(target.sql_token()), timeout=60)

    # The same three aggregates the databricks and snowflake runtimes report.
    # coalesce, so an empty gold reports 0 rather than NULL -- and the guard in
    # compare_products is what stops a 0 being mistaken for agreement.
    row = conn.cursor().execute(
        "SELECT coalesce(sum(revenue_usd),0), coalesce(sum(cancelled_revenue_usd),0), "
        "coalesce(sum(sale_lines),0) FROM fct_revenue_summary").fetchone()

    # CONTRACTS ARE THE SHARED PROJECT'S, not this repo's. gold lives in
    # contoso-data-product precisely so every runtime asserts the same things;
    # listing them from the installed package is what makes a missing test
    # visible instead of quietly absent.
    from contoso_product import gold_dir

    contracts = sorted(p.stem for p in (gold_dir() / "tests").glob("*.sql"))

    snapshot = {
        "revenue_usd": str(row[0]),
        "cancelled_revenue_usd": str(row[1]),
        "sale_lines": str(row[2]),
        "contracts": contracts,
        "runtime": "airflow-fabric",
        "catalog": warehouse,
    }
    out.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(snapshot, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
