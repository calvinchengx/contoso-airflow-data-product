"""The gold numbers this runtime produced, for compare_products.py.

The family's claim is that every platform builds the SAME product. Two green
pipelines do not establish that; the same aggregates and the same contract
names do. This writes the shape `compare_products.py` reads, so this runtime
can be held to the others.

Runs against whatever the environment points at, so it works from a task, a
witness, or a shell -- it reads the Warehouse over TDS exactly as gold's own
dbt profile does.

THE CONTRACT NAMES COME FROM THE RUN, not from the directory. This script used
to build `contracts` by globbing `gold/tests/*.sql`, which names what the shared
project CONTAINS. Published into a snapshot, that reads as what this runtime
CHECKED -- and the two agree only when nothing went wrong, which is the one case
the field exists for. `compare_products` would then report agreement between a
runtime that evaluated a contract and one that never ran it.

So the names are read from `run_results.json`, and a name on disk that the run
did not evaluate is a hard failure rather than a line in the output.
"""
from __future__ import annotations

import json
import os
import pathlib
import struct
import sys

from contoso_airflow.target import Target

SQL_COPT_SS_ACCESS_TOKEN = 1256


def _attrs(token: str) -> dict:
    raw = token.encode("utf-16-le")
    return {SQL_COPT_SS_ACCESS_TOKEN: struct.pack("<i", len(raw)) + raw}


def verdicts(results: pathlib.Path, expected: list[str]) -> tuple[list[str], list[dict]]:
    """The contracts this `dbt test` evaluated, and the ones it failed.

    A separate function so it can be tested without a warehouse, a stack or a
    credential -- the logic that decides what this cell CLAIMS is worth holding
    to a test of its own.

    `expected` is the shared project's singular tests, and it is a CROSS-CHECK
    rather than the source: if the run evaluated everything on disk the two
    agree, and if it did not, this refuses instead of quietly publishing the
    shorter or the longer list.
    """
    if not results.is_file():
        raise SystemExit(
            f"no {results} -- gold's contracts leave a record of what they "
            f"evaluated, and without it this would have to guess. Did the gold "
            f"task group run with DBT_TARGET_PATH pointing here?")
    payload = json.loads(results.read_text(encoding="utf-8"))

    # ASSERT WHICH INVOCATION WROTE IT. `dbt run` shares this directory and
    # overwrites the file, and a `run` artefact reports models and zero
    # failures -- which, believed, publishes "no contract failures" for a run
    # whose contracts were never executed. The sibling platform found this the
    # expensive way; here the nine model tasks write to this same path, so it
    # is not a hypothetical ordering but the normal one.
    which = (payload.get("args") or {}).get("which")
    if which != "test":
        raise SystemExit(
            f"{results} was written by `dbt {which}`, not `dbt test` -- "
            f"refusing to report contract results from another command's "
            f"artefact.")

    evaluated, failures = set(), []
    for r in payload.get("results", []):
        uid = r.get("unique_id", "")
        # `test.contoso_gold.<name>` for a singular test, with a trailing hash
        # for a generic one. The third segment is the name in both.
        name = uid.split(".")[2] if uid.count(".") >= 2 else uid
        evaluated.add(name)
        if r.get("status") in ("pass", "success"):
            continue
        failures.append({"contract": name, "status": r.get("status"),
                         "failures": r.get("failures"),
                         "detail": (r.get("message") or "").strip()[:200]})

    unevaluated = [c for c in expected if c not in evaluated]
    if unevaluated:
        raise SystemExit(
            f"gold's tests/ names {', '.join(unevaluated)} but this `dbt test` "
            f"evaluated no such test -- the snapshot would claim a guarantee "
            f"that was never checked.")
    return sorted(c for c in expected if c in evaluated), failures


def main() -> int:
    import mssql_python

    if len(sys.argv) < 4:
        print("usage: snapshot.py <host> <port> <warehouse-id> [out.json]",
              file=sys.stderr)
        return 2
    host, port, warehouse = sys.argv[1], sys.argv[2], sys.argv[3]
    out = pathlib.Path(sys.argv[4] if len(sys.argv) > 4 else "product_snapshot.json")
    # The same default the DAG writes to, and the same env var overrides it, so
    # a witness run from a shell reads the artefacts the tasks just wrote.
    gold_target = pathlib.Path(
        os.environ.get("CONTOSO_GOLD_TARGET", "/tmp/contoso-gold-target"))

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
    # naming them from the installed package is what makes a missing test
    # visible instead of quietly absent. But the package says what SHOULD have
    # been checked, and only the run says what WAS.
    from contoso_product import gold_dir

    expected = sorted(p.stem for p in (gold_dir() / "tests").glob("*.sql"))
    contracts, failures = verdicts(gold_target / "run_results.json", expected)

    snapshot = {
        "revenue_usd": str(row[0]),
        "cancelled_revenue_usd": str(row[1]),
        "sale_lines": str(row[2]),
        "contracts": contracts,
        "runtime": "airflow-fabric",
        "catalog": warehouse,
    }
    # ABSENT WHEN CLEAN, never `[]`. compare_products reads the distinction:
    # absent means "this runtime checked and everything passed", where an empty
    # list is indistinguishable from a runtime that recorded the field without
    # ever running a contract.
    if failures:
        snapshot["contract_failures"] = failures
    out.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(snapshot, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
