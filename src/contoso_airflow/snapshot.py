"""The gold numbers this runtime produced, for the family to hold it to.

The family's claim is that every platform builds the SAME product. Two green
pipelines do not establish that; the same aggregates and the same contract
names do.

WHY THIS IS A MODULE AND NOT A SCRIPT ANY MORE. It was `scripts/snapshot.py`,
runnable only by hand -- which is exactly why this cell had no snapshot in any
unattended run and could not be held to the family's figures at all (G50). A
DAG task cannot import from `scripts/`, and copying the logic into the DAG would
make two definitions of what this cell publishes. So the logic lives here, the
`publish` task calls it, and the script keeps its command line for a witness run
from a shell.

THE CONTRACT NAMES COME FROM THE RUN, not from the directory. `contracts` used
to be built by globbing `gold/tests/*.sql`, which names what the shared project
CONTAINS. Published into a snapshot, that reads as what this runtime CHECKED --
and the two agree only when nothing went wrong, which is the one case the field
exists for.
"""

from __future__ import annotations

import json
import os
import pathlib

# WHERE THE PLATFORM WANTS IT. The platform owns deployment facts, so it names
# the path and this reads it; the default is for a witness run from a shell,
# where the cwd is the operator's.
SNAPSHOT_ENV = "PRODUCT_SNAPSHOT"
GOLD_TARGET_ENV = "CONTOSO_GOLD_TARGET"
DEFAULT_GOLD_TARGET = "/tmp/contoso-gold-target"

# The three aggregates every cell in the family reports. coalesce, so an empty
# gold reports 0 rather than NULL -- and `compare_products.empty()` is what
# stops three zeros being mistaken for agreement.
AGGREGATES = (
    "SELECT coalesce(sum(revenue_usd),0), coalesce(sum(cancelled_revenue_usd),0), "
    "coalesce(sum(sale_lines),0) FROM fct_revenue_summary"
)


def out_path() -> pathlib.Path:
    return pathlib.Path(os.environ.get(SNAPSHOT_ENV) or "product_snapshot.json")


def gold_target() -> pathlib.Path:
    return pathlib.Path(os.environ.get(GOLD_TARGET_ENV, DEFAULT_GOLD_TARGET))


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


def build(conn, warehouse: str, results: pathlib.Path | None = None) -> dict:
    """This run's snapshot, read from the warehouse it just wrote.

    `conn` is supplied rather than opened here: the task already holds one, and
    a second connection would be a second chance to point at the wrong
    warehouse -- the defect the semantic contract exists to catch.
    """
    row = conn.cursor().execute(AGGREGATES).fetchone()

    # CONTRACTS ARE THE SHARED PROJECT'S, not this repo's. gold lives in
    # contoso-data-product precisely so every runtime asserts the same things;
    # naming them from the installed package is what makes a missing test
    # visible instead of quietly absent. But the package says what SHOULD have
    # been checked, and only the run says what WAS.
    from contoso_product import gold_dir

    expected = sorted(p.stem for p in (gold_dir() / "tests").glob("*.sql"))
    contracts, failures = verdicts(
        (results or gold_target()) / "run_results.json", expected
    )

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
    return snapshot


def write(snapshot: dict, out: pathlib.Path | None = None) -> pathlib.Path:
    out = out or out_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    return out
