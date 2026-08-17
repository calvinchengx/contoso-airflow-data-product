"""Make a bronze Delta path visible to the engine BY NAME.

WHY THIS EXISTS, measured rather than assumed. delta-rs writes bronze straight
to `Tables/<name>`, and the bytes are real -- Sail reads them by path and
returns the right count. But the engine's catalog does not know the NAME:

    SHOW TABLES                              -> []
    SELECT count(*) FROM bronze_pos_customers -> TABLE_OR_VIEW_NOT_FOUND
    SELECT count(*) FROM delta.`abfss://...`  -> 102000

In real Fabric, Delta directories under `Tables/` are discovered by the
Lakehouse and become queryable. This emulator does not perform that discovery,
so the pipeline performs it: one `CREATE TABLE ... USING delta LOCATION` per
bronze table, which is the same statement Fabric's own registration amounts to.

Without it every silver model fails on an unresolved relation, because dbt
resolves `{{ source('bronze', ...) }}` by name.

DURABILITY, and its limit. A registration made in one Livy session IS visible
from another -- measured, both sessions returning 102,000 -- because the
statement agent holds a single long-lived SparkSession and Livy sessions are
separate namespaces over the same catalog. That is exactly the boundary dbt
crosses, so `source()` resolves.

What it does NOT survive is an agent restart: the catalog lives in that
session's memory. So this runs on EVERY landing, with IF NOT EXISTS, rather
than once at setup. One statement per table is a cheap price for not depending
on a process having stayed up.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request


from . import tls
from .target import Target


class RegistrationError(RuntimeError):
    """A bronze table the engine could not be told about."""


def tables_root(workspace: str, lakehouse: str) -> str:
    """`Tables/` in OneLake -- where a Lakehouse's tables ARE.

    Not a convention this product invented. A Delta directory under `Tables/`
    is what the Lakehouse discovers and what its SQL analytics endpoint
    exposes as T-SQL, which is the only way gold can read silver by three-part
    name. Silver's dbt models are given this as `location_root` for exactly
    that reason -- see the note in dbt/silver/dbt_project.yml.
    """
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables"


def table_uri(workspace: str, lakehouse: str, table: str) -> str:
    """The production-shaped OneLake URI for a bronze table.

    `abfss://…@onelake.dfs.fabric.microsoft.com/…`, which is what the engine
    reads on both targets -- the emulator resolves the host itself. Building it
    here rather than in each caller keeps one spelling.
    """
    return f"{tables_root(workspace, lakehouse)}/{table}"


def register(target: Target, agent_url: str, workspace: str, lakehouse: str,
             table: str) -> str:
    """Tell the engine that `table` is the Delta at its OneLake path.

    Idempotent by IF NOT EXISTS: a re-run is the normal case, and a
    registration that failed the second time would make every run after the
    first a failure for no reason.
    """
    uri = table_uri(workspace, lakehouse, table)
    sql = f"CREATE TABLE IF NOT EXISTS {table} USING delta LOCATION '{uri}'"
    body = json.dumps({"session": "bronze-register", "kind": "sql", "code": sql}).encode()
    req = urllib.request.Request(f"{agent_url}/statements", data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=300, context=tls.CONTEXT) as r:
            res = json.loads(r.read())
    except urllib.error.URLError as e:
        raise RegistrationError(
            f"{table}: could not reach the statement agent at {agent_url}: {e}") from e
    # The agent answers 200 with an error PAYLOAD rather than an HTTP error, so
    # a caller checking only the status code reports every failure as a pass.
    if res.get("status") != "ok":
        raise RegistrationError(
            f"{table}: registration refused -- silver's source() will not "
            f"resolve it. {json.dumps(res)[:300]}")
    return uri
