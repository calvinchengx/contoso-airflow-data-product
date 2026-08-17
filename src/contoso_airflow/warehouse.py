"""The TDS side of the target: connect, and prove silver is visible.

WHY THIS STEP EXISTS. Gold reads silver by three-part name across two databases
on the same TDS endpoint -- the Warehouse it builds in, and the Lakehouse's SQL
analytics endpoint it reads from. On real Fabric that endpoint exposes the
Lakehouse's Delta tables automatically and there is nothing to trigger. Here the
first CONNECT is what brings the per-item database up, and it can take a while,
so a gold build that starts cold fails on a connection timeout that says nothing
about what was actually not ready.

It is also the only honest place to answer "is silver there?". dbt's own
`source()` resolution failing tells you a name did not resolve; it does not tell
you whether silver was never written, written somewhere the Lakehouse cannot
see, or simply not reflected yet. This connects, lists, and NAMES what is
missing.

Nothing here is emulator-specific: the same connect against real Fabric is a
no-op that returns the same list.
"""
from __future__ import annotations

import json
import struct
import time
import urllib.error
import urllib.request

from . import tls
from .target import Target

# SQL_COPT_SS_ACCESS_TOKEN -- the ODBC attribute Fabric's Entra auth goes
# through. 4-byte little-endian length, then the token as UTF-16-LE. This is
# the same injection dbt-fabric performs internally; doing it here means the
# witness and the adapter authenticate identically.
SQL_COPT_SS_ACCESS_TOKEN = 1256


class ReflectionError(RuntimeError):
    """Silver is not visible through the SQL analytics endpoint."""


def endpoint(conn_id: str = "fabric_warehouse") -> tuple[str, str]:
    """Where TDS is, from the connection the deployment provisions.

    NO DEFAULT, deliberately. This used to be
    `os.environ.get("FABRIC_TDS_HOST", "fabric-emulator")`, which names a
    local container inside product code -- so a deployment that forgot to set
    the variable would not fail, it would quietly aim at a hostname that
    exists on one machine in the world. An unprovisioned connection raises
    here instead, before a token is minted or a model is built.

    The platform already provisions this connection with exactly these two
    fields; this reads them rather than re-deriving them.
    """
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection(conn_id)
    if not conn.host:
        raise ReflectionError(
            f"connection {conn_id!r} names no host; the deployment must "
            f"provision it with the Warehouse's TDS endpoint.")
    return conn.host, str(conn.port or 1433)


def _attrs(token: str) -> dict:
    raw = token.encode("utf-16-le")
    return {SQL_COPT_SS_ACCESS_TOKEN: struct.pack("<i", len(raw)) + raw}


def connect(target: Target, database: str, host: str, port: str,
            attempts: int = 40, delay: float = 3.0):
    """A TDS connection to one database, retried while it comes up.

    Retry rather than sleep: a fixed wait either flakes on a loaded machine or
    wastes time on a fast one. The token is minted once -- a fresh one per
    attempt would hide an audience rejection behind forty identical failures.
    """
    import mssql_python

    dsn = (f"Server={host},{port};Database={database};"
           f"Encrypt=no;TrustServerCertificate=yes")
    attrs = _attrs(target.sql_token())
    last: Exception | None = None
    for _ in range(attempts):
        try:
            return mssql_python.connect(dsn, attrs_before=attrs, timeout=30)
        except Exception as exc:  # noqa: BLE001 -- driver errors are not a type
            last = exc
            time.sleep(delay)
    raise ReflectionError(f"{database} never accepted a connection: {last}")


def tables(conn) -> set[str]:
    rows = conn.cursor().execute(
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES").fetchall()
    return {f"{r[0]}.{r[1]}" for r in rows}


def refresh_metadata(target: Target, workspace_id: str, lakehouse_id: str) -> list:
    """Tell the SQL analytics endpoint to catch up with Delta.

    ASKING IS NOT THE SAME AS CONNECTING, and the difference is a silent
    no-op. The endpoint's view of the Lakehouse is a snapshot; tables written
    after it was taken are invisible until it is refreshed. Opening a
    connection happens to trigger that refresh on some implementations, which
    makes "just connect again" look like it works -- and does nothing at all
    against a real tenant, where the only lever is this call.

    Measured, before this existed: eight silver models built and landed in
    OneLake, and the endpoint exposed three of them. Not a write problem --
    a metadata problem that a connection-based re-sync could not fix.

    The endpoint's id comes from the lakehouse itself
    (`properties.sqlEndpointProperties.id`), which is where real Fabric puts
    it, so nothing here is target-specific.
    """
    lake = _api(target, "GET", f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
    endpoint = (lake.get("properties", {})
                    .get("sqlEndpointProperties", {})
                    .get("id"))
    if not endpoint:
        raise ReflectionError(
            f"lakehouse {lakehouse_id} reports no SQL analytics endpoint; "
            f"gold has nothing to read silver through.")
    report = _api(target, "POST",
                  f"/v1/workspaces/{workspace_id}/sqlEndpoints/{endpoint}"
                  f"/refreshMetadata")
    return (report or {}).get("value", [])


def _api(target: Target, method: str, path: str) -> dict:
    req = urllib.request.Request(f"{target.api_root}{path}", method=method)
    req.add_header("Authorization", f"Bearer {target.fabric_token()}")
    try:
        with urllib.request.urlopen(req, timeout=300, context=tls.CONTEXT) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise ReflectionError(
            f"{method} {path} -> {e.code}: {e.read()[:200]!r}") from e


def reflect(target: Target, workspace_id: str, lakehouse_id: str, host: str,
            port: str, expect: list[str], schema: str = "dbo") -> dict:
    """Refresh the endpoint, then confirm every expected table.

    Returns a row count per table -- numbers, not a status. A count is the only
    version of this check that cannot pass on an empty table.
    """
    synced = refresh_metadata(target, workspace_id, lakehouse_id)
    print(f"sql endpoint: refreshMetadata reported {len(synced)} table(s)", flush=True)
    conn = connect(target, lakehouse_id, host, port)
    present = tables(conn)
    missing = [t for t in expect if f"{schema}.{t}" not in present]
    if missing:
        raise ReflectionError(
            f"the SQL analytics endpoint does not expose {missing} even after "
            f"refreshMetadata reported {len(synced)} table(s). It sees "
            f"{sorted(present)}. Two things put a silver table here and not "
            f"there: it was written WITHOUT an explicit LOCATION under the "
            f"lakehouse's Tables/ (check location_root -- such a table is real "
            f"in the Spark catalog and invisible to this endpoint), or its "
            f"Delta log is not readable from the endpoint's side.")
    counts = {}
    for t in expect:
        counts[t] = conn.cursor().execute(
            f"SELECT COUNT(*) FROM {schema}.{t}").fetchone()[0]
    return counts
