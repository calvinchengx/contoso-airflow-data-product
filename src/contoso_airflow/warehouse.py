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

import struct
import time

from .target import Target

# SQL_COPT_SS_ACCESS_TOKEN -- the ODBC attribute Fabric's Entra auth goes
# through. 4-byte little-endian length, then the token as UTF-16-LE. This is
# the same injection dbt-fabric performs internally; doing it here means the
# witness and the adapter authenticate identically.
SQL_COPT_SS_ACCESS_TOKEN = 1256


class ReflectionError(RuntimeError):
    """Silver is not visible through the SQL analytics endpoint."""


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


def reflect(target: Target, lakehouse_id: str, host: str, port: str,
            expect: list[str], schema: str = "dbo") -> dict:
    """Connect to the lakehouse endpoint and confirm every expected table.

    Returns a row count per table -- numbers, not a status. A count is the only
    version of this check that cannot pass on an empty table.
    """
    conn = connect(target, lakehouse_id, host, port)
    present = tables(conn)
    missing = [t for t in expect if f"{schema}.{t}" not in present]
    if missing:
        raise ReflectionError(
            f"the SQL analytics endpoint does not expose {missing}. "
            f"It sees {sorted(present)}. A silver table written WITHOUT an "
            f"explicit LOCATION under the lakehouse's Tables/ is real in the "
            f"Spark catalog and invisible here -- check location_root.")
    counts = {}
    for t in expect:
        counts[t] = conn.cursor().execute(
            f"SELECT COUNT(*) FROM {schema}.{t}").fetchone()[0]
    return counts
