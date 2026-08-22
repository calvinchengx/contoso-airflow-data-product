"""Write this runtime's snapshot from a shell.

    python scripts/snapshot.py <host> <port> <warehouse-id> [out.json]

THE LOGIC IS IN `contoso_airflow.snapshot`, not here. The DAG's `publish` task
runs the same code, because a snapshot produced by a witness and a snapshot
produced by a run have to be the same artefact or neither means anything.
"""
from __future__ import annotations

import json
import pathlib
import struct
import sys

from contoso_airflow.snapshot import build, out_path, verdicts, write
from contoso_airflow.target import Target

# Re-exported: the tests that hold `verdicts` to its refusals import it from
# here, and it is the same function.
__all__ = ["build", "verdicts", "write"]

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
    out = pathlib.Path(sys.argv[4]) if len(sys.argv) > 4 else out_path()

    target = Target.from_env()
    conn = mssql_python.connect(
        f"Server={host},{port};Database={warehouse};"
        f"Encrypt=no;TrustServerCertificate=yes",
        attrs_before=_attrs(target.sql_token()), timeout=60)

    snapshot = build(conn, warehouse)
    write(snapshot, out)
    print(json.dumps(snapshot, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
