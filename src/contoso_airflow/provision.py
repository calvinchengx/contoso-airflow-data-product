"""The workspace and lakehouse this product needs, created if absent.

Addressed BY NAME, never by id. The id differs per target and per run; the name
is the cross-target address, and it is what makes the same DAG resolve to the
right place on the emulator and on real Fabric.

Idempotent because a pipeline run is not a first run. After day one the normal
case is that everything already exists, and a provisioner that treats that as an
error is a provisioner that can only be run once.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request


from . import tls
from .target import Target


def _api(target: Target, method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{target.api_root}{path}", data=data, method=method)
    req.add_header("Authorization", f"Bearer {target.fabric_token()}")
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120, context=tls.CONTEXT) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read()[:300].decode(errors="replace")}


def _find(items: list[dict], name: str) -> dict | None:
    return next((i for i in items if i.get("displayName") == name), None)


def _ensure_warehouse(target: Target, workspace_id: str, name: str) -> dict:
    """The Warehouse gold builds into.

    A SEPARATE ITEM FROM THE LAKEHOUSE, because that is what Fabric gives you:
    a Lakehouse is Delta that Spark writes and T-SQL only reads, and gold's
    star is written in T-SQL. Both are databases on the same TDS endpoint, so
    gold reads silver across the boundary by three-part name.

    Created through `/items` with an explicit type rather than a typed
    endpoint, which is the shape the sibling platform uses and the one real
    Fabric documents for Warehouse.
    """
    status, items = _api(target, "GET", f"/v1/workspaces/{workspace_id}/items")
    if status >= 300:
        raise RuntimeError(f"cannot list items: {status} {items}")
    wh = next((i for i in items.get("value", [])
               if i.get("displayName") == name and i.get("type") == "Warehouse"), None)
    if wh is None:
        status, wh = _api(target, "POST", f"/v1/workspaces/{workspace_id}/items",
                          {"displayName": name, "type": "Warehouse"})
        if status >= 300:
            raise RuntimeError(f"cannot create warehouse {name!r}: {status} {wh}")
    return wh


def ensure_workspace(target: Target, workspace: str, lakehouse: str,
                     warehouse: str = "contoso_warehouse") -> dict:
    """Return the context every later task needs: names, and the ids they map to."""
    status, listing = _api(target, "GET", "/v1/workspaces")
    if status >= 300:
        raise RuntimeError(f"cannot list workspaces: {status} {listing}")
    ws = _find(listing.get("value", []), workspace)
    if ws is None:
        status, ws = _api(target, "POST", "/v1/workspaces", {"displayName": workspace})
        if status >= 300:
            raise RuntimeError(f"cannot create workspace {workspace!r}: {status} {ws}")

    # The lakehouse's DISPLAY name is `lake`; its OneLake path segment is
    # `lake.Lakehouse`. Splitting here rather than at every call site, because
    # getting it wrong produces a 404 that reads like a missing file.
    display = lakehouse.split(".", 1)[0]
    status, items = _api(target, "GET", f"/v1/workspaces/{ws['id']}/lakehouses")
    if status >= 300:
        raise RuntimeError(f"cannot list lakehouses: {status} {items}")
    lh = _find(items.get("value", []), display)
    if lh is None:
        status, lh = _api(target, "POST", f"/v1/workspaces/{ws['id']}/lakehouses",
                          {"displayName": display})
        if status >= 300:
            raise RuntimeError(f"cannot create lakehouse {display!r}: {status} {lh}")

    wh = _ensure_warehouse(target, ws["id"], warehouse)

    from datetime import datetime, timezone
    return {
        # OneLake addresses by NAME, the control plane by id. Both travel.
        "workspace": workspace,
        "workspace_id": ws["id"],
        "lakehouse": lakehouse,
        "lakehouse_id": lh["id"],
        # TDS addresses a database by the ITEM ID, for both the warehouse gold
        # writes and the lakehouse endpoint gold reads.
        "warehouse": warehouse,
        "warehouse_id": wh["id"],
        "day": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }
