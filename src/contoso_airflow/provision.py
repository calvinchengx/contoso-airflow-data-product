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

from .target import Target


def _api(target: Target, method: str, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{target.api_root}{path}", data=data, method=method)
    req.add_header("Authorization", f"Bearer {target.fabric_token()}")
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read()[:300].decode(errors="replace")}


def _find(items: list[dict], name: str) -> dict | None:
    return next((i for i in items if i.get("displayName") == name), None)


def ensure_workspace(target: Target, workspace: str, lakehouse: str) -> dict:
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

    from datetime import datetime, timezone
    return {
        # OneLake addresses by NAME, the control plane by id. Both travel.
        "workspace": workspace,
        "workspace_id": ws["id"],
        "lakehouse": lakehouse,
        "lakehouse_id": lh["id"],
        "day": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }
