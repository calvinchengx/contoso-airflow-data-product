"""Publish the product's semantic model to Fabric, and hold it to gold.

The DEFINITION is core's (`contoso_product.semantic`): tables, columns,
measures, and the rule that every family number has one. This module is the
binding — the half core may not hold, because core names no engine. It builds
the Direct Lake M expression from the run's own workspace and warehouse ids,
publishes the item over the Fabric REST surface, and queries it back through
the Power BI `executeQueries` contract.

WHY THE CONTRACT READS GOLD TWICE. `semantic_verdict` sums the same three
columns over TDS and evaluates the same three measures over DAX, then requires
them equal. That is deliberately not "assert the family's numbers": those are
already asserted by `compare_products`, and a semantic layer that merely
repeats a constant would pass while serving a model bound to the wrong table.
What this proves is that the BI path and the SQL path read the same rows of
the same run — so a model pointed at a stale warehouse, an empty schema, or
yesterday's entity fails even when the constant would have matched.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal

from contoso_product import semantic as core

from . import tls
from .target import Target

MODEL_NAME = "contoso-analytics"
PBI_AUDIENCE = "https://analysis.windows.net/powerbi/api"

# Gold lands in `dbo` on a Fabric warehouse. A deployment fact, which is why
# core takes it as a parameter rather than knowing it.
GOLD_SCHEMA = "dbo"


class SemanticError(RuntimeError):
    """A publish or a query this module declines to call a success."""


def _request(url: str, token: str, *, method: str = "GET",
             body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=300, context=tls.CONTEXT) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise SemanticError(
            f"{method} {url} -> {e.code}: {e.read()[:300]!r}") from e


# OneLake's canonical address. NOT derived from the target's api_root, and
# that distinction cost a witness: rewriting the control-plane host carries its
# PORT along (`onelake.dfs...:9443`), and a Direct Lake expression on real
# Fabric never has one. The emulator matches Fabric here exactly -- its parser
# wants `https://onelake.dfs.fabric.microsoft.com/<ws>/<item>` with the path
# straight after the host -- so the ported form was refused as
# `InvalidDataset: shared expression must contain an onelake... URL`, which
# reads as a missing URL rather than a malformed one.
#
# Written as a constant because it IS constant: the same string deploys against
# the emulator and against a real tenant. Only the ids after it change.
ONELAKE_ROOT = "https://onelake.dfs.fabric.microsoft.com"


def direct_lake_expression(workspace_id: str, warehouse_id: str) -> str:
    """The M expression naming where this run's warehouse lives.

    Built here rather than in core: it names a host, which core is forbidden
    to do (`test_no_engine_named_in_core`). The shape is Fabric's own — the
    same expression a Desktop-authored Direct Lake model carries.
    """
    return (f'let Source = AzureStorage.DataLake('
            f'"{ONELAKE_ROOT}/{workspace_id}/{warehouse_id}", '
            f'[HierarchicalNavigation=true]) in Source')


def publish(target: Target, ctx: dict) -> str:
    """Create or replace the SemanticModel item; return its Fabric item id.

    Replace rather than update-in-place: a definition that changed shape
    (a column added to gold, a measure renamed) must not leave the item
    carrying half of each, and this product's items are disposable by design.
    """
    expression = direct_lake_expression(
        ctx["workspace_id"], ctx["warehouse_id"])
    bim = core.model_bim(expression, GOLD_SCHEMA, model_name=MODEL_NAME)
    payload = base64.b64encode(json.dumps(bim).encode()).decode()
    token = target.fabric_token()
    ws = ctx["workspace_id"]

    listing = _request(f"{target.api_root}/v1/workspaces/{ws}/items", token)
    for item in listing.get("value", []):
        if item.get("type") == "SemanticModel" and item.get("displayName") == MODEL_NAME:
            _request(f"{target.api_root}/v1/workspaces/{ws}/items/{item['id']}",
                     token, method="DELETE")
            print(f"semantic: replaced previous item {item['id']}", flush=True)

    _request(f"{target.api_root}/v1/workspaces/{ws}/items", token, method="POST",
             body={"displayName": MODEL_NAME, "type": "SemanticModel",
                   "definition": {"parts": [{"path": "model.bim",
                                             "payload": payload,
                                             "payloadType": "InlineBase64"}]}})

    # RESOLVED FROM THE POWER BI SURFACE, not from the create response. The
    # dataset id `executeQueries` takes is that surface's own, and item-create
    # answers 202 with no id at all when the emulator completes it as an LRO —
    # measured, and the reason this is a listing rather than a field read.
    datasets = _request(
        f"{target.api_root}/v1.0/myorg/groups/{ws}/datasets",
        target.token(PBI_AUDIENCE))
    for ds in datasets.get("value", []):
        if ds.get("name") == MODEL_NAME:
            print(f"semantic: published, dataset {ds['id']}", flush=True)
            return ds["id"]
    raise SemanticError(
        f"{MODEL_NAME} was created but is not listed on the Power BI surface; "
        f"nothing can query it")


def evaluate(target: Target, workspace_id: str, dataset_id: str) -> dict[str, Decimal]:
    """Evaluate every measure the model declares, through executeQueries."""
    row = ", ".join(f'"{name}", [{name}]' for name in core.MEASURES)
    result = _request(
        f"{target.api_root}/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/executeQueries",
        target.token(PBI_AUDIENCE), method="POST",
        body={"queries": [{"query": f"EVALUATE ROW({row})"}]})
    try:
        rows = result["results"][0]["tables"][0]["rows"]
    except (KeyError, IndexError) as exc:
        raise SemanticError(f"executeQueries returned no rows: {result}") from exc
    if len(rows) != 1:
        raise SemanticError(f"ROW() returned {len(rows)} rows, expected 1: {rows}")
    got = {}
    for name in core.MEASURES:
        key = f"[{name}]"
        if key not in rows[0]:
            raise SemanticError(
                f"the model answered without measure {name!r}: {sorted(rows[0])}")
        got[name] = Decimal(str(rows[0][key]))
    return got


def semantic_verdict(measured: dict[str, Decimal],
                     expected: dict[str, Decimal]) -> dict:
    """Compare DAX against SQL, and refuse anything but exact agreement.

    Exact, not tolerant: both sides read the same Delta/SQL rows of the same
    run, so a difference is a wrong binding or a wrong measure, never
    accumulated float error. `revenue_usd` is money and the snapshot carries
    it fixed-point; a tolerance here would be a place for a real defect to
    hide.

    Separated from `evaluate` so the deciding logic is testable without a
    warehouse — the same reason core's `verdicts()` is its own function.
    """
    if set(measured) != set(expected):
        raise SemanticError(
            f"measures answered {sorted(measured)} but gold offers "
            f"{sorted(expected)}; refusing a partial comparison")
    disagreed = {name: {"dax": str(measured[name]), "sql": str(expected[name])}
                 for name in expected if measured[name] != expected[name]}
    if disagreed:
        raise SemanticError(
            f"the semantic model disagrees with the gold it is bound to: "
            f"{json.dumps(disagreed, indent=2)}")
    return {name: str(v) for name, v in sorted(measured.items())}
