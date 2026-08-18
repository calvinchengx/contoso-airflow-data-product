"""OneLake, over ADLS Gen2, with nothing but stdlib and a bearer token.

WHY NOT AN SDK. Phase 0 measured every SDK-shaped route into this emulator and
all of them failed the same way: dlt's Azure credentials carry account-key, SAS
and service-principal shapes and no bearer field, so an unrecognised credential
falls through to `DefaultAzureCredential`. Unisolated, that authenticated
against REAL Microsoft endpoints using the developer's own `az login` state and
looped on AADSTS50020 for ninety seconds. A misconfigured SDK does not fail
here — it retargets production.

create → append → flush is three plain HTTP calls. It is the same sequence
`fabric-platform-notebook-pipelines/platform/fabric.py:upload()` performs against BOTH
targets, it has no credential chain to fall through, and the emulator's own
`e2e/adls-sdk` and `azcopy` witnesses already prove the surface.
"""
from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request

from .. import tls
from ..target import Target

# Chunked because that is how a real ADLS client writes a large export, and
# because a single append would say nothing about whether `position` is handled
# across calls. Same reason the platform this mirrors chunks at 8 MiB.
CHUNK = 8 * 1024 * 1024


class OneLakeError(RuntimeError):
    """A OneLake call that failed, naming what was attempted."""


def _request(target: Target, method: str, url: str, tok: str,
             data: bytes | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {tok}")
    if data is not None:
        req.add_header("Content-Type", "application/octet-stream")
    # The emulator serves OneLake on the Fabric port and routes by Host header.
    # Without it the request reaches the CONTROL PLANE, which answers 405 —
    # correctly, and confusingly if you are expecting a storage error. Real
    # Fabric has its own hostname and supplies no override.
    if target.onelake_host_header:
        req.add_header("Host", target.onelake_host_header)
    try:
        with urllib.request.urlopen(req, timeout=300, context=tls.CONTEXT) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        raise OneLakeError(
            f"{method} {url.split('?')[0]} -> {e.code}: {e.read()[:200]!r}") from e


def _url(target: Target, workspace: str, item: str, rel: str, query: str = "") -> str:
    return f"{target.onelake_url}/{workspace}/{item}/{urllib.parse.quote(rel)}{query}"


def tables_root(workspace: str, lakehouse: str) -> str:
    """`Tables/` in OneLake -- where a Lakehouse's tables ARE.

    Not a convention this product invented. A Delta directory under `Tables/`
    is what the Lakehouse discovers, which is what puts it in the engine's
    catalog AND what its SQL analytics endpoint exposes as T-SQL. Silver's dbt
    models are given this as `location_root` for exactly that reason.

    In `abfss://` form because that is the address the ENGINE reads; the
    functions above speak to the DFS surface over HTTP and take the pieces
    separately.
    """
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables"


def upload(target: Target, workspace: str, item: str, rel: str, blob: bytes) -> int:
    """create → append → flush. Returns bytes written."""
    tok = target.storage_token()
    _request(target, "PUT", _url(target, workspace, item, rel, "?resource=file"), tok)
    pos = 0
    while pos < len(blob):
        part = blob[pos:pos + CHUNK]
        _request(target, "PATCH",
                 _url(target, workspace, item, rel, f"?action=append&position={pos}"),
                 tok, part)
        pos += len(part)
    _request(target, "PATCH",
             _url(target, workspace, item, rel, f"?action=flush&position={pos}"), tok)
    return pos


def read(target: Target, workspace: str, item: str, rel: str) -> bytes:
    """The bytes back. Used by bronze, and by the landing witness to prove the
    write was verbatim — a writer confirming itself proves nothing."""
    _, body = _request(target, "GET", _url(target, workspace, item, rel),
                       target.storage_token())
    return body
