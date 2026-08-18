"""The three HTTP vendors, as dlt sources.

WHAT DLT OWNS HERE, and it is the hard part: paging, auth, retry, and the
per-run state that makes a re-run incremental rather than duplicative. What it
does NOT own is the shape of what lands. The family's rule, from
`fabric-platform-notebook-pipelines/platform/ingest_pos.py`:

    Landed VERBATIM — no parsing, no reshaping. Bronze's job is to be the bytes
    as they arrived, so that a question about the source can be answered
    without going back to the vendor.

So each resource yields a MANIFEST — what was fetched, where it was put, how
many bytes — while the response body goes to OneLake unchanged. dlt's normalise
step therefore sees the manifest, not the payload, and cannot reshape a vendor's
bytes on the way to landing.

THREE VENDORS, THREE DIALECTS, none of it smoothed over:

  POS        delimited text (csv) + JSON Lines, PAGED. Pages land as separate
             parts. Reassembling here would put the whole export in this
             process's memory and lose the vendor's own pagination as evidence.
  Web        JSON arrays, orders NESTED (one order carries its `lines`).
             Flattening is a decision and it belongs downstream.
  Reference  PARQUET, not paged, ~4 KB. The master-data publisher — not an
             operational system, which is why it is a vendor at all.
"""
from __future__ import annotations

import urllib.error
import urllib.request
from dataclasses import dataclass

import dlt

from ..io import onelake
from ..target import Target


@dataclass(frozen=True)
class Feed:
    path: str
    subdir: str
    ext: str


# Exactly the feeds fabric-platform-notebook-pipelines pulls. Same paths, same extensions,
# same landing layout — because `compare_products.py` has to be able to hold
# both platforms to the same numbers, and a different feed list would make a
# difference in the totals mean nothing.
POS_FEEDS = [Feed("/api/v1/export/customers", "customers", "csv"),
             Feed("/api/v1/export/orders", "orders", "jsonl")]
WEB_FEEDS = [Feed("/api/v2/export/customers", "customers", "json"),
             Feed("/api/v2/export/products", "products", "json"),
             Feed("/api/v2/export/orders", "orders", "json")]
REFERENCE_FEEDS = [Feed("/reference/v1/product-hierarchy", "product_hierarchy", "parquet"),
                   Feed("/reference/v1/fx-rates", "fx_rates", "parquet")]


def _get(url: str, api_key: str, page: int | None = None) -> tuple[bytes, dict]:
    if page is not None:
        url = f"{url}?page={page}"
    req = urllib.request.Request(url)
    req.add_header("X-API-Key", api_key)
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read(), dict(r.headers)


def _land(target: Target, workspace: str, lakehouse: str, rel: str, blob: bytes) -> dict:
    if not blob:
        # An empty body is a vendor fault, and landing it would defer the
        # failure to bronze — where it looks like a parsing bug rather than a
        # missing export.
        raise ValueError(f"vendor returned an empty body for {rel}")
    written = onelake.upload(target, workspace, lakehouse, rel, blob)
    if written != len(blob):
        raise ValueError(f"{rel}: wrote {written} of {len(blob)} bytes")
    return {"path": rel, "bytes": written}


def _paged_vendor(name: str, base_url: str, api_key: str, feeds: list[Feed],
                  target: Target, workspace: str, lakehouse: str, day: str):
    """POS and Web: paged HTTP, one landed part per page."""
    for feed in feeds:
        body, headers = _get(f"{base_url}{feed.path}", api_key, page=1)
        total = int(headers.get("X-Total-Pages", 1))
        for page in range(1, total + 1):
            if page > 1:
                body, headers = _get(f"{base_url}{feed.path}", api_key, page=page)
            rel = (f"Files/landing/{name}/{day}/{feed.subdir}/"
                   f"part-{page:04d}.{feed.ext}")
            landed = _land(target, workspace, lakehouse, rel, body)
            yield {"vendor": name, "feed": feed.subdir, "page": page,
                   "pages": total, "day": day, **landed}


@dlt.source(name="contoso_pos")
def pos_source(base_url: str, api_key: str, target: Target,
               workspace: str, lakehouse: str, day: str):
    @dlt.resource(name="pos_landing", write_disposition="append")
    def landing():
        yield from _paged_vendor("contoso_pos", base_url, api_key, POS_FEEDS,
                                 target, workspace, lakehouse, day)
    return landing


@dlt.source(name="contoso_web")
def web_source(base_url: str, api_key: str, target: Target,
               workspace: str, lakehouse: str, day: str):
    @dlt.resource(name="web_landing", write_disposition="append")
    def landing():
        yield from _paged_vendor("contoso_web", base_url, api_key, WEB_FEEDS,
                                 target, workspace, lakehouse, day)
    return landing


@dlt.source(name="contoso_reference")
def reference_source(base_url: str, api_key: str, target: Target,
                     workspace: str, lakehouse: str, day: str):
    """Not paged: the whole export is about four kilobytes, and asking for a
    page it does not implement would be inventing a protocol the vendor does
    not speak."""
    @dlt.resource(name="reference_landing", write_disposition="append")
    def landing():
        for feed in REFERENCE_FEEDS:
            body, _ = _get(f"{base_url}{feed.path}", api_key)
            rel = f"Files/landing/contoso_reference/{day}/{feed.subdir}.{feed.ext}"
            landed = _land(target, workspace, lakehouse, rel, body)
            yield {"vendor": "contoso_reference", "feed": feed.subdir,
                   "page": 1, "pages": 1, "day": day, **landed}
    return landing
