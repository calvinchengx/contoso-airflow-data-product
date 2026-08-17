"""Product tests: no platform, no emulator, no credentials.

The runtimes are exercised by the witness; these are about the product's own
decisions — the ones that would be wrong before anything is stood up.
"""
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from contoso_airflow import bronze  # noqa: E402
from contoso_airflow.sources import http_vendors  # noqa: E402


def test_the_feeds_match_the_fabric_platform_exactly():
    # compare_products.py holds both platforms to the same numbers. A feed list
    # that drifted would make an equal total mean nothing.
    assert [(f.path, f.subdir, f.ext) for f in http_vendors.POS_FEEDS] == [
        ("/api/v1/export/customers", "customers", "csv"),
        ("/api/v1/export/orders", "orders", "jsonl")]
    assert [f.subdir for f in http_vendors.WEB_FEEDS] == ["customers", "products", "orders"]
    assert [f.subdir for f in http_vendors.REFERENCE_FEEDS] == [
        "product_hierarchy", "fx_rates"]


@pytest.mark.parametrize("blob,ext,rows", [
    (b'{"id":1}\n{"id":2}\n', "jsonl", 2),
    (b'[{"id":1},{"id":2},{"id":3}]', "json", 3),
    (b'{"id":1}', "json", 1),                      # a lone object is still one row
    (b"id,name\n1,a\n2,b\n", "csv", 2),
])
def test_bronze_parses_each_vendor_dialect(blob, ext, rows):
    assert len(bronze._parse(blob, ext)) == rows


def test_bronze_refuses_a_dialect_it_does_not_know():
    # Guessing would be the silent-wrong-thing this product exists to refuse.
    with pytest.raises(ValueError, match="no parser"):
        bronze._parse(b"...", "xml")


def test_web_orders_keep_their_nested_lines():
    # Flattening is silver's decision. Bronze doing it would destroy the only
    # copy of what the vendor sent.
    rows = bronze._parse(b'[{"id":1,"lines":[{"sku":"A"},{"sku":"B"}]}]', "json")
    assert rows[0]["lines"] == [{"sku": "A"}, {"sku": "B"}]


def test_the_dag_imports_and_has_the_shape_the_pipeline_needs():
    pytest.importorskip("airflow.sdk")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
    import contoso_daily  # noqa: F401
