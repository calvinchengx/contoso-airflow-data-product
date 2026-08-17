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


def test_the_dag_imports_and_has_the_shape_the_pipeline_needs(monkeypatch):
    pytest.importorskip("airflow.sdk")
    pytest.importorskip("cosmos")
    # Cosmos CACHES its rendered graph in an Airflow Variable, which needs a
    # metadata database. These tests are deliberately hermetic -- no platform,
    # no emulator, no credentials -- so the cache is off here rather than a
    # database being stood up for a test about the product's own shape.
    # Rendering still happens; only its persistence is skipped.
    monkeypatch.setenv("AIRFLOW__COSMOS__ENABLE_CACHE", "False")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
    import contoso_daily

    dag = contoso_daily.contoso_daily()
    ids = {t.task_id for t in dag.tasks}
    # The hops that must exist, by name: bronze lands, then dbt builds silver,
    # then gold. A rename that broke the chain would otherwise pass.
    assert {"provision", "land", "to_bronze", "report", "fabric_env",
            "reflect"} <= ids
    assert any(t.startswith("silver.") for t in ids), sorted(ids)[:12]
    assert any(t.startswith("gold.") for t in ids), sorted(ids)[:12]
    # The gate is BETWEEN the two groups, not merely present. Gold reads silver
    # across a database boundary, and without this it fails on unresolved
    # sources -- which says a name did not resolve, not that silver was written
    # somewhere the Lakehouse cannot see.
    reflect = dag.get_task("reflect")
    assert any(t.startswith("silver.") for t in reflect.upstream_task_ids)
    assert any(t.startswith("gold.") for t in reflect.downstream_task_ids)


def test_the_two_profiles_do_not_share_one_bearer():
    """Silver's token is the control plane's; gold's is Azure SQL's.

    They are rendered from the same task environment, so one key for both
    means whichever profile is wrong gets a token the other surface refuses.
    Measured once as `login failed: invalid token: audience not accepted`,
    reported by dbt as a generic authorization error.
    """
    root = pathlib.Path(__file__).resolve().parents[1] / "dbt"
    silver = (root / "silver" / "profiles.yml").read_text()
    gold = (root / "gold" / "profiles.yml").read_text()
    assert "env_var('DBT_ACCESS_TOKEN'" in silver
    assert "env_var('DBT_SQL_ACCESS_TOKEN'" in gold
    assert "env_var('DBT_ACCESS_TOKEN'" not in gold


def test_silver_is_written_where_the_lakehouse_can_see_it():
    """`location_root`, or gold has no sources at all.

    Without it the adapter emits no LOCATION, the engine writes into its own
    warehouse directory, and the tables are real, queryable from Spark, and
    absent from the SQL analytics endpoint. Measured: the endpoint listed the
    8 bronze tables and none of silver's.
    """
    project = (pathlib.Path(__file__).resolve().parents[1]
               / "dbt" / "silver" / "dbt_project.yml").read_text()
    assert "+location_root:" in project
    assert "DBT_SILVER_LOCATION_ROOT" in project


def test_bronze_reads_a_debezium_envelope_not_the_envelope_itself():
    # Parsing these as plain JSON Lines would land envelopes and lose every
    # operation inside them -- green, and describing nothing.
    import json
    env = json.dumps({"payload": {"op": "u", "ts_ms": 1,
                                  "before": {"erp_customer_id": 7, "segment": "old"},
                                  "after": {"erp_customer_id": 7, "segment": "new"}}})
    rows = bronze._parse(env.encode() + b"\n", "cdc")
    assert rows == [{"erp_customer_id": 7, "segment": "new", "__op": "u", "__ts_ms": 1}]


def test_a_delete_keeps_its_identity_from_before():
    # A delete carries no `after`; dropping it would silently lose 1,200 events.
    import json
    env = json.dumps({"payload": {"op": "d", "ts_ms": 2,
                                  "before": {"erp_customer_id": 9}, "after": None}})
    rows = bronze._parse(env.encode() + b"\n", "cdc")
    assert rows == [{"erp_customer_id": 9, "__op": "d", "__ts_ms": 2}]
