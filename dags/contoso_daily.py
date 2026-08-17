"""The Contoso daily pipeline: four vendors → landing → bronze.

This replaces `contoso-fabric-platform/platform/pipeline.py`, which is a
`STEPS = [(name, description), …]` list run in order, stopping at the first
failure. The steps were always a graph; a list was the only shape available
without an orchestrator. Here the four vendors genuinely are independent, so
they are four mapped tasks that retry alone rather than four positions in a
sequence where the second failing means the third never runs.

TASK SDK ONLY (`airflow.sdk`). That is Airflow 3's boundary between task code
and the scheduler's internals, and it is what lets this same file run on a
managed Airflow in production without edits.

NOTHING HERE NAMES A TARGET. `conn_id="fabric"` is the whole of it: no host, no
tenant, no grant type, no branch on emulator-versus-real. The platform
provisions that connection against fabric-emulator locally and against real
Fabric in production, and this file cannot tell the difference — which is the
property that makes "the same DAGs, against real Fabric" true rather than hoped
for.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

WORKSPACE = "contoso-analytics"
LAKEHOUSE = "lake.Lakehouse"

# Which vendor produces which bronze tables. Named here because the mapping is
# the pipeline's shape, and burying it in four near-identical task bodies is how
# a fifth vendor becomes a copy-paste.
VENDORS = [
    {"name": "contoso_pos", "conn": "contoso_pos",
     "tables": {"customers": "bronze_pos_customers", "orders": "bronze_pos_orders"}},
    {"name": "contoso_web", "conn": "contoso_web",
     "tables": {"customers": "bronze_web_customers", "products": "bronze_web_products",
                "orders": "bronze_web_orders"}},
    {"name": "contoso_reference", "conn": "contoso_reference",
     "tables": {"product_hierarchy": "bronze_ref_product_hierarchy",
                "fx_rates": "bronze_ref_fx_rates"}},
]


@dag(
    dag_id="contoso_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["contoso", "bronze"],
    doc_md=__doc__,
)
def contoso_daily():
    @task
    def provision() -> dict:
        """The workspace and lakehouse this product needs, by NAME.

        Addressed by name rather than id on purpose: the id differs per target
        and per run, the name is the cross-target address. Idempotent — an
        existing workspace is the normal case on every run after the first.
        """
        from contoso_airflow.provision import ensure_workspace
        from contoso_airflow.target import Target

        target = Target.from_connection("fabric")
        return ensure_workspace(target, WORKSPACE, LAKEHOUSE)

    @task
    def land(vendor: dict, ctx: dict) -> dict:
        """One vendor, pulled by dlt and landed VERBATIM.

        Mapped rather than sequenced: a vendor whose API is down retries by
        itself and does not hold up the other three. That is the whole reason
        this is a DAG and not the step list it replaces.
        """
        from airflow.sdk import BaseHook

        from contoso_airflow.sources import http_vendors
        from contoso_airflow.target import Target

        target = Target.from_connection("fabric")
        conn = BaseHook.get_connection(vendor["conn"])
        source = {
            "contoso_pos": http_vendors.pos_source,
            "contoso_web": http_vendors.web_source,
            "contoso_reference": http_vendors.reference_source,
        }[vendor["name"]]

        # ITERATE THE RESOURCE, do not run a load. dlt's extraction is what is
        # wanted here -- the source definitions, the paging, the per-feed
        # resources -- and its extract step is exactly that. A pipeline adds
        # normalise+load, and the only thing there would be to load is the
        # MANIFEST, into a throwaway in-memory database that is discarded when
        # the task ends. That is machinery with no beneficiary, and reaching for
        # it is what produced `InvalidInMemoryDuckdbCredentials` here.
        #
        # When incremental extraction arrives it will need a pipeline, because
        # dlt keeps incremental state against one -- and it will need somewhere
        # DURABLE to keep it, which an in-memory destination could never have
        # provided. That is a Phase 2 decision, and pretending to have made it
        # now would have left a pipeline whose state silently reset every run.
        resources = source(
            base_url=conn.host, api_key=conn.password or "",
            target=target, workspace=ctx["workspace"],
            lakehouse=ctx["lakehouse"], day=ctx["day"])
        manifest = [dict(row) for row in resources]
        if not manifest:
            raise ValueError(f"{vendor['name']}: landed nothing")
        return {"vendor": vendor["name"], "manifest": manifest}

    @task
    def to_bronze(landed: dict, ctx: dict) -> dict:
        """Landing → bronze, re-reading the landed bytes rather than trusting
        the step that wrote them."""
        from contoso_airflow import bronze
        from contoso_airflow.target import Target

        target = Target.from_connection("fabric")
        vendor = next(v for v in VENDORS if v["name"] == landed["vendor"])
        out = {}
        for feed, table in vendor["tables"].items():
            parts = [p for p in landed["manifest"] if p["feed"] == feed]
            if not parts:
                raise ValueError(f"{landed['vendor']}: nothing landed for feed {feed!r}")
            out[table] = bronze.build_table(
                target, ctx["workspace"], ctx["lakehouse"], parts, table)
        return {"vendor": landed["vendor"], "tables": out}

    @task(outlets=[Asset("contoso://bronze")])
    def report(results: list[dict]) -> dict:
        """One line per bronze table, and the totals silver will be held to.

        Emits the `contoso://bronze` asset, so the dbt DAG is triggered by
        bronze ACTUALLY LANDING rather than by a clock that hopes it did.
        """
        total = 0
        for r in results:
            for table, m in r["tables"].items():
                print(f"bronze {table}: {m['rows']} rows, {m['parts']} part(s), "
                      f"delta version {m['version']}", flush=True)
                total += m["rows"]
        print(f"BRONZE_TOTAL_ROWS {total}", flush=True)
        return {"total_rows": total,
                "tables": {t: m["rows"] for r in results for t, m in r["tables"].items()}}

    ctx = provision()
    landed = land.partial(ctx=ctx).expand(vendor=VENDORS)
    report(to_bronze.partial(ctx=ctx).expand(landed=landed))


contoso_daily()
