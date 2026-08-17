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
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig

import os
import pathlib

from contoso_product import gold_dir

WORKSPACE = "contoso-analytics"
LAKEHOUSE = "lake.Lakehouse"

# The product's dbt projects, resolved from THIS FILE rather than an absolute
# path: the bundle mounts the repo at /opt/product locally and clones it
# somewhere else in production, and a hardcoded path would be right in exactly
# one of those.
DBT_DIR = pathlib.Path(__file__).resolve().parent.parent / "dbt"
# The venv dbt, not whatever is first on PATH -- cosmos shells out, and the
# worker's PATH is not guaranteed to lead anywhere useful from a task.
DBT_BIN = os.environ.get("DBT_EXECUTABLE", "/home/airflow/.local/bin/dbt")

# GOLD'S sources.yml DEMANDS THESE AT PARSE TIME, and not because the shared
# project is careless. It reads:
#
#     database: "{{ env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID')) }}"
#
# The outer call has a fallback, but the fallback is ITSELF an env_var with no
# default, and Jinja evaluates it eagerly -- so LAKEHOUSE_ID is required even
# when CONTOSO_SILVER_DATABASE is set. Cosmos renders by running `dbt ls`, so an
# unset value stops the DAG appearing at all.
#
# Satisfied here rather than fixed there: contoso-data-product is consumed by
# three other platforms, and changing a shared project to suit one consumer's
# renderer is the wrong direction. The real value is supplied per run.
os.environ.setdefault("LAKEHOUSE_ID", "00000000-0000-0000-0000-000000000000")

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
    # The fourth vendor is not an API. Its history arrives as a change STREAM,
    # which is why it carries a broker rather than a base URL -- and why a
    # snapshot of the same table would be a different and much weaker claim.
    {"name": "contoso_erp", "conn": "contoso_erp",
     "tables": {"changes": "bronze_erp_customer_changes"}},
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
        if vendor["name"] == "contoso_erp":
            from contoso_airflow.sources import erp_cdc

            # The broker and topic ride in the connection's extra, the way the
            # HTTP vendors' base URL rides in its host. Same seam, same reason:
            # in production these point at the real ERP's stream and this file
            # does not change.
            extra = conn.extra_dejson
            resources = erp_cdc.erp_source(
                bootstrap=extra["bootstrap"], topic=extra["topic"],
                target=target, workspace=ctx["workspace"],
                lakehouse=ctx["lakehouse"], day=ctx["day"])
            manifest = [dict(row) for row in resources]
            if not manifest:
                raise ValueError("contoso_erp: landed nothing")
            return {"vendor": vendor["name"], "manifest": manifest}

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

        import os

        target = Target.from_connection("fabric")
        # Where statements go. The platform supplies it; silver cannot resolve
        # `source()` against tables the engine was never told about, so this is
        # not optional decoration.
        # REQUIRED, not optional. Silver resolves `source()` by name, and a
        # bronze table the engine was never told about does not exist to dbt.
        # This used to be `.get(...)` and the platform did not set it, so
        # registration was skipped SILENTLY and the DAG reported success on a
        # catalog that stayed empty -- the failure only visible three steps
        # later, as eight simultaneous model errors blamed on the SQL.
        try:
            agent_url = os.environ["FABRIC_SPARK_AGENT_URL"]
        except KeyError:
            raise RuntimeError(
                "FABRIC_SPARK_AGENT_URL is not set, so bronze cannot be "
                "registered with the engine's catalog and every silver model "
                "would fail to resolve its source. The platform supplies it."
            ) from None
        vendor = next(v for v in VENDORS if v["name"] == landed["vendor"])
        out = {}
        for feed, table in vendor["tables"].items():
            parts = [p for p in landed["manifest"] if p["feed"] == feed]
            if not parts:
                raise ValueError(f"{landed['vendor']}: nothing landed for feed {feed!r}")
            out[table] = bronze.build_table(
                target, ctx["workspace"], ctx["lakehouse"], parts, table,
                agent_url=agent_url)
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

    @task
    def fabric_env(ctx: dict) -> dict:
        """The dbt profiles' environment, minted per run.

        Both profiles are entirely `env_var()`-driven so production points them
        at real Fabric unedited -- which means SOMETHING has to supply those
        values, and a bearer cannot be baked into a rendered DAG. This task
        resolves them from the same connection every other task uses, and the
        dbt task groups read them back through templating.
        """
        from contoso_airflow.catalog import tables_root
        from contoso_airflow.target import Target

        target = Target.from_connection("fabric")
        return {
            # SILVER's bearer: Livy, on the Fabric control plane.
            "DBT_ACCESS_TOKEN": target.fabric_token(),
            "DBT_FABRIC_ENDPOINT": f"{target.api_root}/v1",
            "DBT_WORKSPACE_ID": ctx["workspace_id"],
            "DBT_LAKEHOUSE_ID": ctx["lakehouse_id"],
            "DBT_LAKEHOUSE_NAME": ctx["lakehouse"].split(".", 1)[0],
            # WHERE SILVER'S TABLES GO. Without it dbt-fabricspark issues
            # `create or replace table` with no LOCATION, the engine writes to
            # its own warehouse directory, and the tables are real, queryable
            # and INVISIBLE to the Lakehouse -- so the SQL analytics endpoint
            # never reflects them and gold's every source() fails. Measured:
            # the endpoint listed the 8 bronze tables and none of silver's.
            "DBT_SILVER_LOCATION_ROOT": tables_root(
                ctx["workspace"], ctx["lakehouse"]),
            # GOLD's bearer: TDS FedAuth, a different audience to the same
            # credential. Its own key, because both profiles are in the same
            # environment and one `DBT_ACCESS_TOKEN` cannot be both.
            "DBT_SQL_ACCESS_TOKEN": target.sql_token(),
            "DBT_HOST": os.environ.get("FABRIC_TDS_HOST", "fabric-emulator"),
            "DBT_PORT": os.environ.get("FABRIC_TDS_PORT", "1433"),
            # Gold BUILDS in the warehouse and READS the lakehouse endpoint;
            # two databases on the one TDS endpoint, joined by three-part name.
            "DBT_DATABASE": ctx["warehouse_id"],
            "LAKEHOUSE_ID": ctx["lakehouse_id"],
            "CONTOSO_SILVER_DATABASE": ctx["lakehouse_id"],
            # `dbo`, not the Spark database name: the endpoint reflects
            # OneLake `Tables/` into dbo regardless of the catalog namespace
            # Spark wrote under. Measured, not assumed.
            "CONTOSO_SILVER_SCHEMA": os.environ.get("CONTOSO_SILVER_SCHEMA", "dbo"),
        }

    ctx = provision()
    landed = land.partial(ctx=ctx).expand(vendor=VENDORS)
    bronze_done = report(to_bronze.partial(ctx=ctx).expand(landed=landed))
    env = fabric_env(ctx)

    # dbt's own graph becomes Airflow's. Cosmos renders ONE TASK PER MODEL with
    # dbt's dependencies as the edges, so bronze -> silver -> gold appears in the
    # UI as real lineage and a failing model retries alone rather than re-running
    # `dbt build`. Its per-model test tasks are also the quality gate: a failing
    # test fails its own task and its dependents never run, which is why there is
    # no separate contract operator re-checking the same rules in Python.
    # A DICT WHOSE VALUES ARE TEMPLATES, not a template that renders a dict.
    # `env` is a templated field, so Airflow renders each VALUE -- give it one
    # string for the whole mapping and it renders to the repr of a dict, dbt
    # receives no variables at all, and the profile silently falls back to its
    # render-time placeholders. The symptom is a 401 against
    # workspaces/00000000-0000-0000-0000-000000000000, which reads like an auth
    # problem and is really an empty environment.
    def _env(key: str) -> str:
        return "{{ ti.xcom_pull(task_ids='fabric_env')['" + key + "'] }}"

    ENV = {k: _env(k) for k in (
        "DBT_ACCESS_TOKEN", "DBT_SQL_ACCESS_TOKEN", "DBT_FABRIC_ENDPOINT",
        "DBT_WORKSPACE_ID", "DBT_LAKEHOUSE_ID", "DBT_LAKEHOUSE_NAME",
        "DBT_SILVER_LOCATION_ROOT", "DBT_HOST", "DBT_PORT",
        "DBT_DATABASE", "LAKEHOUSE_ID", "CONTOSO_SILVER_DATABASE",
        "CONTOSO_SILVER_SCHEMA")}

    silver = DbtTaskGroup(
        group_id="silver",
        project_config=ProjectConfig(dbt_project_path=DBT_DIR / "silver"),
        profile_config=ProfileConfig(
            profile_name="contoso_silver", target_name="dev",
            profiles_yml_filepath=DBT_DIR / "silver" / "profiles.yml"),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
        operator_args={"env": ENV, "install_deps": True},
    )

    # GOLD'S MODELS ARE NOT IN THIS REPO. contoso-data-product ships them -- 9
    # models, 5 singular tests, 62 schema tests -- and exists so gold is not
    # copied per platform: "two fct_sales.sql files agree until the day someone
    # fixes a bug in one of them". This product supplies the profile and points
    # dbt at the installed package.
    gold = DbtTaskGroup(
        group_id="gold",
        project_config=ProjectConfig(dbt_project_path=gold_dir()),
        profile_config=ProfileConfig(
            profile_name="contoso_gold", target_name="dev",
            profiles_yml_filepath=DBT_DIR / "gold" / "profiles.yml"),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
        operator_args={"env": ENV},
    )

    @task
    def reflect(ctx: dict) -> dict:
        """Gold reads silver over TDS. Prove it can, before gold tries.

        THE MODELS ARE THE LIST. Reading the silver project's own directory
        rather than repeating the names means a model added tomorrow is
        checked tomorrow, and a check that silently stops covering something
        is the failure mode this whole pipeline is built against.

        Placed between the two task groups because it separates two failures
        that look identical from inside dbt: silver never built, and silver
        built somewhere the Lakehouse cannot see.
        """
        from contoso_airflow.target import Target
        from contoso_airflow.warehouse import reflect as do_reflect

        expect = sorted(p.stem for p in (DBT_DIR / "silver" / "models").glob("*.sql"))
        counts = do_reflect(
            Target.from_connection("fabric"),
            lakehouse_id=ctx["lakehouse_id"],
            host=os.environ.get("FABRIC_TDS_HOST", "fabric-emulator"),
            port=os.environ.get("FABRIC_TDS_PORT", "1433"),
            expect=expect,
            schema=os.environ.get("CONTOSO_SILVER_SCHEMA", "dbo"),
        )
        for table, rows in sorted(counts.items()):
            print(f"silver {table}: {rows} rows visible over TDS", flush=True)
        return counts

    bronze_done >> env >> silver >> reflect(ctx) >> gold


contoso_daily()
