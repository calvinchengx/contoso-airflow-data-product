"""The Contoso daily pipeline: four vendors → landing → bronze.

This replaces `fabric-platform-notebook-pipelines/platform/pipeline.py`, which is a
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
from airflow.sdk import Asset, Metadata, dag, task
from cosmos import (DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig,
                    RenderConfig)
from cosmos.constants import TestBehavior

import os
import pathlib
import shutil

from contoso_product import gold_dir, silver_dir

WORKSPACE = "contoso-analytics"
LAKEHOUSE = "lake.Lakehouse"

# The product's dbt projects, resolved from THIS FILE rather than an absolute
# path: the bundle mounts the repo at /opt/product locally and clones it
# somewhere else in production, and a hardcoded path would be right in exactly
# one of those.
DBT_DIR = pathlib.Path(__file__).resolve().parent.parent / "dbt"
# WHERE THIS DEPLOYMENT PUT dbt -- resolved, never assumed. cosmos shells out,
# so it needs a real path, and the previous default named the local worker
# image's own layout (`/home/airflow/.local/bin/dbt`). That is a property of
# one deployment; on MWAA, Composer, Astronomer or a plain venv it is wrong,
# and wrong in the quiet way -- the DAG parses and every dbt task fails at
# execution with "no such file".
#
# `shutil.which` asks the environment the tasks actually run in. An explicit
# DBT_EXECUTABLE still wins, for a deployment that installs dbt somewhere off
# PATH. Falling back to the bare name rather than raising keeps DAG PARSING
# working where dbt is absent from the scheduler but present on the worker.
DBT_BIN = os.environ.get("DBT_EXECUTABLE") or shutil.which("dbt") or "dbt"

# WHERE GOLD'S dbt ARTEFACTS SURVIVE THE TASK THAT WROTE THEM.
#
# cosmos clones the project into a temp directory and runs there, so
# `target/run_results.json` -- the only record of WHICH tests an invocation
# actually evaluated -- is deleted with the clone. `scripts/snapshot.py` had no
# way to read it, so it named the contracts by globbing `gold/tests/*.sql`: a
# list of what the project CONTAINS, published as though it were what the run
# CHECKED. Those agree only when nothing went wrong, which is precisely the
# case the field exists for.
#
# dbt honours DBT_TARGET_PATH as an absolute path, so the artefacts land
# somewhere both the task and the snapshot can see. GOLD ONLY: silver's dbt
# writes its own run_results, and one shared directory would let silver's
# verdict be read as gold's -- the same confusion between two artefacts that
# the `args.which` assertion below exists to catch between two commands.
#
# The nine model tasks write here too, and race; that is harmless because
# `gold_test` runs after all of them (TestBehavior.AFTER_ALL) and is therefore
# the last writer, and because a snapshot that read a model task's artefact by
# mistake is refused rather than published -- see snapshot.py.
GOLD_TARGET = pathlib.Path(
    os.environ.get("CONTOSO_GOLD_TARGET", "/tmp/contoso-gold-target"))

# GOLD'S sources.yml DEMANDS THIS AT PARSE TIME. It reads
# `env_var('DBT_SILVER_DATABASE')`, and Cosmos renders by running `dbt ls`, so
# an unset value stops the DAG appearing at all. The real value is supplied per
# run; this only has to exist.
#
# IT WAS FIXED THERE AFTER ALL. This used to set LAKEHOUSE_ID, because gold's
# default was `env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID'))` and
# Jinja evaluates a default EAGERLY, so the Fabric-only fallback was required on
# every engine. The comment here argued that changing a shared project to suit
# one consumer's renderer was the wrong direction -- sound reasoning from a
# false premise, because the nesting was a defect in the project rather than a
# quirk of Cosmos. Core v0.6.0 removed it and moved the names, since Snowflake's
# dbt Projects refuse any key that is not UPPERCASE and DBT_-prefixed.
os.environ.setdefault("DBT_SILVER_DATABASE", "00000000-0000-0000-0000-000000000000")

# THE ASSETS THIS PRODUCT PUBLISHES, declared at PARSE time so they exist in
# Airflow's Assets view whether or not a run has happened yet, and so another
# DAG can schedule on one.
#
# WE DECLARE THESE OURSELVES rather than take what cosmos derives, for two
# reasons. Cosmos builds its URIs from OpenLineage, whose dbt processor knows
# `fabric` and not `fabricspark` -- so silver emits nothing at all
# (OpenLineage#4874 fixes that upstream). And the URIs it does build for gold
# embed `fabric-emulator:1433`, a deployment literal this repo has none of
# anywhere else; the same models against real Fabric would publish different
# asset names, so nothing downstream could depend on them.
#
# The names are read from the projects themselves, so a model added tomorrow
# publishes an asset tomorrow rather than being quietly absent.
SILVER_TABLES = sorted(p.stem for p in (silver_dir() / "models").glob("*.sql"))
GOLD_MODELS = sorted(p.stem for p in (gold_dir() / "models").glob("*.sql"))
SILVER_ASSETS = [Asset(f"contoso://silver/{t}") for t in SILVER_TABLES]
GOLD_ASSETS = [Asset(f"contoso://gold/{m}") for m in GOLD_MODELS]
# The BI-facing output. One asset, not one per measure: the model is the
# artifact a consumer opens, and a measure is a column of it.
SEMANTIC_ASSET = Asset("contoso://semantic/contoso-analytics")

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

    @task
    def fabric_env(ctx: dict) -> dict:
        """The dbt profiles' environment, minted per run.

        Both profiles are entirely `env_var()`-driven so production points them
        at real Fabric unedited -- which means SOMETHING has to supply those
        values, and a bearer cannot be baked into a rendered DAG. This task
        resolves them from the same connection every other task uses, and the
        dbt task groups read them back through templating.
        """
        from contoso_airflow.io.onelake import tables_root
        from contoso_airflow.target import Target
        from contoso_airflow.warehouse import endpoint

        target = Target.from_connection("fabric")
        tds_host, tds_port = endpoint()
        return {
            # SILVER's bearer: Livy, on the Fabric control plane.
            "DBT_ACCESS_TOKEN": target.fabric_token(),
            "DBT_FABRIC_ENDPOINT": f"{target.api_root}/v1",
            "DBT_WORKSPACE_ID": ctx["workspace_id"],
            "DBT_LAKEHOUSE_ID": ctx["lakehouse_id"],
            "DBT_LAKEHOUSE_NAME": ctx["lakehouse"].split(".", 1)[0],
            # THE SAME NAME, UNDER THE CORE'S SPELLING. Core's silver reads
            # `DBT_BRONZE_SCHEMA` because "lakehouse" is a Fabric word and the
            # core product names no engine -- a Databricks catalog and a
            # Snowflake schema answer the same question differently. Binding it
            # here is this platform's job: on Fabric a Lakehouse's Tables/ are
            # discovered into a schema named after the lakehouse.
            "DBT_BRONZE_SCHEMA": ctx["lakehouse"].split(".", 1)[0],
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
            # FROM THE CONNECTION, not from an env var with a local default.
            # The deployment that owns the Warehouse states where it is.
            "DBT_HOST": tds_host,
            "DBT_PORT": tds_port,
            # Gold BUILDS in the warehouse and READS the lakehouse endpoint;
            # two databases on the one TDS endpoint, joined by three-part name.
            "DBT_DATABASE": ctx["warehouse_id"],
            # DBT_-PREFIXED SINCE CORE v0.6.0. Snowflake's dbt Projects refuse
            # any env var key that is not UPPERCASE and DBT_-prefixed, so the
            # old names could not be supplied there at all -- gold ran on every
            # engine in this family except that one. LAKEHOUSE_ID is no longer
            # passed for dbt: it was read only because gold's default was
            # `env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID'))` and
            # Jinja evaluates a default EAGERLY. Fabric's own LAKEHOUSE_ID,
            # which notebookutils reads, is a different thing and untouched.
            "DBT_SILVER_DATABASE": ctx["lakehouse_id"],
            # `dbo`, not the Spark database name: the endpoint reflects
            # OneLake `Tables/` into dbo regardless of the catalog namespace
            # Spark wrote under. Measured, not assumed.
            "DBT_SILVER_SCHEMA": os.environ.get("DBT_SILVER_SCHEMA", "dbo"),
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
        "DBT_BRONZE_SCHEMA",
        "DBT_SILVER_LOCATION_ROOT", "DBT_HOST", "DBT_PORT",
        "DBT_DATABASE", "DBT_SILVER_DATABASE",
        "DBT_SILVER_SCHEMA")}

    # SILVER'S MODELS ARE NOT IN THIS REPO EITHER, as of core v0.2.0.
    # contoso-data-product ships them -- 8 models, a conform macro, a singular
    # test -- for the same reason it ships gold: this product carried the only
    # dbt silver in the family while the core carried a second one in PySpark,
    # and two definitions of one layer agree until they do not. This product
    # supplies the profile and the deployment bindings; the models come from
    # the package.
    #
    # `install_deps` is gone with them. Silver's one external package was
    # dbt_utils, for a single `accepted_range`; that is now a singular test in
    # the core project, so nothing here has to fetch a dbt package into an
    # installed wheel's own directory before it can build.
    silver = DbtTaskGroup(
        group_id="silver",
        project_config=ProjectConfig(dbt_project_path=silver_dir()),
        # TESTS AFTER ALL MODELS HERE TOO, and this is a CORRECTION.
        #
        # This group passed no `render_config` at all, which is not the same as
        # passing nothing: cosmos's default is `TestBehavior.AFTER_EACH`
        # (`cosmos/config.py`), and AFTER_EACH renders one test task per model
        # and evaluates NO SINGULAR TEST -- a singular test is attached to no
        # model, so a per-model task has nowhere to hang it. The DAG rendered
        # clean, ran clean, and asserted one guarantee fewer than it appeared
        # to.
        #
        # The comment on gold below used to say AFTER_EACH was "right for
        # silver, where every test belongs to the one model it follows". That
        # was TRUE WHEN IT WAS WRITTEN and stopped being true underneath it:
        # core v0.2.0 moved silver's `accepted_range` off dbt_utils and into
        # `silver_orders_never_holds_a_non_positive_quantity.sql`, a singular
        # test -- so silver acquired exactly the kind of test the default
        # cannot run, and nothing failed to say so.
        #
        # That is the shape worth naming: not a wrong decision, but a decision
        # whose PREMISE expired in another repository, silently, because the
        # default it relied on fails by omission rather than by error.
        # NO ASSET EMISSION FROM COSMOS, here or on gold below (G37). This DAG
        # declares its own target-neutral assets (`contoso://…`), emitted by
        # the tasks that VERIFY rows exist -- `reflect` and `publish_gold` --
        # so cosmos's per-model emission is redundant. It is also broken for
        # this graph: cosmos assigned three concurrent gold model tasks the
        # SAME outlet (`dbo/fct_orders`, claimed six times in one run's log),
        # they raced to register it, and Airflow flipped the losers to failed
        # AFTER their own dbt reported PASS -- a task recorded failed whose
        # payload says SUCCESS, one run in two. Cosmos's URIs are additionally
        # wrong for this repo on their own terms: they embed the emulator's
        # host and port, so the same models against real Fabric would publish
        # different asset names (the test asserting that predates this fix).
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_ALL,
                                   emit_datasets=False),
        profile_config=ProfileConfig(
            profile_name="contoso_silver", target_name="dev",
            profiles_yml_filepath=DBT_DIR / "silver" / "profiles.yml"),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
        operator_args={"env": ENV},
    )

    # GOLD'S MODELS ARE NOT IN THIS REPO. contoso-data-product ships them -- 9
    # models, 5 singular tests, 62 schema tests -- and exists so gold is not
    # copied per platform: "two fct_sales.sql files agree until the day someone
    # fixes a bug in one of them". This product supplies the profile and points
    # dbt at the installed package.
    gold = DbtTaskGroup(
        group_id="gold",
        project_config=ProjectConfig(dbt_project_path=gold_dir()),
        # TESTS AFTER ALL MODELS. Cosmos's default puts a test task immediately
        # after each model, which drops singular tests on the floor -- see the
        # silver group above, where relying on that default cost one contract.
        # Gold's suite includes
        # SINGULAR tests that span the star: `revenue_summary_loses_no_revenue`
        # compares a fact against its summary, `every_country_resolves_to_the
        # _dimension` joins a fact to a dimension. Cosmos attaches such a test
        # to ONE of the models it references, so it runs while the others do
        # not exist yet. Measured: dim_customer built, then its test task died
        # on `Invalid object name '…dbo.fct_daily_revenue'` -- a table three
        # models downstream. That reads like a broken test and is an ordering
        # artefact.
        #
        # The cost is honest: gold's 67 tests become one task rather than one
        # per model, so a single failure no longer isolates itself. That is the
        # right trade only because the alternative is tests that cannot pass.
        # emit_datasets=False: see the silver group -- G37 was measured on THIS
        # group's fct_orders outlet.
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_ALL,
                                   emit_datasets=False),
        profile_config=ProfileConfig(
            profile_name="contoso_gold", target_name="dev",
            profiles_yml_filepath=DBT_DIR / "gold" / "profiles.yml"),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
        # GOLD'S OWN ENV, not silver's. The extra entry is DBT_TARGET_PATH; see
        # GOLD_TARGET above for why it is here and why silver does not get it.
        operator_args={"env": {**ENV, "DBT_TARGET_PATH": str(GOLD_TARGET)}},
    )

    @task(outlets=SILVER_ASSETS)
    def reflect(ctx: dict):
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
        from contoso_airflow.warehouse import endpoint
        from contoso_airflow.warehouse import reflect as do_reflect

        host, port = endpoint()
        expect = sorted(p.stem for p in (silver_dir() / "models").glob("*.sql"))
        counts = do_reflect(
            Target.from_connection("fabric"),
            workspace_id=ctx["workspace_id"],
            lakehouse_id=ctx["lakehouse_id"],
            host=host,
            port=port,
            expect=expect,
            schema=os.environ.get("CONTOSO_SILVER_SCHEMA", "dbo"),
        )
        for table, rows in sorted(counts.items()):
            print(f"silver {table}: {rows} rows visible over TDS", flush=True)

        # ONE EVENT PER TABLE, CARRYING ITS COUNT. The event is emitted by the
        # step that just READ the table over TDS, so "asset produced" means the
        # rows are there and reachable -- not merely that a task exited 0.
        for table, rows in sorted(counts.items()):
            yield Metadata(Asset(f"contoso://silver/{table}"), {"rows": rows})
        yield counts

    @task(outlets=GOLD_ASSETS)
    def publish_gold(ctx: dict):
        """Count every gold model over TDS, and publish one asset each.

        SYMMETRICAL WITH `reflect`, and for the same reason: the task that
        emits an asset is the task that just READ it. dbt reporting `PASS` says
        the models built; a count says the star holds rows a consumer can
        select. Those came apart earlier in this project's life -- eight silver
        models built green while the lakehouse held none of them.

        Placed after the gold group rather than inside it because cosmos owns
        those tasks, and giving every model task the same outlets would have
        each of the nine claim all nine.
        """
        from contoso_airflow.target import Target
        from contoso_airflow.warehouse import connect, endpoint

        host, port = endpoint()
        conn = connect(Target.from_connection("fabric"), ctx["warehouse_id"], host, port)
        counts = {}
        for model in GOLD_MODELS:
            counts[model] = conn.cursor().execute(
                f"SELECT COUNT(*) FROM dbo.{model}").fetchone()[0]
            print(f"gold {model}: {counts[model]} rows", flush=True)

        empty = [m for m, n in counts.items() if n == 0]
        if empty:
            # A star with an empty fact is not a built star. Failing here keeps
            # the asset UNPUBLISHED rather than announcing something hollow.
            raise ValueError(f"gold models built but hold no rows: {empty}")

        for model, rows in sorted(counts.items()):
            yield Metadata(Asset(f"contoso://gold/{model}"), {"rows": rows})
        yield counts

    @task(outlets=[SEMANTIC_ASSET])
    def semantic_model(ctx: dict):
        """Publish the semantic model over gold, then hold it to gold.

        THE ONE ARTIFACT A BI CONSUMER OPENS. Everything upstream of here is
        reachable only over SQL; this is the product's outputs in the shape a
        report actually reads, and until now the family had none.

        AFTER `publish_gold`, not beside it: the model binds Direct Lake to
        tables that must already hold rows, and a model published over an
        empty star would answer 0 rather than fail.

        The contract reads gold TWICE -- three sums over TDS, the same three
        measures over DAX -- and requires exact agreement. Asserting the
        family's constants here instead would pass while the model was bound
        to the wrong warehouse; comparing the two paths of the SAME run cannot.
        """
        from contoso_airflow import semantic
        from contoso_airflow.target import Target
        from contoso_airflow.warehouse import connect, endpoint

        target = Target.from_connection("fabric")
        dataset = semantic.publish(target, ctx)

        # Gold over SQL: the same three columns snapshot.py sums, read from the
        # warehouse this run just wrote.
        host, port = endpoint()
        conn = connect(target, ctx["warehouse_id"], host, port)
        row = conn.cursor().execute(
            "SELECT coalesce(sum(revenue_usd),0), "
            "coalesce(sum(cancelled_revenue_usd),0), "
            "coalesce(sum(sale_lines),0) FROM dbo.fct_revenue_summary").fetchone()
        from contoso_product import semantic as core
        expected = core.expected_measures({
            "revenue_usd": row[0],
            "cancelled_revenue_usd": row[1],
            "sale_lines": row[2],
        })

        measured = semantic.evaluate(target, ctx["workspace_id"], dataset)
        verdict = semantic.semantic_verdict(measured, expected)
        for name, value in verdict.items():
            print(f"semantic {name}: {value} (DAX == SQL)", flush=True)

        yield Metadata(SEMANTIC_ASSET,
                       {"dataset": dataset, **verdict})
        yield {"dataset": dataset, "measures": verdict}

    bronze_done >> env >> silver >> reflect(ctx) >> gold >> publish_gold(ctx) >> semantic_model(ctx)


contoso_daily()
