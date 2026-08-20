"""Product tests: no platform, no emulator, no credentials.

The runtimes are exercised by the witness; these are about the product's own
decisions — the ones that would be wrong before anything is stood up.
"""
import json
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
    # READ FROM THE INSTALLED CORE, not from this repo. As of core v0.2.0 the
    # silver project lives in contoso-data-product and this product supplies
    # only the profile and the bindings -- so the thing this test guards is
    # whether the CORE still declares a location_root, and whether this
    # platform still supplies the value it reads.
    from contoso_product import silver_dir

    project = (silver_dir() / "dbt_project.yml").read_text(encoding="utf-8")
    assert "+location_root:" in project
    assert "DBT_SILVER_LOCATION_ROOT" in project
    dag = (pathlib.Path(__file__).resolve().parents[1]
           / "dags" / "contoso_daily.py").read_text(encoding="utf-8")
    assert "DBT_SILVER_LOCATION_ROOT" in dag, (
        "core reads DBT_SILVER_LOCATION_ROOT and this platform no longer "
        "supplies it -- silver would land where the endpoint cannot see it")


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


def test_the_product_names_no_deployment_it_could_be_pointed_at():
    """No hostname, port or filesystem path belonging to one deployment.

    THE RULE IS THE DELIVERABLE, not a preference: this repo has to deploy to a
    managed Airflow against real Fabric unedited. Every literal here was found
    in it once. Each was a DEFAULT, which is what made them dangerous -- a
    deployment that failed to set the variable did not fail, it silently aimed
    at a container name that exists on one machine in the world.

    Comments are exempt: they explain where a value comes from, and one that
    names the local stack while the code takes it from a connection is
    documentation, not coupling.
    """
    root = pathlib.Path(__file__).resolve().parents[1]
    banned = ["fabric-emulator", "entra-emulator", "spark-agent", ":9443",
              "/home/airflow", "localhost", "127.0.0.1"]

    def docstring_lines(source: str) -> set[int]:
        """Line numbers belonging to a docstring, and only a docstring.

        Comments and docstrings are prose; a string literal anywhere else is a
        VALUE, and a value is exactly what this test exists to catch -- so the
        exemption is found structurally rather than by skipping every quoted
        string, which would exempt the defaults that caused the problem.
        """
        import ast

        exempt = set()
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                     ast.AsyncFunctionDef)):
                continue
            body = getattr(node, "body", None)
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                exempt.update(range(body[0].lineno, body[0].end_lineno + 1))
        return exempt

    offenders = []
    for path in [*root.glob("dags/*.py"), *root.glob("src/**/*.py"),
                 *root.glob("dbt/**/*.yml")]:
        source = path.read_text()
        exempt = docstring_lines(source) if path.suffix == ".py" else set()
        for n, line in enumerate(source.splitlines(), 1):
            if n in exempt:
                continue
            code = line.split("#", 1)[0]
            for literal in banned:
                if literal in code:
                    offenders.append(f"{path.relative_to(root)}:{n} {literal}")
    assert not offenders, (
        "these name one deployment and must come from a Connection or the "
        f"environment instead: {offenders}")


def test_every_silver_and_gold_model_publishes_an_asset():
    """The Assets view is a contract, so it must not quietly go stale.

    Cosmos derives its own assets from OpenLineage, which knows `fabric` and
    not `fabricspark` -- so silver published nothing at all, and gold published
    URIs embedding `fabric-emulator:1433`. These are declared from the two
    projects' own model directories instead, so a model added tomorrow
    publishes an asset tomorrow.
    """
    pytest.importorskip("airflow.sdk")
    pytest.importorskip("cosmos")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
    import contoso_daily

    from contoso_product import silver_dir

    silver = {p.stem for p in (silver_dir() / "models").glob("*.sql")}
    assert silver, "no silver models found -- the scan proved nothing"
    assert {a.uri for a in contoso_daily.SILVER_ASSETS} == {
        f"contoso://silver/{t}" for t in silver}

    assert contoso_daily.GOLD_MODELS, "no gold models found -- the scan proved nothing"
    assert {a.uri for a in contoso_daily.GOLD_ASSETS} == {
        f"contoso://gold/{m}" for m in contoso_daily.GOLD_MODELS}

    # Target-neutral, like every other name this repo publishes. Cosmos's own
    # URIs carry the host and port, so the same models against real Fabric
    # would publish different asset names and nothing could depend on them.
    for asset in contoso_daily.SILVER_ASSETS + contoso_daily.GOLD_ASSETS:
        assert asset.uri.startswith("contoso://"), asset.uri
        assert "fabric-emulator" not in asset.uri and ":1433" not in asset.uri


def test_the_assets_are_emitted_by_the_tasks_that_verify_them():
    # An asset event should mean "the rows are there", not "a task exited 0".
    # reflect reads silver over TDS; publish_gold counts the star.
    pytest.importorskip("airflow.sdk")
    pytest.importorskip("cosmos")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
    import contoso_daily

    dag = contoso_daily.contoso_daily()
    reflect_uris = {a.uri for a in dag.get_task("reflect").outlets}
    gold_uris = {a.uri for a in dag.get_task("publish_gold").outlets}
    assert reflect_uris == {a.uri for a in contoso_daily.SILVER_ASSETS}
    assert gold_uris == {a.uri for a in contoso_daily.GOLD_ASSETS}
    # publish_gold runs AFTER the gold group, or it would count tables that do
    # not exist yet.
    assert any(t.startswith("gold.") for t in dag.get_task("publish_gold").upstream_task_ids)


def test_every_singular_test_in_both_projects_has_a_task_to_run_it(monkeypatch):
    """A guarantee with no task is not a guarantee, and it fails silently.

    THIS TEST EXISTS BECAUSE IT ALREADY HAPPENED. The silver group passed no
    `render_config`, so cosmos's default `TestBehavior.AFTER_EACH` applied: one
    test task per model, and NOTHING for a singular test, which belongs to no
    model. Silver ships 13 data tests; 12 ran. The thirteenth --
    `silver_orders_never_holds_a_non_positive_quantity`, the check that the
    quarantine split does not leak -- was rendered by no task at all, through a
    green run of this DAG that the plan recorded as evidence.

    Measured rather than reasoned about, in both directions: before the fix the
    rendered graph held `silver.silver_customers.test` and four siblings and no
    whole-suite task; after it, one `silver.silver_test`.

    So the check is on the SHAPE THAT MAKES SINGULAR TESTS POSSIBLE -- a
    whole-suite task per group -- rather than on the config value that produces
    it today. A future cosmos that renders singular tests some other way should
    pass this; a group that silently reverts to per-model tests should not.
    """
    pytest.importorskip("airflow.sdk")
    pytest.importorskip("cosmos")
    monkeypatch.setenv("AIRFLOW__COSMOS__ENABLE_CACHE", "False")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
    import contoso_daily

    from contoso_product import gold_dir, silver_dir

    dag = contoso_daily.contoso_daily()
    ids = {t.task_id for t in dag.tasks}
    for group, project in (("silver", silver_dir()), ("gold", gold_dir())):
        singular = sorted(p.stem for p in (project / "tests").glob("*.sql"))
        if not singular:
            continue
        assert f"{group}.{group}_test" in ids, (
            f"{group} ships {len(singular)} singular test(s) -- {singular} -- and "
            f"the rendered graph has no whole-suite `{group}.{group}_test` task "
            f"to run them. Per-model test tasks cannot: a singular test is "
            f"attached to no model. Tasks: {sorted(t for t in ids if t.startswith(group))}"
        )
        assert not any(t.endswith(".test") for t in ids if t.startswith(f"{group}.")), (
            f"{group} renders per-model test tasks alongside the suite task, "
            f"which is the shape that dropped {singular} before"
        )


def _run_results(which, results):
    return json.dumps({"args": {"which": which}, "results": results})


def test_the_snapshot_names_the_contracts_the_run_evaluated(tmp_path):
    """Not the ones the directory happens to contain.

    THIS IS THE DEFECT THIS CELL PUBLISHED FOR ITS WHOLE LIFE. `contracts` was
    `glob('gold/tests/*.sql')` -- what the shared project CONTAINS -- written
    into a snapshot that `compare_products` reads as what this runtime CHECKED.
    The two agree only when nothing went wrong, which is the single case the
    field exists for: a cell that ran no contract at all would still name five.
    """
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
    import snapshot

    rr = tmp_path / "run_results.json"
    rr.write_text(_run_results("test", [
        {"unique_id": "test.contoso_gold.revenue_summary_loses_no_revenue",
         "status": "pass"},
        # A generic test carries a trailing hash; the name is still segment 3.
        {"unique_id": "test.contoso_gold.not_null_fct_sales_amount.9a1b2c",
         "status": "pass"},
    ]), encoding="utf-8")

    contracts, failures = snapshot.verdicts(
        rr, ["revenue_summary_loses_no_revenue"])
    assert contracts == ["revenue_summary_loses_no_revenue"]
    assert failures == []


def test_the_snapshot_refuses_a_contract_the_run_never_evaluated(tmp_path):
    """A name on disk that no test produced must fail, not be published."""
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
    import snapshot

    rr = tmp_path / "run_results.json"
    rr.write_text(_run_results("test", [
        {"unique_id": "test.contoso_gold.revenue_summary_loses_no_revenue",
         "status": "pass"}]), encoding="utf-8")

    with pytest.raises(SystemExit, match="money_is_never_stored_as_float"):
        snapshot.verdicts(rr, ["money_is_never_stored_as_float",
                               "revenue_summary_loses_no_revenue"])


def test_the_snapshot_refuses_another_command_s_artefact(tmp_path):
    """`dbt run` overwrites run_results.json, and reports zero failures.

    Believed, that publishes "no contract failures" for a run whose contracts
    never executed. Here the nine gold model tasks write to the same directory,
    so this is the ordinary case rather than a corner one.
    """
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
    import snapshot

    rr = tmp_path / "run_results.json"
    rr.write_text(_run_results("run", [
        {"unique_id": "model.contoso_gold.fct_sales", "status": "success"}]),
        encoding="utf-8")

    with pytest.raises(SystemExit, match="dbt run"):
        snapshot.verdicts(rr, ["revenue_summary_loses_no_revenue"])


def test_a_failing_contract_is_recorded_rather_than_hidden(tmp_path):
    """Recording a measurement and asserting a pass are different acts.

    A cell that refuses to publish anything when a contract fails removes
    itself from the family comparison, and the family loses its evidence for
    the defect. So the failure travels WITH the numbers; `compare_products`
    is what makes it fatal.
    """
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
    import snapshot

    rr = tmp_path / "run_results.json"
    rr.write_text(_run_results("test", [
        {"unique_id": "test.contoso_gold.money_is_never_stored_as_float",
         "status": "fail", "failures": 3, "message": "got 3 float columns"}]),
        encoding="utf-8")

    contracts, failures = snapshot.verdicts(rr, ["money_is_never_stored_as_float"])
    assert contracts == ["money_is_never_stored_as_float"]
    assert failures == [{"contract": "money_is_never_stored_as_float",
                         "status": "fail", "failures": 3,
                         "detail": "got 3 float columns"}]


def test_gold_writes_its_dbt_artefacts_where_the_snapshot_reads_them(monkeypatch):
    """The two halves of the fix must agree, and nothing else holds them so.

    The DAG tells gold's dbt where to write `run_results.json`; the snapshot
    reads it from the same place. They are joined by one env var and a matching
    default, in two files -- exactly the shape that drifts.
    """
    pytest.importorskip("airflow.sdk")
    pytest.importorskip("cosmos")
    monkeypatch.setenv("AIRFLOW__COSMOS__ENABLE_CACHE", "False")
    root = pathlib.Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "dags"))
    sys.path.insert(0, str(root / "scripts"))
    import contoso_daily
    import snapshot as snap

    assert "CONTOSO_GOLD_TARGET" in (root / "scripts" / "snapshot.py").read_text(
        encoding="utf-8"), "the snapshot no longer reads the DAG's target path"
    assert snap  # imported for the same reason the DAG is: it must parse

    dag = contoso_daily.contoso_daily()
    gold = dag.get_task("gold.gold_test")
    assert gold.env["DBT_TARGET_PATH"] == str(contoso_daily.GOLD_TARGET)
    # SILVER MUST NOT SHARE IT. One directory for both projects would let
    # silver's `dbt test` artefact be read as gold's verdict -- the same
    # confusion between two artefacts that `args.which` catches between two
    # commands.
    silver = dag.get_task("silver.silver_test")
    assert "DBT_TARGET_PATH" not in silver.env


def test_no_dbt_task_emits_an_asset_cosmos_invented(monkeypatch):
    """Every asset this DAG publishes is declared by name at the top of the
    file and emitted by a task that VERIFIED the rows -- reflect and
    publish_gold. Cosmos must emit none.

    THIS TEST EXISTS BECAUSE IT ALREADY HAPPENED (G37). With emission left on,
    cosmos gave three concurrent gold model tasks the SAME outlet --
    `dbo/fct_orders`, claimed six times in one run's log -- and they raced to
    register it. Airflow flipped the losers to failed AFTER their own dbt
    reported PASS=1 ERROR=0, so a task that did its work was recorded failed,
    one run in two, on a graph whose whole purpose is to be compared against
    another graph. Nothing could see it until `make verify` ran the pipeline
    twice.

    IT ASSERTS `emit_datasets`, NOT `outlets`, AND THE FIRST VERSION OF THIS
    TEST HAD THAT WRONG. Cosmos's own parameter doc says emission happens
    "during task execution": a rendered task carries NO outlets either way, so
    `assert not task.outlets` passes identically with emission on and off and
    could never catch this. Measured rather than reasoned about -- the pre-fix
    DAG renders 0 outlets, exactly as the fixed one does.

    What made the earlier version look like it worked is worse than a false
    pass. With the flag removed it DID fail -- but with
    `sqlite3.OperationalError: no such table: variable`, because emission-on
    sends cosmos through its Variable cache and the test omitted the
    `AIRFLOW__COSMOS__ENABLE_CACHE` monkeypatch its siblings set. A crash in
    the right direction reads exactly like a working guard. The monkeypatch
    below removes the crash, so what remains is an assertion or nothing.

    `emit_datasets` IS observable on the rendered operator, and it is the value
    the task will actually use at run time. Checked in both directions: every
    task False as written, and `False, True` with the knob removed.
    """
    pytest.importorskip("airflow.sdk")
    pytest.importorskip("cosmos")
    # Hermetic, like every other render in this file: no metadata database.
    monkeypatch.setenv("AIRFLOW__COSMOS__ENABLE_CACHE", "False")
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
    import contoso_daily

    dag = contoso_daily.contoso_daily()
    dbt_tasks = [t for t in dag.tasks
                 if t.task_id.startswith(("silver.", "gold."))]
    assert dbt_tasks, "no dbt tasks rendered -- the scan proved nothing"
    emitting = {t.task_id for t in dbt_tasks if getattr(t, "emit_datasets", True)}
    assert not emitting, (
        f"cosmos will emit assets for these tasks at run time; the G37 race is "
        f"live for them: {sorted(emitting)}")
