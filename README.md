# contoso-data-product-fabric-airflow3

[![CI](https://github.com/calvinchengx/contoso-data-product-fabric-airflow3/actions/workflows/ci.yml/badge.svg)](https://github.com/calvinchengx/contoso-data-product-fabric-airflow3/actions/workflows/ci.yml)
[![Airflow 3](https://img.shields.io/badge/Apache_Airflow-3-017CEE?logo=apacheairflow&logoColor=white)](dags/contoso_daily.py)
[![dlt](https://img.shields.io/badge/dlt-1.30.0-FF6B35)](pyproject.toml)
[![dbt](https://img.shields.io/badge/dbt-fabricspark_%2B_fabric-FF694B?logo=dbt&logoColor=white)](dbt/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

The Contoso **data product** for Apache Airflow 3: dlt sources that land four
vendors into a Fabric Lakehouse, dbt models that take bronze to gold, and the
ODCS contracts that gate every hop.

It is a **product, not a platform.** There is no Airflow here and no
fabric-emulator here — no compose file, no Dockerfile, no image pin. A platform
is `make`'d and pointed at this repo:

```sh
make up     # -> ../fabric-platform-airflow3, with this directory as the product
make run    # trigger contoso_daily
```

## What the product contains

The SQL is not here. It lives in the core so seven leaves cannot drift into
seven versions of it, and that costs you a click, so this list exists to give
it back. `make show-product` copies the same files into `product/` where you
can open them; the block below is generated from the pinned package and a test
fails when it falls behind.

<!-- BEGIN product inventory: python -m contoso_product.show --markdown -->

The product is [`contoso-data-product`](https://github.com/calvinchengx/contoso-data-product/tree/v0.5.1) at **v0.5.1**, the version this repository pins. It is not vendored here: these files live there and are staged locally by `make show-product`.

**silver**: 8 models, 1 singular test

- [`silver_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_customers.sql)
- [`silver_fx_daily`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_fx_daily.sql)
- [`silver_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_orders.sql)
- [`silver_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_party.sql)
- [`silver_product_hierarchy`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_product_hierarchy.sql)
- [`silver_quarantine_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_quarantine_orders.sql)
- [`silver_web_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_web_customers.sql)
- [`silver_web_order_lines`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/models/silver_web_order_lines.sql)

Assertions over silver, each failing the build on its own:

- [`silver_orders_never_holds_a_non_positive_quantity`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/silver/tests/silver_orders_never_holds_a_non_positive_quantity.sql)

**gold**: 9 models, 5 singular tests

- [`dim_country`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_country.sql)
- [`dim_customer`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_customer.sql)
- [`dim_date`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_date.sql)
- [`dim_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_party.sql)
- [`dim_product`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/dim_product.sql)
- [`fct_daily_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/fct_daily_revenue.sql)
- [`fct_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/fct_orders.sql)
- [`fct_revenue_summary`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/fct_revenue_summary.sql)
- [`fct_sales`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/models/fct_sales.sql)

Assertions over gold, each failing the build on its own:

- [`both_selling_systems_reach_the_pack`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/both_selling_systems_reach_the_pack.sql)
- [`every_country_resolves_to_the_dimension`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/every_country_resolves_to_the_dimension.sql)
- [`fiscal_year_is_not_the_calendar_year`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/fiscal_year_is_not_the_calendar_year.sql)
- [`money_is_never_stored_as_float`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/money_is_never_stored_as_float.sql)
- [`revenue_summary_loses_no_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.1/src/contoso_product/gold/tests/revenue_summary_loses_no_revenue.sql)

<!-- END product inventory -->

## How Airflow picks this up

Three different mechanisms, and conflating them is where setups go wrong:

| what | how | where it is configured |
|---|---|---|
| DAG **files** | a DAG bundle rooted at `dags/` | the platform (`LocalDagBundle` locally, `GitDagBundle` at a tag in production) |
| **dependencies** | `uv pip install -r pyproject.toml` into the worker | the platform's 3-line Dockerfile |
| **task code** | `from airflow.sdk import …` — the Airflow 3 Task SDK | here |

The bundle delivers files and installs nothing. That is why `pyproject.toml`
lists what the worker must import, and why there is no `requirements.txt`.

## How it reaches Fabric

`conn_id="fabric"`, and nothing else. No host, no tenant, no grant type, and no
branch on emulator-versus-real. The platform provisions that connection against
fabric-emulator locally and against real Fabric in production; this repo cannot
tell which answered, which is what makes "the same DAGs, against real Fabric" a
property rather than a hope.

## The four sources, and why they are four different problems

| vendor | transport | format | shape |
|---|---|---|---|
| Contoso POS | HTTP / OpenAPI | delimited text + JSON Lines | **paged** — parts land separately |
| Contoso Web | HTTP / OpenAPI | JSON arrays | **nested** — orders carry their `lines` |
| Contoso Reference | HTTP / OpenAPI | **Parquet** | master data, not paged |
| Contoso ERP | Debezium → Redpanda | **CDC change stream** | not HTTP at all |

Three of them are operational systems; Reference is the group data office's
publisher, which is why it is a vendor rather than a table maintained here.

## Landing is verbatim, and dlt does not get to reshape it

    Landed VERBATIM — no parsing, no reshaping. Bronze's job is to be the bytes
    as they arrived, so that a question about the source can be answered
    without going back to the vendor.

dlt owns the hard part — paging, auth, retry, incremental state — and its
resources yield a **manifest** of what was fetched and where it went. The
vendor's bytes go to OneLake unchanged and never pass through dlt's normaliser.

**dlt's own Azure destinations are deliberately not used.** Measured against the
emulator: dlt's credential model carries account-key, SAS and service-principal
shapes and has **no bearer field**, so a raw `storage_options` dict is
unrecognised and dlt falls through to `DefaultAzureCredential`. Run without
network isolation, that authenticated against **real Microsoft endpoints** using
the developer's own `az login` state. So writes go through delta-rs and stdlib
HTTP — the two paths this emulator is already witnessed against, neither of
which has a credential chain to fall through.

## Not the emulator's Airflow

fabric-emulator has an `ApacheAirflowJob` item and an Airflow sidecar. That is
the **opposite** direction: Fabric as the control plane, driving Airflow. Here
Airflow is the control plane and Fabric is a target. This repo never touches
`ApacheAirflowJob` and the platform never sets `FABRIC_AIRFLOW_URL`.

Apache-2.0.
