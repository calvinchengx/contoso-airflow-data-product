{{ config(materialized='table') }}
-- Web orders, one row per LINE.
--
-- Bronze holds them nested -- the storefront thinks in baskets, so an order
-- carries its own `lines` array, and bronze kept it verbatim. Flattening is a
-- decision, and this is where it belongs: gold counts lines, not baskets.
select
    o.web_order_id,
    lower(trim(o.email)) as email,
    -- to_timestamp FIRST, then cast. `to_date` on an ISO-8601 string fails in
    -- Sail's parser -- `found T at 10:11`, the T of `2026-08-17T10:23:45` --
    -- because it parses the literal rather than the value. Measured: to_date on
    -- a bare `2026-08-17` works, to_timestamp on the full ISO string works, and
    -- the two-step is what the sibling's Spark silver does anyway
    -- (`F.to_timestamp` then `F.to_date`).
    cast(to_timestamp(o.placed_at) as date) as order_date,
    o.status,
    cast(line.line_no as int) as line_no,
    line.product_id,
    cast(line.quantity as int) as quantity,
    {{ money('line.unit_price') }} as unit_price,
    {{ money('cast(line.quantity as int) * line.unit_price') }} as amount,
    -- A LITERAL, because the storefront does not publish one. bronze_web_orders
    -- carries web_order_id, email, placed_at, status and the nested lines, and
    -- nothing in the lines names a currency either -- so this is an assertion
    -- about the vendor, not a passthrough.
    --
    -- 'USD' is not this model's invention: contoso-data-product's own silver
    -- states the same thing (`F.lit("USD").alias("currency")`), and gold --
    -- which both platforms share -- joins fx on it. Omitting it here is what
    -- made `fct_sales` fail with `Invalid column name 'currency'`, three
    -- layers from the decision that caused it.
    'USD' as currency
from {{ source('bronze', 'bronze_web_orders') }} as o
lateral view explode(o.lines) exploded as line
