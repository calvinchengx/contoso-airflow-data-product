{{ config(materialized='table') }}
-- Web orders, one row per LINE.
--
-- Bronze holds them nested -- the storefront thinks in baskets, so an order
-- carries its own `lines` array, and bronze kept it verbatim. Flattening is a
-- decision, and this is where it belongs: gold counts lines, not baskets.
select
    o.web_order_id,
    lower(trim(o.email)) as email,
    cast(to_date(o.placed_at) as date) as order_date,
    o.status,
    cast(line.line_no as int) as line_no,
    line.product_id,
    cast(line.quantity as int) as quantity,
    {{ money('line.unit_price') }} as unit_price,
    {{ money('cast(line.quantity as int) * line.unit_price') }} as amount
from {{ source('bronze', 'bronze_web_orders') }} as o
lateral view explode(o.lines) exploded as line
