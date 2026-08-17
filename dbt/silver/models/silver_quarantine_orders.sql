{{ config(materialized='table') }}
-- What silver_orders REFUSED, kept rather than dropped.
--
-- A quarantine that is discarded is indistinguishable from data that never
-- arrived. Keeping it is what lets "how many did we reject, and why" be
-- answered without re-reading bronze.
with latest as (
    select
        order_id, customer_id, product_id, order_date, quantity, unit_price,
        status, event_seq,
        row_number() over (partition by order_id order by event_seq desc) as _rn
    from {{ source('bronze', 'bronze_pos_orders') }}
)
select
    order_id, customer_id, product_id, order_date, quantity, unit_price,
    status, event_seq,
    case when quantity <= 0 then 'non_positive_quantity'
         when unit_price is null then 'missing_unit_price'
    end as quarantine_reason
from latest
where _rn = 1
  and (quantity <= 0 or unit_price is null)
