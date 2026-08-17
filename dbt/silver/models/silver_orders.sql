{{ config(materialized='table') }}
-- POS orders: latest event per order, then quarantine the unusable.
--
-- LATEST BY event_seq DESC, not by arrival. The fixture disagrees between
-- capture order and business order on purpose, so ordering by anything else
-- would quietly repair a disagreement the pipeline exists to face.
with latest as (
    select
        order_id, customer_id, product_id, order_date, channel, store_id,
        currency, discount_pct, tax_rate, shipping_fee, payment_method,
        is_gift, promo_code, quantity, unit_price, status, event_seq,
        row_number() over (partition by order_id order by event_seq desc) as _rn
    from {{ source('bronze', 'bronze_pos_orders') }}
)
select
    order_id, customer_id, product_id, order_date, channel, store_id,
    currency, discount_pct, tax_rate, shipping_fee, payment_method,
    is_gift, promo_code, quantity, status, event_seq,
    {{ money('unit_price') }} as unit_price,
    {{ money('quantity * unit_price') }} as amount
from latest
where _rn = 1
  -- The same predicate silver_quarantine_orders keeps, negated. Both models
  -- read it from here so they cannot drift apart and lose or double a row.
  and not (quantity <= 0 or unit_price is null)
