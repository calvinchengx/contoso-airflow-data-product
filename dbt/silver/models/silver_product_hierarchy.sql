{{ config(materialized='table') }}
-- The reference vendor's hierarchy, as-is.
--
-- Deliberately thin: this vendor is the group data office's publisher, and
-- reshaping what it publishes would put this pipeline's opinion between the
-- definition and everything reported against it.
-- NAMED, not `select *`, because ONE of these columns has to change type.
--
-- The vendor publishes `list_price_usd` as a JSON number, so bronze holds it
-- as a double -- correct for bronze, which stores what arrived. Money is not a
-- binary float, and passing it through unchanged is what made gold's
-- `dim_product.list_price_usd` a float: gold unions the published catalogue
-- with the SKUs nobody published, that side is already
-- `cast(null as decimal(19,4))`, and a UNION takes the WIDER type -- so one
-- double on this side demoted the whole column back to floating point. Every
-- row held the right number and `money_is_never_stored_as_float` failed, which
-- is exactly the failure it was written for.
--
-- contoso-data-product's own silver casts the same column to MONEY. This is
-- that decision, in this dialect.
select
    product_id,
    product_name,
    category,
    department,
    segment,
    {{ money('list_price_usd') }} as list_price_usd
from {{ source('bronze', 'bronze_ref_product_hierarchy') }}
