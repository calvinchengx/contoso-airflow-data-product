{{ config(materialized='table') }}
-- COLUMNS NAMED EXPLICITLY, and this is a deliberate divergence from the
-- sibling's Spark silver, which carries every bronze column through.
--
-- Neither exclusion mechanism is available here. Sail has no SELECT-star-EXCEPT
-- (it answers "found _rn ... expected query"), and dbt_utils.star cannot
-- introspect columns through this adapter -- it emits its own placeholder
-- comment and leaves a bare comma. So the choice is a hand-written 100-column
-- list that rots the first time a vendor adds a field, or naming what silver
-- actually owes gold.
--
-- NOTE ON THIS COMMENT ITSELF: no backticks and no block-comment markers below.
-- A model comment is submitted to the engine as part of the statement, and an
-- inner slash-star inside a dash-dash comment opens a block comment Sail never
-- closes -- it swallows the query and reports "found end of input expected
-- something else, or backtick". Prose in a model is executable context, not
-- documentation around it.
--
-- Naming them is also the better contract: these six are what gold reads, and a
-- silver that passes through a hundred unexamined columns is not conformed, it
-- is forwarded. The cost is that `customer_columns` no longer matches the
-- sibling's metric -- a real difference, recorded rather than hidden.
--
-- THE DUPLICATES ARE DELIBERATE. The POS vendor ships a 2% duplicate ratio, so
-- bronze holds 102,000 rows for 100,000 customers -- bronze's job is to be what
-- arrived. Silver is where that is resolved, and the sibling's own constant
-- says so: EXPECTED_SILVER_CUSTOMERS = 100,000.
--
-- row_number over customer_id, matching the sibling's Window exactly. A
-- distinct() would collapse rows differing in any column and keep both;
-- picking one row per key is a decision, and this is it.
with ranked as (
    select
        customer_id,
        name,
        email,
        country,
        marketing_segment,
        loyalty_tier,
        row_number() over (partition by customer_id order by customer_id) as _rn
    from {{ source('bronze', 'bronze_pos_customers') }}
)
select
    customer_id,
    name,
    -- OVERWRITTEN IN PLACE. Gold reads `email` and `country`; a suffixed column
    -- would leave every gold model unresolved, so silver's column names are its
    -- contract with gold.
    lower(trim(coalesce(email, ''))) as email,
    {{ conform_country('country') }} as country,
    marketing_segment,
    loyalty_tier
from ranked
where _rn = 1
