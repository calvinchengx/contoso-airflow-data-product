{{ config(materialized='table') }}
-- The SECOND vendor's customers, conformed the same way and kept separate.
--
-- Not unioned with POS here: the two systems describe overlapping people
-- without either knowing the other exists, and resolving that is
-- silver_party's job. Merging them at this level would decide identity by
-- accident of source order.
select
    lower(trim(email)) as email,
    full_name,
    {{ conform_country('country') }} as country,
    signup_ts
from {{ source('bronze', 'bronze_web_customers') }}
