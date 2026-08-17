{{ config(materialized='table') }}
-- ONE PERSON, seen by two systems that do not know about each other.
--
-- POS and Web both hold customers, overlapping without either being aware. The
-- join key is EMAIL, because that is the only attribute both vendors carry that
-- identifies a human -- customer_id is POS's invention and means nothing to Web.
--
-- A POS customer with no email cannot be matched, and gets a key of its own
-- (`pos:<id>`) rather than being dropped or -- worse -- collapsed with every
-- other emailless customer into one party. Both of those would be silent.
--
-- FULL OUTER, so a person known only to Web survives too. An inner join would
-- quietly answer "who do both systems know" to a question about who exists.
with pos as (
    select
        customer_id,
        email,
        country,
        marketing_segment,
        loyalty_tier,
        case when email = '' then concat('pos:', cast(customer_id as string))
             else concat('email:', email) end as party_key
    from {{ ref('silver_customers') }}
),
web as (
    select
        email,
        country,
        concat('email:', email) as party_key
    from {{ ref('silver_web_customers') }}
)
select
    coalesce(p.party_key, w.party_key) as party_key,
    coalesce(p.email, w.email) as email,
    p.customer_id as pos_customer_id,
    p.customer_id is not null as in_pos,
    w.email is not null as in_web,
    coalesce(p.country, w.country) as country,
    p.marketing_segment,
    p.loyalty_tier
from pos p
full outer join web w on p.party_key = w.party_key
