{{ config(materialized='table') }}
-- FX rates CARRIED FORWARD to a dense daily calendar.
--
-- The vendor quotes on business days only -- 132 rows for 4 currencies over a
-- 47-day span, so weekends have no quote. A join on rate_date alone would drop
-- every weekend order silently, which is the failure this model exists to
-- prevent: the rate in force on Saturday is Friday's, and `rate_is_carried`
-- says so rather than leaving the reader to infer it.
with quotes as (
    select currency, rate_date, {{ rate('rate_to_usd') }} as rate_to_usd
    from {{ source('bronze', 'bronze_ref_fx_rates') }}
),
bounds as (
    select min(rate_date) as lo, max(rate_date) as hi from quotes
),
calendar as (
    select date_add(b.lo, cast(s.pos as int)) as rate_date
    from bounds b
    lateral view posexplode(split(space(datediff(b.hi, b.lo)), ' ')) s as pos, val
),
dense as (
    select c.rate_date, q.currency
    from calendar c cross join (select distinct currency from quotes) q
),
effective as (
    select d.rate_date, d.currency, max(p.rate_date) as quoted_on
    from dense d
    join quotes p on p.currency = d.currency and p.rate_date <= d.rate_date
    group by d.rate_date, d.currency
)
select
    e.rate_date,
    e.currency,
    q.rate_to_usd,
    e.quoted_on,
    (e.quoted_on <> e.rate_date) as rate_is_carried
from effective e
join quotes q on q.currency = e.currency and q.rate_date = e.quoted_on
