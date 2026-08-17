{# Country conforming, in ONE place.

   The vendors disagree about country spelling on purpose -- POS ships variants,
   Web ships its own, and gold reports everything against the reference
   hierarchy. Conforming per-model would let two models disagree, which is the
   bug this macro exists to make impossible. #}
{# WHITESPACE-CONTROLLED (`{%-`/`-%}`). Without it the loop emits a blank line
   per variant -- twelve of them inside a CASE, inside a CTE, inside dbt's own
   create-table wrapper. Sail's parser rejected the result with `found end of
   input expected something else, or '`'` while the same body ran fine when
   submitted directly, which made it look like a wrapper problem rather than a
   rendering one. Compact output is also simply easier to read in target/. #}
{% macro conform_country(col) -%}
  coalesce(
    case upper(trim({{ col }}))
      {%- for variant, canonical in var('country_variants').items() %}
      when '{{ variant }}' then '{{ canonical }}'
      {%- endfor %}
    end,
    upper(trim({{ col }}))
  )
{%- endmacro %}

{# Money and rate precision, named rather than repeated. The sibling exports
   MONEY = decimal(19,4) and RATE = decimal(19,6); a model picking its own
   precision is how two platforms stop agreeing on a total. #}
{% macro money(col) %}cast({{ col }} as decimal(19,4)){% endmacro %}
{% macro rate(col) %}cast({{ col }} as decimal(19,6)){% endmacro %}
