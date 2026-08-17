{{ config(materialized='table') }}
-- The reference vendor's hierarchy, as-is.
--
-- Deliberately thin: this vendor is the group data office's publisher, and
-- reshaping what it publishes would put this pipeline's opinion between the
-- definition and everything reported against it.
select * from {{ source('bronze', 'bronze_ref_product_hierarchy') }}
