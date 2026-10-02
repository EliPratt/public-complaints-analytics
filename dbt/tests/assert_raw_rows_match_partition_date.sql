-- Every raw row must belong to the day its partition was extracted for.
-- This is the check extract.py's count reconciliation can't make on its own:
-- when the API date filter is wrong, the count and the export agree with each
-- other, and the staging dedupe hides the duplicates that result.
-- Returns offending partitions; the test passes when this returns nothing.
select
    _partition_date,
    count(*) as off_day_rows
from {{ source('raw', 'complaints') }}
where try_cast(left(date_received, 10) as date) is distinct from _partition_date
group by _partition_date
