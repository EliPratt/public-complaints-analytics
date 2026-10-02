-- A complaint can't be forwarded to a company before the CFPB received it.
-- Returns offending rows; the test passes when this returns nothing.
select
    complaint_id,
    date_received,
    date_sent_to_company
from {{ ref('stg_complaints') }}
where date_sent_to_company < date_received
