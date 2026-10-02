{#
  One row per complaint, typed and cleaned.

  - Casts use try_cast so a bad value becomes NULL instead of failing the
    whole build; the not_null tests in _stg_models.yml then catch it.
  - Yes/No fields become booleans. 'N/A' and blanks become NULL.
  - Empty strings become NULL so "missing" means one thing downstream.
  - Dedupes to the most recently extracted row per complaint_id, in case a
    complaint ever appears in more than one daily partition.
  - The CFPB API export omits the narrative, consumer consent, and consumer
    disputed fields that appear in the full CSV download, so they are not
    modeled here. If the API starts sending them, load.py adds the columns
    to raw.complaints automatically; add them here deliberately.
#}

with source as (

    select * from {{ source('raw', 'complaints') }}

),

cleaned as (

    select
        try_cast(complaint_id as bigint)                    as complaint_id,

        -- dates
        try_cast(date_received as date)                     as date_received,
        try_cast(date_sent_to_company as date)              as date_sent_to_company,

        -- what the complaint is about
        nullif(trim(product), '')                           as product,
        nullif(trim(sub_product), '')                       as sub_product,
        nullif(trim(issue), '')                             as issue,
        nullif(trim(sub_issue), '')                         as sub_issue,

        -- who it is about
        nullif(trim(company), '')                           as company_name,
        nullif(trim(company_public_response), '')           as company_public_response,
        nullif(trim(company_response_to_consumer), '')      as company_response,

        -- consumer details
        nullif(trim(state), '')                             as state,
        nullif(trim(zip_code), '')                          as zip_code,
        nullif(trim(tags), '')                              as tags,
        nullif(trim(submitted_via), '')                     as submitted_via,

        -- Yes/No flags
        case lower(trim(timely_response))
            when 'yes' then true
            when 'no'  then false
        end                                                 as is_timely_response,

        -- lineage
        _partition_date,
        _extracted_at,
        _loaded_at

    from source

),

deduped as (

    select *
    from cleaned
    qualify row_number() over (
        partition by complaint_id
        order by _extracted_at desc, _loaded_at desc
    ) = 1

)

select
    *,
    date_sent_to_company - date_received                    as days_to_company
from deduped
