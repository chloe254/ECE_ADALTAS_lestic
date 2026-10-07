{{ config(severity='warn') }}

-- The 3 first digits of the zip code must belong to a range of the state
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('state_zip_ranges') }} r
    where r.state = u.state
      and try_cast(left(u.zip_code, 3) as integer) between r.zip_prefix_min and r.zip_prefix_max
)