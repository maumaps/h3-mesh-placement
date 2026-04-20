set client_min_messages = notice;

-- Pull user-tunable RF constants from the single pipeline config.
select value::double precision as mast_height
from mesh_pipeline_settings
where setting = 'mast_height_m'
\gset

select value::double precision as frequency
from mesh_pipeline_settings
where setting = 'frequency_hz'
\gset

\if :{?batch_limit}
\else
\set batch_limit 250000
\endif

-- Refuse to run a batch if the prepare stage has not materialized the missing
-- pair queue yet; this keeps resume explicit and debuggable.
do
$$
begin
    if to_regclass('mesh_route_missing_pairs') is null then
        raise exception 'mesh_route_missing_pairs is missing; run the fill_mesh_los_cache prepare stage first';
    end if;
end;
$$;

begin;

-- LOS cache batches are fully derivable from source data, so they do not need
-- synchronous commit latency on every configured batch transaction.
set local synchronous_commit = off;

-- Repeated LOS batch queries are short-lived and structurally identical, so
-- JIT compilation overhead is wasted here. Disable it inside each batch.
set local jit = off;

-- Claim one ordered slice from the queue inside this transaction so batch work
-- is atomic: if metric computation fails, the claimed rows roll back into the
-- queue instead of being lost. `skip locked` also makes the same script safe
-- to run from multiple workers later without a second queue design. The claim
-- feeds straight into metric computation and cache upsert, so there is no temp
-- claimed table or temp metrics table anymore.
with claimed as (
    select
        mp.ctid,
        mp.src_h3,
        mp.dst_h3
    from mesh_route_missing_pairs mp
    order by
        mp.building_endpoint_count desc,
        mp.disconnected_priority,
        mp.priority,
        mp.building_count desc,
        mp.src_h3,
        mp.dst_h3
    limit :batch_limit
    for update skip locked
),
deleted as (
    delete from mesh_route_missing_pairs mp
    using claimed c
    where mp.ctid = c.ctid
    returning
        c.src_h3,
        c.dst_h3
),
metrics as (
    select
        d.src_h3,
        d.dst_h3,
        (q.metrics).clearance as clearance,
        (q.metrics).path_loss_db as path_loss_db,
        (q.metrics).distance_m as distance_m,
        (q.metrics).d1_m as d1_m,
        (q.metrics).d2_m as d2_m
    from deleted d
    cross join lateral (
        select h3_visibility_clearance_compute_row(
            d.src_h3,
            d.dst_h3,
            :mast_height::double precision,
            :mast_height::double precision,
            :frequency::double precision
        ) as metrics
    ) q
    where q.metrics is not null
),
upserted as (
    insert into mesh_los_cache (
        src_h3,
        dst_h3,
        mast_height_src,
        mast_height_dst,
        frequency_hz,
        distance_m,
        clearance,
        d1_m,
        d2_m,
        path_loss_db,
        computed_at
    )
    select
        m.src_h3,
        m.dst_h3,
        :mast_height,
        :mast_height,
        :frequency,
        m.distance_m,
        m.clearance,
        m.d1_m,
        m.d2_m,
        m.path_loss_db,
        now()
    from metrics m
    on conflict on constraint mesh_los_cache_pkey do nothing
    returning 1
)
select
    (select count(*)::text from deleted) as claimed_pairs,
    count(*)::text as processed_pairs,
    case
        when exists (select 1 from mesh_route_missing_pairs limit 1) then 'on'
        else 'off'
    end as has_remaining_pairs,
    case
        when exists (select 1 from deleted) then 'on'
        else 'off'
    end as has_claimed_pairs,
    case
        when count(*) > 0 then 'on'
        else 'off'
    end as has_processed_pairs
from upserted
\gset

-- Treat an empty late-start batch as success so finite GNU parallel runs can
-- overprovision jobs from a queue snapshot and let extra jobs exit cleanly.
\if :has_claimed_pairs
\else
rollback;
\echo claimed no queue rows; batch finished cleanly
\quit 0
\endif

-- Fail loudly if the claim step found work but the metric computation returned
-- nothing, because otherwise the batch would silently discard claimed rows.
\if :has_processed_pairs
\else
rollback;
\echo mesh_route_missing_metrics produced no rows; batch limit or LOS staging is inconsistent
\quit 3
\endif

-- Show committed progress after this single transaction so a long run is easy
-- to monitor and safe to resume.
select
    :'claimed_pairs' as claimed_pairs,
    :'processed_pairs' as processed_pairs,
    :'has_remaining_pairs'::boolean as has_remaining_pairs;

commit;
