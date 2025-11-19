set client_min_messages = warning;

drop table if exists mesh_los_cache;
-- cache line-of-sight results for unordered h3 pairs to avoid recomputation
create table mesh_los_cache (
    src_h3 h3index not null,
    dst_h3 h3index not null,
    distance_m double precision not null,
    is_visible boolean not null,
    checked_at timestamptz not null default now(),
    constraint mesh_los_cache_pkey primary key (src_h3, dst_h3)
);

create index if not exists mesh_los_cache_visible_idx on mesh_los_cache (is_visible);
