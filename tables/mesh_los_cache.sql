set client_min_messages = warning;

drop table if exists mesh_los_cache;
-- cache fresnel clearance and derived metrics for unordered h3 pairs with explicit mast/frequency parameters
create table mesh_los_cache (
    src_h3 h3index not null,
    dst_h3 h3index not null,
    mast_height_src double precision not null,
    mast_height_dst double precision not null,
    frequency_hz double precision not null,
    distance_m double precision not null,
    clearance double precision not null,
    d1_m double precision not null,
    d2_m double precision not null,
    path_loss_db double precision not null,
    computed_at timestamptz not null default now(),
    constraint mesh_los_cache_pkey primary key (src_h3, dst_h3, mast_height_src, mast_height_dst, frequency_hz)
);

create index if not exists mesh_los_cache_pkey_include
    on mesh_los_cache (
        src_h3,
        dst_h3,
        mast_height_src,
        mast_height_dst,
        frequency_hz
    )
    include (
        clearance,
        path_loss_db,
        distance_m,
        d1_m,
        d2_m,
        computed_at
    );
