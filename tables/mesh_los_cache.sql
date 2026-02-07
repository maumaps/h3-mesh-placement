set client_min_messages = warning;

-- Preserve cache across rebuilds; only create if missing.
create table if not exists mesh_los_cache (
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

do
$$
begin
    if not exists (
        select 1
        from information_schema.columns
        where table_schema = current_schema()
          and table_name = 'mesh_los_cache'
          and column_name = 'computed_at'
    ) then
        alter table mesh_los_cache add column computed_at timestamptz;
        update mesh_los_cache set computed_at = now() where computed_at is null;
        alter table mesh_los_cache alter column computed_at set default now();
        alter table mesh_los_cache alter column computed_at set not null;
    end if;
end;
$$ language plpgsql;

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
