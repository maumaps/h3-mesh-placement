set client_min_messages = warning;

truncate mesh_visibility_edges;

drop table if exists tmp_visibility_missing_elevation;
-- Temporary table capturing towers lacking GEBCO elevation coverage
create temporary table tmp_visibility_missing_elevation (
    tower_id integer primary key,
    h3 h3index not null
) on commit preserve rows;

insert into tmp_visibility_missing_elevation (tower_id, h3)
select
    t.tower_id,
    t.h3
from mesh_towers t
left join gebco_elevation_h3_r8 ge
  on ge.h3 = t.h3
where ge.h3 is null;

with eligible_towers as (
    select t.*
    from mesh_towers t
    where not exists (
        select 1
        from tmp_visibility_missing_elevation missing
        where missing.h3 = t.h3
    )
),
edge_pairs as (
    select
        t1.tower_id as source_id,
        t2.tower_id as target_id,
        t1.h3 as source_h3,
        t2.h3 as target_h3,
        ST_Distance(t1.centroid_geog, t2.centroid_geog) as distance_m,
        h3_los_between_cells(t1.h3, t2.h3) as is_visible,
        ST_MakeLine(t1.centroid_geog::geometry, t2.centroid_geog::geometry) as geom
    from eligible_towers t1
    join eligible_towers t2
      on t1.tower_id < t2.tower_id
)
insert into mesh_visibility_edges (
    source_id,
    target_id,
    source_h3,
    target_h3,
    distance_m,
    is_visible,
    geom
)
select * from edge_pairs;

do
$$
declare
    missing_list text;
    missing_count integer;
begin
    select
        string_agg(h3::text, ', ' order by h3),
        count(*)
    into missing_list, missing_count
    from tmp_visibility_missing_elevation;

    if missing_count > 0 then
        raise warning 'Skipped % tower(s) without GEBCO elevation samples: %',
            missing_count,
            missing_list;
    end if;
end;
$$;

drop table if exists tmp_visibility_missing_elevation;
