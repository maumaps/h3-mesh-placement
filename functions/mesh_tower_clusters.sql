set client_min_messages = warning;

drop function if exists mesh_tower_clusters();
-- create helper returning connected components for the current mesh_towers graph
create or replace function mesh_tower_clusters()
    returns table (tower_id integer, cluster_id integer)
    language sql
    stable
    parallel restricted
as
$$
with recursive base_pairs as (
    select
        t1.tower_id as source_id,
        t2.tower_id as target_id,
        t1.h3 as source_h3,
        t2.h3 as target_h3
    from mesh_towers t1
    join mesh_towers t2
        on t1.tower_id < t2.tower_id
    where ST_DWithin(t1.centroid_geog, t2.centroid_geog, 70000)
),
visible_edges as (
    select
        b.source_id,
        b.target_id
    from base_pairs b
    where h3_los_between_cells(b.source_h3, b.target_h3)
),
undirected_edges as (
    select source_id, target_id from visible_edges
    union all
    select target_id as source_id, source_id as target_id from visible_edges
),
reachable as (
    select
        mt.tower_id as source_id,
        mt.tower_id as target_id
    from mesh_towers mt
    union
    select
        r.source_id,
        ue.target_id
    from reachable r
    join undirected_edges ue
        on ue.source_id = r.target_id
)
select
    source_id as tower_id,
    min(target_id) as cluster_id
from reachable
group by source_id;
$$;
