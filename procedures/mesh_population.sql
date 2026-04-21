set client_min_messages = notice;

-- Add a small fixed-k set of serviceable population anchors before routing.

do $$
declare
    v_enabled boolean;
    v_min_count integer;
    v_max_count integer;
begin
    select
        coalesce((select value::boolean from mesh_pipeline_settings where setting = 'enable_population'), true),
        greatest(coalesce((select value::integer from mesh_pipeline_settings where setting = 'population_anchor_min_count'), 7), 0),
        greatest(coalesce((select value::integer from mesh_pipeline_settings where setting = 'population_anchor_max_count'), 7), 0)
    into v_enabled, v_min_count, v_max_count;
    raise notice 'Population stage: enabled=%, min_count=%, max_count=%', v_enabled, v_min_count, v_max_count;
end;
$$;
-- The stage deliberately has no city-specific inputs; calibration places
-- belong in tests, not in production configuration.

-- Reset prior population towers so reruns stay idempotent and unlock their cells.
delete from mesh_towers
where source = coalesce((
    select value
    from mesh_pipeline_settings
    where setting = 'population_anchor_source'
), 'population');

-- Clear surface flags for cells that just lost a population tower so eligibility stays accurate.
update mesh_surface_h3_r8 s
set has_tower = false,
    distance_to_closest_tower = null
where has_tower
  and not exists (
        select 1
        from mesh_towers t
        where t.h3 = s.h3
    );

-- Recompute spacing after cleanup to refresh can_place_tower before clustering.
update mesh_surface_h3_r8 s
set distance_to_closest_tower = sub.dist_m
from (
    select
        s2.h3,
        min(ST_Distance(s2.centroid_geog, t.centroid_geog)) as dist_m
    from mesh_surface_h3_r8 s2
    join mesh_towers t on true
    group by s2.h3
) sub
where s.h3 = sub.h3;

-- Pick fixed-k population anchors by interleaving serviceability and nearby population.
with settings as (
    select
        coalesce((select value::boolean from mesh_pipeline_settings where setting = 'enable_population'), true) as enabled,
        greatest(coalesce((select value::integer from mesh_pipeline_settings where setting = 'population_anchor_min_count'), 7), 0) as min_count,
        greatest(coalesce((select value::integer from mesh_pipeline_settings where setting = 'population_anchor_max_count'), 7), 0) as max_count,
        coalesce((select value from mesh_pipeline_settings where setting = 'population_anchor_source'), 'population') as source,
        coalesce((select value::double precision from mesh_pipeline_settings where setting = 'max_los_distance_m'), 80000) as max_los_distance_m,
        coalesce((select value::double precision from mesh_pipeline_settings where setting = 'population_building_weight'), 1.0) as building_weight,
        coalesce((select value::double precision from mesh_pipeline_settings where setting = 'population_nearby_population_weight'), 1.0) as nearby_population_weight,
        coalesce((select value from mesh_pipeline_settings where setting = 'population_cluster_weight_metric'), 'population') as cluster_weight_metric,
        greatest(coalesce((select value::double precision from mesh_pipeline_settings where setting = 'population_existing_anchor_weight'), 1000000), 1) as existing_anchor_weight,
        greatest(coalesce((select value::integer from mesh_pipeline_settings where setting = 'population_anchor_cluster_oversampling'), 2), 1) as cluster_oversampling
),
normalized_settings as (
    select
        enabled,
        least(min_count, greatest(max_count, min_count)) as min_count,
        greatest(max_count, min_count) as max_count,
        source,
        max_los_distance_m,
        building_weight,
        nearby_population_weight,
        case
            when cluster_weight_metric in ('population', 'population_70km') then cluster_weight_metric
            else 'population'
        end as cluster_weight_metric,
        existing_anchor_weight,
        cluster_oversampling
    from settings
),
existing_anchor_cells as (
    -- Existing non-population placement is part of the demand picture.
    -- Give those cells a large KMeans weight so clusters form around already
    -- served islands, then drop those occupied clusters after clustering.
    select distinct on (t.h3)
        t.h3,
        t.centroid_geog
    from mesh_towers t
),
placeable_candidates as (
    -- Cluster the full serviceable demand field first. Covered candidates are
    -- not removed, so KMeans sees true settlement geometry.
    select
        s.h3,
        s.centroid_geog,
        coalesce(s.population, 0)::double precision as local_population,
        coalesce(s.population_70km, s.population, 0)::double precision as nearby_population,
        coalesce(s.building_count, 0)::double precision as building_count
    from mesh_surface_h3_r8 s
    where s.can_place_tower
      and coalesce(s.population_70km, s.population, 0) > 0
),
building_candidate_count as (
    select count(*) as candidate_count
    from placeable_candidates
    where building_count > 0
),
rankable_candidates as (
    select pc.*
    from placeable_candidates pc
    cross join normalized_settings ns
    cross join building_candidate_count bcc
    where pc.building_count > 0
       or bcc.candidate_count < ns.max_count
),
scored_candidates as (
    select
        rc.h3,
        rc.centroid_geog,
        rc.local_population,
        rc.nearby_population,
        rc.building_count,
        false as existing_anchor,
        greatest(
            case ns.cluster_weight_metric
                when 'population' then rc.local_population
                else rc.nearby_population
            end,
            1
        ) as cluster_weight,
        power(ln(1 + rc.nearby_population), ns.nearby_population_weight)
            * power(ln(2 + rc.building_count), ns.building_weight) as score
    from rankable_candidates rc
    cross join normalized_settings ns

    union all

    select
        eac.h3,
        eac.centroid_geog,
        0::double precision as local_population,
        0::double precision as nearby_population,
        0::double precision as building_count,
        true as existing_anchor,
        ns.existing_anchor_weight as cluster_weight,
        0::double precision as score
    from existing_anchor_cells eac
    cross join normalized_settings ns
),
cluster_count as (
    select
        least(
            (select max_count * cluster_oversampling from normalized_settings),
            greatest(1, count(*))
        )::integer as k
    from scored_candidates
),
candidate_points as (
    -- Pass demand/existing-anchor weight as the M coordinate. PostGIS weighted
    -- KMeans treats positive M values as point weights, so centroids move
    -- toward high-demand cells and already served tower islands.
    select
        sc.*,
        ST_SetSRID(
            ST_MakePoint(
                ST_X(gc.geom_3d),
                ST_Y(gc.geom_3d),
                ST_Z(gc.geom_3d),
                sc.cluster_weight
            ),
            4978
        ) as cluster_geom
    from scored_candidates sc
    cross join lateral (
        select ST_Transform(ST_Force3D(sc.centroid_geog::geometry), 4978) as geom_3d
    ) as gc
),
clustered as (
    select
        cp.*,
        ST_ClusterKMeans(cp.cluster_geom, (select k from cluster_count)) over () as cluster_id
    from candidate_points cp
),
cluster_centroids as (
    -- Reconstruct the weighted KMeans centroid for each assigned cluster.
    select
        cluster_id,
        bool_or(existing_anchor) as cluster_has_existing_anchor,
        ST_SetSRID(
            ST_MakePoint(
                sum(ST_X(cluster_geom) * cluster_weight) / sum(cluster_weight),
                sum(ST_Y(cluster_geom) * cluster_weight) / sum(cluster_weight),
                sum(ST_Z(cluster_geom) * cluster_weight) / sum(cluster_weight)
            ),
            4978
        ) as centroid_geom
    from clustered
    group by cluster_id
),
cluster_winners as (
    select distinct on (cl.cluster_id)
        cl.cluster_id,
        cl.h3,
        cl.nearby_population,
        cl.building_count,
        cl.score
    from clustered cl
    join cluster_centroids cc on cc.cluster_id = cl.cluster_id
    where not cl.existing_anchor
      and not cc.cluster_has_existing_anchor
    order by
        cl.cluster_id,
        ST_3DDistance(cl.cluster_geom, cc.centroid_geom) asc,
        cl.score desc,
        cl.nearby_population desc,
        cl.building_count desc,
        cl.h3
),
selected_anchors as (
    select * from cluster_winners
),
trimmed_anchors as (
    select sa.*
    from selected_anchors sa
    order by
        sa.score desc,
        sa.nearby_population desc,
        sa.building_count desc,
        sa.h3
    limit (select max_count from normalized_settings)
),
inserted as (
    insert into mesh_towers (h3, source)
    select ta.h3, ns.source
    from trimmed_anchors ta
    cross join normalized_settings ns
    where ns.enabled
      and ns.max_count > 0
    on conflict (h3) do nothing
    returning h3
)
select coalesce(count(*), 0) as inserted_population_towers
from inserted;

-- Keep surface tower flags in sync with the refreshed mesh_towers content.
update mesh_surface_h3_r8 s
set has_tower = true,
    distance_to_closest_tower = 0
where exists (
    select 1
    from mesh_towers t
    where t.h3 = s.h3
);

-- Recompute spacing after tower set changed so can_place_tower stays accurate downstream.
update mesh_surface_h3_r8 s
set distance_to_closest_tower = sub.dist_m
from (
    select
        s2.h3,
        min(ST_Distance(s2.centroid_geog, t.centroid_geog)) as dist_m
    from mesh_surface_h3_r8 s2
    join mesh_towers t on true
    group by s2.h3
) sub
where s.h3 = sub.h3;

do $$
declare
    v_count integer;
    v_source text;
begin
    select coalesce(value, 'population') into v_source
    from mesh_pipeline_settings where setting = 'population_anchor_source';
    select count(*) into v_count from mesh_towers where source = coalesce(v_source, 'population');
    raise notice 'Population stage complete: % anchor towers (source=%)', v_count, coalesce(v_source, 'population');
end;
$$;
