set client_min_messages = warning;

drop function if exists mesh_surface_fill_visible_population(h3index);

-- Fill visible population metric for a single candidate cell
create or replace function mesh_surface_fill_visible_population(target_h3 h3index)
    returns numeric
    language plpgsql
    volatile
    parallel restricted
as
$$
declare
    target_centroid public.geography;
    visible_pop numeric;
begin
    if target_h3 is null then
        return null;
    end if;

    select centroid_geog
    into target_centroid
    from mesh_surface_h3_r8
    where h3 = target_h3
      and can_place_tower;

    if target_centroid is null then
        return null;
    end if;

    with candidate_neighbors as materialized (
        select
            t.h3,
            t.population
        from mesh_surface_h3_r8 t
        where t.population > 0
          and ST_DWithin(target_centroid, t.centroid_geog, 70000)
    )
    select coalesce(sum(cn.population), 0)
    into visible_pop
    from candidate_neighbors cn
    where h3_los_between_cells(target_h3, cn.h3);

    update mesh_surface_h3_r8
    set visible_population = visible_pop
    where h3 = target_h3;

    return visible_pop;
end;
$$;
