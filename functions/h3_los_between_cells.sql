set client_min_messages = warning;

drop function if exists h3_los_between_cells(h3index, h3index);

create or replace function h3_los_between_cells(h3_a h3index, h3_b h3index)
    returns boolean
    language plpgsql
    cost 100000
as
$$
declare
    norm_src      h3index;
    norm_dst      h3index;
    cached        boolean;
    distance_m    double precision;
    is_visible    boolean;

    max_distance  constant double precision := 60000;  -- hard cut at 60 km
    tower_height  constant double precision := 40;     -- tower height above ground, meters

    ele_src       double precision;
    ele_dst       double precision;
    line_geom     geometry;
    has_blocker   boolean;
begin
    -- Nulls => no LOS
    if h3_a is null or h3_b is null then
        return false;
    end if;

    -- Same cell => trivially visible
    if h3_a = h3_b then
        return true;
    end if;

    -- Normalize order for cache symmetry
    norm_src := least(h3_a, h3_b);
    norm_dst := greatest(h3_a, h3_b);

    -- Cache lookup
    select mlc.is_visible
    into cached
    from mesh_los_cache mlc
    where mlc.src_h3 = norm_src
      and mlc.dst_h3 = norm_dst;

    if cached is not null then
        return cached;
    end if;

    -- Geodesic distance between cell centroids
    distance_m := ST_Distance(norm_src::geography, norm_dst::geography);

    -- Hard cutoff: do not even try beyond horizon range you care about
    if distance_m > max_distance then
        is_visible := false;
    else
        -- Straight line between tower centroids (no segmentize needed here)
        line_geom := ST_MakeLine(norm_src::geometry, norm_dst::geometry);

        -- Tower top elevations at both ends
        select coalesce(ele, 0) + tower_height
        into ele_src
        from mesh_surface_h3_r8
        where h3 = norm_src;

        if ele_src is null then
            ele_src := tower_height;
        end if;

        select coalesce(ele, 0) + tower_height
        into ele_dst
        from mesh_surface_h3_r8
        where h3 = norm_dst;

        if ele_dst is null then
            ele_dst := tower_height;
        end if;

        /*
         * 1) h3_grid_path_cells(norm_src, norm_dst) gives us the H3 cells along
         *    the path between the towers (inclusive).
         * 2) We join that to mesh_surface_h3_r8 to get elevations.
         * 3) For each intermediate cell:
         *      frac = position of cell centroid along the line [0..1]
         *      los_height(frac) = ele_src + (ele_dst - ele_src) * frac
         *    If terrain elevation >= los_height at any point, LOS is blocked.
         */
        with path as (
            select h3_grid_path_cells(norm_src, norm_dst) as h3
        ),
        samples as (
            select
                c.h3,
                ST_LineLocatePoint(
                    line_geom,
                    ST_PointOnSurface(c.geom)
                )::double precision as frac,
                c.ele
            from path p
            join mesh_surface_h3_r8 c
              on c.h3 = p.h3
            where p.h3 not in (norm_src, norm_dst)
        )
        select exists (
            select 1
            from samples s
            where s.frac > 0
              and s.frac < 1
              and (
                    ele_src + (ele_dst - ele_src) * s.frac
                  ) <= coalesce(s.ele, 0)
        )
        into has_blocker;

        is_visible := not coalesce(has_blocker, false);
    end if;

    -- Write to cache
    insert into mesh_los_cache (src_h3, dst_h3, distance_m, is_visible, checked_at)
    values (norm_src, norm_dst, distance_m, is_visible, now())
    on conflict (src_h3, dst_h3) do update
        set is_visible = excluded.is_visible,
            distance_m = excluded.distance_m,
            checked_at = now();

    return is_visible;
end;
$$;
