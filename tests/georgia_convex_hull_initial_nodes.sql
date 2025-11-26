set client_min_messages = warning;

-- Ensure every seed node lies inside the convex hull
begin;

do
$$
declare
    outside_count bigint;
begin
    select count(*)
    into outside_count
    from mesh_initial_nodes n
    cross join georgia_convex_hull h
    where not ST_Intersects(h.geom, n.geom);

    if outside_count > 0 then
        raise exception
            'georgia_convex_hull misses % initial nodes; convex hull must cover all seed points',
            outside_count;
    end if;
end;
$$;

rollback;
