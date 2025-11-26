set client_min_messages = warning;

begin;

do
$$
declare
    sample_tower record;
    affected_cell h3index;
    refresh_radius constant double precision := 15000;
    retries integer := 0;
begin
    select
        t.h3,
        t.centroid_geog
    into sample_tower
    from mesh_towers t
    limit 1;

    if sample_tower.h3 is null then
        raise exception 'mesh_surface_refresh_reception_metrics test requires at least one tower';
    end if;

    loop
        select s.h3
        into affected_cell
        from mesh_surface_h3_r8 s
        where s.has_tower is not true
          and ST_DWithin(s.centroid_geog, sample_tower.centroid_geog, refresh_radius)
          and s.distance_to_closest_tower < 70000
        limit 1;

        exit when affected_cell is not null;

        retries := retries + 1;

        if retries > 5 then
            raise exception 'mesh_surface_refresh_reception_metrics test failed to find nearby surface cell for tower %',
                sample_tower.h3::text;
        end if;
    end loop;

    update mesh_surface_h3_r8
    set clearance = null,
        path_loss = null
    where h3 = affected_cell;

    perform mesh_surface_refresh_reception_metrics(
        sample_tower.h3,
        refresh_radius,
        70000,
        5
    );

    perform (
        select
            case
                when clearance is null or path_loss is null then
                    raise exception 'Reception metrics not restored for % near tower %',
                        affected_cell::text,
                        sample_tower.h3::text
                else
                    null
            end
        from mesh_surface_h3_r8
        where h3 = affected_cell
    );
end;
$$;

rollback;
