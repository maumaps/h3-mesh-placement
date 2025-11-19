set client_min_messages = warning;

-- Ensure georgia_roads_geom only contains highways that should be driveable by cars
begin;

do
$$
declare
    allowed_highways constant text[] := array[
        'motorway', 'motorway_link',
        'trunk', 'trunk_link',
        'primary', 'primary_link',
        'secondary', 'secondary_link',
        'tertiary', 'tertiary_link',
        'unclassified', 'residential',
        'living_street', 'service',
        'road'
    ];
    offending text;
begin
    select string_agg(distinct highway, ', ' order by highway)
    into offending
    from georgia_roads_geom
    where highway is not null
      and not (highway = any(allowed_highways));

    if offending is not null then
        raise exception
            'georgia_roads_geom contains non-car highways: % (allowed: %)',
            offending,
            array_to_string(allowed_highways, ', ');
    end if;
end;
$$;

rollback;
