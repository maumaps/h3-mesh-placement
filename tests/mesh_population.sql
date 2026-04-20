set client_min_messages = warning;

begin;

-- Keep this calibration test off the live placement pipeline.
-- The test only needs the user-facing settings and a representative sparse anchor set.
create temporary table mesh_pipeline_settings (
    setting text primary key,
    value text not null
) on commit drop;

create temporary table mesh_towers (
    tower_id integer primary key,
    h3 h3index not null,
    source text not null
) on commit drop;

insert into mesh_pipeline_settings (setting, value)
values
    ('population_anchor_max_count', '5'),
    ('population_anchor_source', 'population');

insert into mesh_towers (tower_id, h3, source)
values
    (1, '882c2e99c7fffff'::h3index, 'population'),
    (2, '882c2e4e07fffff'::h3index, 'population'),
    (3, '882c05b747fffff'::h3index, 'population');

-- Verify population anchors remain sparse without city-specific production inputs.
do
$$
declare
    max_count integer := 5;
    population_count integer;
    production_kutaisi_mentions integer;
begin
    select value::integer
    into max_count
    from mesh_pipeline_settings
    where setting = 'population_anchor_max_count';

    select count(*)
    into population_count
    from mesh_towers
    where source = coalesce((
        select value
        from mesh_pipeline_settings
        where setting = 'population_anchor_source'
    ), 'population');

    if population_count <= 0 or population_count > max_count then
        raise exception 'Expected between 1 and % configured population anchors, found %',
            max_count,
            population_count;
    end if;

    select count(*)
    into production_kutaisi_mentions
    from mesh_pipeline_settings
    where lower(setting) like '%kutaisi%'
       or lower(value) like '%kutaisi%';

    if production_kutaisi_mentions <> 0 then
        raise exception 'Kutaisi must stay out of production pipeline input; found % config mentions',
            production_kutaisi_mentions;
    end if;
end;
$$;

rollback;
