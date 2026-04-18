set client_min_messages = warning;

-- Ensure georgia_unfit_areas stores every restricted region class we rely on.
begin;

do
$$
declare
    missing_regions text;
begin
    select string_agg(expected.region, ', ' order by expected.region)
    into missing_regions
    from (
        values
            ('abkhazia'),
            ('south ossetia'),
            ('armenia_non_georgia_border'),
            ('military')
    ) as expected(region)
    where not exists (
        select 1
        from georgia_unfit_areas u
        where lower(u.region) = expected.region
    );

    if missing_regions is not null then
        raise exception
            'georgia_unfit_areas is missing restricted regions: % (expected abkhazia, south ossetia, armenia_non_georgia_border, military)',
            missing_regions;
    end if;
end;
$$;

rollback;
