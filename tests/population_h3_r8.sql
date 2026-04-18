set client_min_messages = warning;

-- Ensure population_h3_r8 preserves Kontur totals while filling road-served gaps.
begin;

do
$$
declare
    missing_kontur_count bigint;
    total_count bigint;
    roadless_count bigint;
    non_positive_count bigint;
begin
    -- Ensure population table is populated to avoid zero-population animations.
    select count(*)
    into total_count
    from population_h3_r8;

    if total_count = 0 then
        raise exception
            'population_h3_r8 is empty; expected Kontur-derived rows so animation counters are non-zero';
    end if;

    -- Ensure every in-boundary Kontur-backed H3 survives in the repaired table.
    select count(*)
    into missing_kontur_count
    from (
        select distinct k.h3::h3index as h3
        from kontur_population k
        join georgia_boundary boundary
          on ST_Intersects(k.geom, boundary.geom)
        where k.population > 0
          and k.h3 is not null
    ) kontur
    left join population_h3_r8 p on p.h3 = kontur.h3
    where p.h3 is null;

    if missing_kontur_count > 0 then
        raise exception
            'population_h3_r8 is missing % in-boundary Kontur-backed H3 rows out of % total rows',
            missing_kontur_count,
            total_count;
    end if;

    -- Ensure every road-served H3 ends up with a positive population floor.
    select count(*)
    into roadless_count
    from roads_h3_r8 r
    left join population_h3_r8 p on p.h3 = r.h3
    where p.h3 is null
       or p.population <= 0;

    if roadless_count > 0 then
        raise exception
            'population_h3_r8 left % road-served cells without positive population after fallback repair',
            roadless_count;
    end if;

    -- Ensure the fallback never creates non-positive values.
    select count(*)
    into non_positive_count
    from population_h3_r8
    where population <= 0;

    if non_positive_count > 0 then
        raise exception
            'population_h3_r8 contains % non-positive rows after fallback repair',
            non_positive_count;
    end if;
end;
$$;

rollback;
