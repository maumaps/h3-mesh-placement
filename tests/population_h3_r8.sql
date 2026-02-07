set client_min_messages = warning;

-- Ensure population_h3_r8 directly reflects Kontur's H3 ids
begin;

do
$$
declare
    missing_count bigint;
    total_count bigint;
begin
    -- Ensure population table is populated to avoid zero-population animations.
    select count(*)
    into total_count
    from population_h3_r8;

    if total_count = 0 then
        raise exception
            'population_h3_r8 is empty; expected Kontur-derived rows so animation counters are non-zero';
    end if;

    -- Ensure population_h3_r8 directly reflects Kontur H3 ids.
    select count(*)
    into missing_count
    from population_h3_r8 p
    left join kontur_population k
        on k.h3::h3index = p.h3
    where k.h3 is null;

    if missing_count > 0 then
        raise exception
            'population_h3_r8 contains % cells not backed by kontur_population; expected 1:1 casting over % total rows',
            missing_count,
            total_count;
    end if;
end;
$$;

rollback;
