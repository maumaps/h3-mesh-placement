set client_min_messages = warning;

-- Ensure population_h3_r8 directly reflects Kontur's H3 ids
begin;

do
$$
declare
    missing_count bigint;
begin
    select count(*)
    into missing_count
    from population_h3_r8 p
    left join kontur_population k
        on k.h3::h3index = p.h3
    where k.h3 is null;

    if missing_count > 0 then
        raise exception
            'population_h3_r8 contains % cells not backed by kontur_population; expected 1:1 casting',
            missing_count;
    end if;
end;
$$;

rollback;
