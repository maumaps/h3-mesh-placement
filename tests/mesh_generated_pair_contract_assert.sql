do
$$
begin
    if (select count(*) from mesh_towers where tower_id in (100, 101)) <> 1 then
        raise exception 'Contractible generated pair 100/101 should leave exactly one route tower';
    end if;

    if not exists (
        select 1
        from mesh_towers
        where tower_id = 100
          and h3 = (select h3 from test_cells where label = 'synthetic_route')
    ) then
        raise exception 'Generated pair contraction should move kept tower 100 to the synthetic LOS-preserving H3';
    end if;

    if exists (select 1 from mesh_towers where tower_id = 101) then
        raise exception 'Generated pair contraction should delete redundant tower 101';
    end if;

    if (select count(*) from mesh_towers where tower_id in (102, 103)) <> 2 then
        raise exception 'Generated pair contraction should preserve required external route neighbors 102/103';
    end if;

    if (select count(*) from mesh_towers where tower_id in (200, 201)) <> 2 then
        raise exception 'Blocked generated pair 200/201 should remain because no synthetic H3 preserves both external neighbors';
    end if;

    if exists (select 1 from mesh_tower_wiggle_queue where tower_id = 101) then
        raise exception 'Deleted generated tower 101 should be removed from wiggle queue';
    end if;
end;
$$;

rollback;
