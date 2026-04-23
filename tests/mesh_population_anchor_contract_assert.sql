do
$$
begin
    if exists (select 1 from mesh_towers where tower_id = 100) then
        raise exception 'Soft population anchor 100 should contract into route replacement 101';
    end if;

    if exists (select 1 from mesh_towers where tower_id = 102) then
        raise exception 'Route leaf 102 should be removed because replacement 101 preserves its required non-population neighbors';
    end if;

    if not exists (select 1 from mesh_towers where tower_id = 101) then
        raise exception 'Route replacement 101 should remain after soft-star contraction';
    end if;

    if not exists (select 1 from mesh_towers where tower_id = 200) then
        raise exception 'Blocked population anchor 200 should remain because its route neighbors do not preserve each other';
    end if;

    if (select count(*) from mesh_towers where tower_id in (201, 202)) <> 2 then
        raise exception 'Blocked route neighbors 201 and 202 should remain when contraction cannot preserve the visible-neighbor set';
    end if;

    if exists (select 1 from mesh_towers where tower_id = 300) then
        raise exception 'High-population anchor 300 should still contract because contraction is graph-role based, not threshold based';
    end if;



    if exists (select 1 from mesh_towers where tower_id = 500) then
        raise exception 'Synthetic population anchor 500 should contract after route 501 moves to the LOS-preserving candidate cell';
    end if;

    if (select h3 from mesh_towers where tower_id = 501) <> (select h3 from test_cells where label = 'synthetic_candidate') then
        raise exception 'Route tower 501 should move to the synthetic candidate cell before population anchor 500 is removed';
    end if;

    if (select count(*) from mesh_towers where tower_id in (502, 503, 504)) <> 3 then
        raise exception 'Synthetic contraction should preserve external route neighbors 502/503/504';
    end if;

    if not exists (select 1 from mesh_towers where tower_id = 600) then
        raise exception 'Bridge population anchor 600 should remain because deleting it would break global LOS connectivity between route 601 and seed 611 through population anchor 610';
    end if;

    if not exists (select 1 from mesh_towers where tower_id = 610) then
        raise exception 'Bridge population anchor 610 should remain because it has no generated replacement candidate';
    end if;

    if (select count(*) from mesh_towers where tower_id in (601, 611)) <> 2 then
        raise exception 'Bridge route 601 and seed 611 should remain while the connectivity-preserving population anchors stay installed';
    end if;

    if (select count(*) from mesh_towers where tower_id in (401, 402)) <> 2 then
        raise exception 'Close route-only pair 401/402 should remain because contraction only processes population-anchor stars';
    end if;

    if exists (select 1 from mesh_tower_wiggle_queue where tower_id in (100, 102, 300, 500)) then
        raise exception 'Contracted towers should be removed from mesh_tower_wiggle_queue';
    end if;
end;
$$;

rollback;
