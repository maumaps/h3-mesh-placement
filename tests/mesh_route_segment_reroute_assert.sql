-- Verify the local two-relay route chain moved onto the better cached-LOS pair.
do $$
declare
    left_h3 h3index;
    right_h3 h3index;
    blocked_h3 h3index;
    dirty_count integer;
begin
    select h3 into left_h3 from mesh_towers where tower_id = 2;
    select h3 into right_h3 from mesh_towers where tower_id = 3;
    select h3 into blocked_h3 from mesh_towers where tower_id = 11;

    if left_h3 <> '882c2e4e07fffff'::h3index then
        raise exception 'route segment reroute should move left relay tower 2 onto building-bearing candidate 882c2e4e07fffff, got %', left_h3::text;
    end if;

    if right_h3 <> '882c05b747fffff'::h3index then
        raise exception 'route segment reroute should move right relay tower 3 onto downstream candidate 882c05b747fffff, got %', right_h3::text;
    end if;

    if blocked_h3 <> '882c2c6d61fffff'::h3index then
        raise exception 'route segment reroute should not move tower 11 because it has an extra visible neighbor, got %', blocked_h3::text;
    end if;

    select count(*)
    into dirty_count
    from mesh_tower_wiggle_queue
    where tower_id in (2, 3)
      and is_dirty;

    if dirty_count <> 2 then
        raise exception 'route segment reroute should mark both moved relay towers dirty for wiggle, got % dirty rows', dirty_count;
    end if;

    if exists (
        select 1
        from mesh_surface_h3_r8
        where h3 in ('882c2e419dfffff'::h3index, '882c2e566dfffff'::h3index)
          and has_tower
    ) then
        raise exception 'route segment reroute should clear has_tower from old relay H3 cells after moving towers';
    end if;
end $$;
