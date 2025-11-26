set client_min_messages = warning;

begin;

do
$$
declare
    sample_h3 h3index;
    positive_src h3index;
    positive_dst h3index;
    blocked_src h3index;
    pos_clearance double precision;
    pos_loss double precision;
    neg_clearance double precision;
    neg_loss double precision;
    has_reception_val boolean;
begin
    select h3
    into sample_h3
    from mesh_surface_h3_r8
    where has_tower is not true
      and distance_to_closest_tower between 100 and 10000
    limit 1;

    if sample_h3 is null then
        raise exception 'Reception test could not find target cell without tower but within 10 km of coverage';
    end if;

    select h3
    into positive_src
    from mesh_initial_nodes_h3_r8
    where name ilike '%Poti%'
    limit 1;

    if positive_src is null then
        raise exception 'Reception test could not find Poti seed tower';
    end if;

    select h3
    into positive_dst
    from mesh_initial_nodes_h3_r8
    where name ilike '%Gomismta%'
    limit 1;

    if positive_dst is null then
        raise exception 'Reception test could not find Gomismta seed tower';
    end if;

    select clearance, path_loss_db
    into pos_clearance, pos_loss
    from h3_visibility_metrics(positive_src, positive_dst, 28, 28, 868e6);

    update mesh_surface_h3_r8
    set clearance = pos_clearance,
        path_loss = pos_loss
    where h3 = sample_h3;

    select has_reception
    into has_reception_val
    from mesh_surface_h3_r8
    where h3 = sample_h3;

    if has_reception_val is not true then
        raise exception 'Expected has_reception=true for % with clearance % m and path loss % dB from Poti -> Gomismta link',
            sample_h3::text, pos_clearance, pos_loss;
    end if;

    select h3
    into blocked_src
    from mesh_initial_nodes_h3_r8
    where name ilike '%Tbilisi%'
    limit 1;

    if blocked_src is null then
        raise exception 'Reception test could not find Tbilisi seed tower';
    end if;

    select clearance, path_loss_db
    into neg_clearance, neg_loss
    from h3_visibility_metrics(blocked_src, positive_src, 28, 28, 868e6);

    if neg_clearance > 0 then
        raise exception 'Expected non-positive clearance for Tbilisi -> Poti hop, got % meters',
            neg_clearance;
    end if;

    update mesh_surface_h3_r8
    set clearance = neg_clearance,
        path_loss = neg_loss
    where h3 = sample_h3;

    select has_reception
    into has_reception_val
    from mesh_surface_h3_r8
    where h3 = sample_h3;

    if has_reception_val is not false then
        raise exception 'Expected has_reception=false for % with clearance % m and path loss % dB from Tbilisi -> Poti hop',
            sample_h3::text, neg_clearance, neg_loss;
    end if;
end;
$$;

rollback;
