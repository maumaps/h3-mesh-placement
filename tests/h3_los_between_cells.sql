set client_min_messages = warning;

-- Validate h3_los_between_cells() using doc/H3 talk visibility pairs.
begin;

truncate mesh_los_cache;

do
$$
declare
    rec record;
    src_h3 h3index;
    dst_h3 h3index;
    forward_result boolean;
    reverse_result boolean;
    clearance_forward double precision;
    clearance_reverse double precision;
    constant_mast_height constant double precision := 28;
    constant_frequency constant double precision := 868e6;
    metrics_clearance double precision;
    metrics_loss double precision;
begin
    for rec in
        select *
        from (
            values
                ('Poti', 'Feria 2', true),
                ('Poti', 'SoNick', true),
                ('Komzpa', 'Feria 2', true),
                ('Komzpa', 'SoNick', false),
                ('Tbilisi hackerspace', 'Poti', false),
                ('Tbilisi hackerspace', 'Feria 2', false),
                ('Tbilisi hackerspace', 'Komzpa', false),
                ('Tbilisi hackerspace', 'SoNick', false)
        ) as expectations(src_name, dst_name, expected_visible)
    loop
        select h3
        into src_h3
        from mesh_initial_nodes_h3_r8
        where name = rec.src_name;

        if not found then
            raise exception 'Missing seed node "%"', rec.src_name;
        end if;

        select h3
        into dst_h3
        from mesh_initial_nodes_h3_r8
        where name = rec.dst_name;

        if not found then
            raise exception 'Missing seed node "%"', rec.dst_name;
        end if;

        clearance_forward := h3_visibility_clearance(
            src_h3,
            dst_h3,
            constant_mast_height,
            constant_mast_height,
            constant_frequency
        );

        forward_result := h3_los_between_cells(src_h3, dst_h3);
        if forward_result is distinct from rec.expected_visible then
            raise exception 'LOS mismatch % (%s) -> % (%s): expected %, got % per doc/H3 talk requirements',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, rec.expected_visible, forward_result;
        end if;

        if (clearance_forward > 0) is distinct from rec.expected_visible then
            raise exception 'Clearance mismatch % (%s) -> % (%s): expected visibility %, got clearance % meters',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, rec.expected_visible, clearance_forward;
        end if;

        select m.clearance, m.path_loss_db
        into metrics_clearance, metrics_loss
        from h3_visibility_metrics(
            src_h3,
            dst_h3,
            constant_mast_height,
            constant_mast_height,
            constant_frequency
        ) as m;

        if metrics_clearance is distinct from clearance_forward then
            raise exception 'Metrics clearance mismatch % (%s) -> % (%s): helper %, metrics %',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, clearance_forward, metrics_clearance;
        end if;

        if metrics_loss is null or metrics_loss <= 0 then
            raise exception 'Metrics loss invalid for % (%s) -> % (%s): got % dB',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, metrics_loss;
        end if;

        clearance_reverse := h3_visibility_clearance(
            dst_h3,
            src_h3,
            constant_mast_height,
            constant_mast_height,
            constant_frequency
        );

        reverse_result := h3_los_between_cells(dst_h3, src_h3);
        if reverse_result is distinct from forward_result then
            raise exception 'LOS symmetry mismatch between % (%s) and % (%s): forward %, reverse %',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, forward_result, reverse_result;
        end if;

        if clearance_reverse is distinct from clearance_forward then
            raise exception 'Clearance symmetry mismatch between % (%s) and % (%s): forward clearance %, reverse clearance %',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, clearance_forward, clearance_reverse;
        end if;
    end loop;
end;
$$;

rollback;
