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
begin
    for rec in
        select *
        from (
            values
                ('Poti', 'Gomismta', true),
                ('Poti', 'Feria 2', true),
                ('Komzpa', 'Feria 2', true),
                ('Komzpa', 'Batumi South', true),
                ('Batumi South', 'SoNick', true),
                ('Tbilisi hackerspace', 'Poti', false),
                ('Tbilisi hackerspace', 'Gomismta', false),
                ('Tbilisi hackerspace', 'Feria 2', false),
                ('Tbilisi hackerspace', 'Komzpa', false),
                ('Tbilisi hackerspace', 'Batumi South', false),
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

        forward_result := h3_los_between_cells(src_h3, dst_h3);
        if forward_result is distinct from rec.expected_visible then
            raise exception 'LOS mismatch % (%s) -> % (%s): expected %, got % per doc/H3 talk requirements',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, rec.expected_visible, forward_result;
        end if;

        reverse_result := h3_los_between_cells(dst_h3, src_h3);
        if reverse_result is distinct from forward_result then
            raise exception 'LOS symmetry mismatch between % (%s) and % (%s): forward %, reverse %',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, forward_result, reverse_result;
        end if;
    end loop;
end;
$$;

rollback;
