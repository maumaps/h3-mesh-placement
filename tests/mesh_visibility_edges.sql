set client_min_messages = warning;

-- Ensure mesh_visibility_edges entries covering seed towers match h3_visibility_clearance() expectations.
begin;

do
$$
declare
    rec record;
    constant_mast_height constant double precision := 28;
    constant_frequency constant double precision := 868e6;
    max_distance constant double precision := 70000;
begin
    for rec in
        with pairs as (
            select
                a.h3 as src_h3,
                b.h3 as dst_h3,
                coalesce(a.name, 'seed') as src_name,
                coalesce(b.name, 'seed') as dst_name,
                h3_visibility_clearance(
                    a.h3,
                    b.h3,
                    constant_mast_height,
                    constant_mast_height,
                    constant_frequency
                ) as clearance,
                ST_Distance(a.h3::geography, b.h3::geography) as distance_m
            from mesh_initial_nodes_h3_r8 a
            join mesh_initial_nodes_h3_r8 b
              on a.h3 < b.h3
            where ST_Distance(a.h3::geography, b.h3::geography) <= max_distance
        )
        select
            p.src_h3,
            p.dst_h3,
            p.src_name,
            p.dst_name,
            p.distance_m,
            p.clearance,
            (p.clearance > 0) as expected_visible,
            e.is_visible as stored_visible
        from pairs p
        left join mesh_visibility_edges e
          on least(e.source_h3, e.target_h3) = p.src_h3
         and greatest(e.source_h3, e.target_h3) = p.dst_h3
    loop
        if rec.stored_visible is null then
            raise exception 'Seed visibility edge missing for % (%s) -> % (%s) at %.2f km; clearance % meters and expected visibility %',
                rec.src_name, rec.src_h3::text, rec.dst_name, rec.dst_h3::text, rec.distance_m / 1000.0, rec.clearance, rec.expected_visible;
        end if;

        if rec.stored_visible is distinct from rec.expected_visible then
            raise exception 'Seed visibility mismatch for % (%s) -> % (%s): stored % but clearance % meters implies %',
                rec.src_name, rec.src_h3::text, rec.dst_name, rec.dst_h3::text, rec.stored_visible, rec.clearance, rec.expected_visible;
        end if;
    end loop;
end;
$$;

do
$$
declare
    rec record;
begin
    for rec in
        with expectations as (
            select
                least(src_name, dst_name) as src_name,
                greatest(src_name, dst_name) as dst_name,
                expected_visible
            from (
                values
                    ('Poti', 'Gomismta', true),
                    ('Poti', 'Feria 2', true),
                    ('Poti', 'SoNick', true),
                    ('Komzpa', 'Feria 2', true),
                    ('Komzpa', 'Batumi South', true),
                    ('Batumi South', 'SoNick', true),
                    ('Tbilisi hackerspace', 'Poti', false),
                    ('Tbilisi hackerspace', 'Gomismta', false),
                    ('Tbilisi hackerspace', 'Feria 2', false),
                    ('Tbilisi hackerspace', 'Komzpa', false),
                    ('Tbilisi hackerspace', 'Batumi South', false),
                    ('Tbilisi hackerspace', 'SoNick', false)
            ) as v(src_name, dst_name, expected_visible)
        ),
        actual as (
            select
                least(coalesce(a.name, 'seed'), coalesce(b.name, 'seed')) as src_name,
                greatest(coalesce(a.name, 'seed'), coalesce(b.name, 'seed')) as dst_name,
                e.is_visible
            from mesh_visibility_edges e
            join mesh_initial_nodes_h3_r8 a
              on a.h3 = e.source_h3
            join mesh_initial_nodes_h3_r8 b
              on b.h3 = e.target_h3
        )
        select
            exp.src_name,
            exp.dst_name,
            exp.expected_visible,
            act.is_visible
        from expectations exp
        left join actual act
          on act.src_name = exp.src_name
         and act.dst_name = exp.dst_name
    loop
        if rec.is_visible is null then
            raise exception 'Seed visibility row missing for % -> %; expected visibility % per doc/H3 ground truth',
                rec.src_name, rec.dst_name, rec.expected_visible;
        end if;

        if rec.is_visible is distinct from rec.expected_visible then
            raise exception 'Seed visibility row mismatch for % -> %: stored %, expected % per doc/H3 ground truth',
                rec.src_name, rec.dst_name, rec.is_visible, rec.expected_visible;
        end if;
    end loop;
end;
$$;

rollback;
