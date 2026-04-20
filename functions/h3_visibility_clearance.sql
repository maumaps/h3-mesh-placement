set client_min_messages = warning;

drop function if exists h3_visibility_clearance_compute_row(h3index, h3index, double precision, double precision, double precision);
drop function if exists h3_visibility_clearance_compute(h3index, h3index, double precision, double precision, double precision);
drop function if exists h3_visibility_clearance(h3index, h3index, double precision, double precision, double precision);

-- Pure computation helper that evaluates Fresnel clearance and path loss without touching the cache
create or replace function h3_visibility_clearance_compute_row(
    h3_a h3index,
    h3_b h3index,
    mast_height_src double precision,
    mast_height_dst double precision,
    frequency_hz double precision
)
    returns h3_visibility_metrics
    language plpgsql
    stable
    parallel safe
as
$$
declare
    norm_src       h3index;
    norm_dst       h3index;
    norm_mast_src  double precision;
    norm_mast_dst  double precision;
    norm_frequency double precision;
    src_ele        double precision;
    dst_ele        double precision;
    total_distance double precision;
    grid_step_count integer;
    curvature_scale double precision;
    fresnel_scale double precision;
    clearance_val  double precision;
    worst_d1       double precision;
    worst_d2       double precision;
    -- Effective Earth radius (4/3) to account for atmospheric refraction.
    effective_radius constant double precision := (4.0 / 3.0) * 6371000.0;
    speed_of_light   constant double precision := 299792458.0;
    wavelength     double precision;
begin
    if h3_a is null or h3_b is null then
        return null;
    end if;

    if frequency_hz is null or frequency_hz <= 0 then
        raise exception 'frequency_hz must be positive (got %)', frequency_hz;
    end if;

    if mast_height_src is null or mast_height_src < 0 then
        raise exception 'mast_height_src must be non-negative (got %)', mast_height_src;
    end if;

    if mast_height_dst is null or mast_height_dst < 0 then
        raise exception 'mast_height_dst must be non-negative (got %)', mast_height_dst;
    end if;

    if h3_a <= h3_b then
        norm_src := h3_a;
        norm_dst := h3_b;
        norm_mast_src := mast_height_src;
        norm_mast_dst := mast_height_dst;
    else
        norm_src := h3_b;
        norm_dst := h3_a;
        norm_mast_src := mast_height_dst;
        norm_mast_dst := mast_height_src;
    end if;

    norm_frequency := frequency_hz;

    if norm_src = norm_dst then
        -- Single-cell links need only the local elevation sample.
        select g.ele
        into src_ele
        from gebco_elevation_h3_r8 g
        where g.h3 = norm_src;

        if not found or src_ele is null then
            return null;
        end if;

        return (least(norm_mast_src, norm_mast_dst), null::double precision, 0::double precision, 0::double precision, 0::double precision)::h3_visibility_metrics;
    end if;

    total_distance := ST_Distance(norm_src::geography, norm_dst::geography);

    if total_distance <= 0 then
        return null;
    end if;

    wavelength := speed_of_light / norm_frequency;
    grid_step_count := h3_grid_distance(norm_src, norm_dst) + 1;
    curvature_scale := (total_distance * total_distance) / (2 * effective_radius);
    fresnel_scale := wavelength * total_distance;

    -- Sample elevation values directly from the ordered H3 path and annotate them in
    -- the same pass with endpoint elevations and sample count. This keeps the hot
    -- LOS path to one join over gebco_elevation_h3_r8 with no separate path CTE.
    with annotated_samples as (
        select
            p.h3,
            case
                when grid_step_count <= 1 then 0::double precision
                else ((p.step_no - 1)::double precision / (grid_step_count - 1)::double precision)
            end as frac,
            case
                when grid_step_count <= 1 then 1::double precision
                else 1 - ((p.step_no - 1)::double precision / (grid_step_count - 1)::double precision)
            end as frac_rev,
            ms.ele,
            bool_or(ms.ele is null) over () as has_missing_ele,
            max(ms.ele) filter (where p.h3 = norm_src) over () as src_ele,
            max(ms.ele) filter (where p.h3 = norm_dst) over () as dst_ele
        from h3_grid_path_cells(norm_src, norm_dst) with ordinality as p(h3, step_no)
        left join gebco_elevation_h3_r8 ms
          on ms.h3 = p.h3
    ),
    -- Compute Fresnel clearance at each sample point along the link.
    calc as (
        select
            s.frac,
            s.ele,
            s.src_ele,
            s.dst_ele,
            total_distance * s.frac as d1,
            total_distance * s.frac_rev as d2,
            ((s.src_ele + norm_mast_src) + ((s.dst_ele + norm_mast_dst) - (s.src_ele + norm_mast_src)) * s.frac)
            - (
                s.ele
                + (curvature_scale * s.frac * s.frac_rev)
                + sqrt(fresnel_scale * s.frac * s.frac_rev)
            ) as clearance_value
        from annotated_samples s
        where not s.has_missing_ele
          and s.src_ele is not null
          and s.dst_ele is not null
    ),
    -- Choose the worst (minimum) clearance sample as the link clearance and keep
    -- the endpoint elevations that produced that line of sight profile.
    worst as (
        select
            c.src_ele,
            c.dst_ele,
            c.clearance_value,
            c.d1,
            c.d2
        from calc c
        order by c.clearance_value asc
        limit 1
    )
    select
        worst.src_ele,
        worst.dst_ele,
        worst.clearance_value,
        worst.d1,
        worst.d2
    into src_ele, dst_ele, clearance_val, worst_d1, worst_d2
    from worst;


    if src_ele is null or dst_ele is null then
        -- Missing endpoint elevations mean the path is not computable.
        return null;
    end if;

    if clearance_val is null then
        -- Missing intermediate elevations can leave no valid worst sample.
        -- Return no row so the caller can treat the path as not visible.
        return null;
    end if;

    if worst_d1 is null or worst_d2 is null then
        worst_d1 := total_distance / 2;
        worst_d2 := total_distance / 2;
    end if;

    return (clearance_val, h3_path_loss(total_distance, norm_frequency, clearance_val, worst_d1, worst_d2), total_distance, worst_d1, worst_d2)::h3_visibility_metrics;
end;
$$;

-- Compatibility wrapper that preserves the old table-function interface for existing callers.
create or replace function h3_visibility_clearance_compute(
    h3_a h3index,
    h3_b h3index,
    mast_height_src double precision,
    mast_height_dst double precision,
    frequency_hz double precision
)
    returns table (
        clearance double precision,
        path_loss_db double precision,
        distance_m double precision,
        d1_m double precision,
        d2_m double precision
    )
    language sql
    stable
    parallel safe
as
$$
    select
        (metrics).clearance,
        (metrics).path_loss_db,
        (metrics).distance_m,
        (metrics).d1_m,
        (metrics).d2_m
    from (
        select h3_visibility_clearance_compute_row(
            h3_a,
            h3_b,
            mast_height_src,
            mast_height_dst,
            frequency_hz
        ) as metrics
    ) q
    where q.metrics is not null;
$$;

-- create helper that returns minimum clearance between los and first fresnel zone, caching results
create or replace function h3_visibility_clearance(
    h3_a h3index,
    h3_b h3index,
    mast_height_src double precision,
    mast_height_dst double precision,
    frequency_hz double precision
)
    returns double precision
    language plpgsql
    cost 100000
    parallel restricted
as
$$
declare
    norm_src       h3index;
    norm_dst       h3index;
    norm_mast_src  double precision;
    norm_mast_dst  double precision;
    norm_frequency double precision;
    cached_clearance double precision;
    computed record;
begin
    if h3_a is null or h3_b is null then
        return null;
    end if;

    if h3_a <= h3_b then
        norm_src := h3_a;
        norm_dst := h3_b;
        norm_mast_src := mast_height_src;
        norm_mast_dst := mast_height_dst;
    else
        norm_src := h3_b;
        norm_dst := h3_a;
        norm_mast_src := mast_height_dst;
        norm_mast_dst := mast_height_src;
    end if;

    norm_frequency := frequency_hz;

    select mlc.clearance
    into cached_clearance
    from mesh_los_cache mlc
    where mlc.src_h3 = norm_src
      and mlc.dst_h3 = norm_dst
      and mlc.mast_height_src = norm_mast_src
      and mlc.mast_height_dst = norm_mast_dst
      and mlc.frequency_hz = norm_frequency;

    if cached_clearance is not null then
        return cached_clearance;
    end if;

    select *
    into computed
    from h3_visibility_clearance_compute(
        norm_src,
        norm_dst,
        norm_mast_src,
        norm_mast_dst,
        norm_frequency
    );

    if computed.clearance is null then
        return null;
    end if;

    if computed.distance_m > 0 then
        insert into mesh_los_cache (
            src_h3,
            dst_h3,
            mast_height_src,
            mast_height_dst,
            frequency_hz,
            distance_m,
            clearance,
            d1_m,
            d2_m,
            path_loss_db,
            computed_at
        )
        values (
            norm_src,
            norm_dst,
            norm_mast_src,
            norm_mast_dst,
            norm_frequency,
            computed.distance_m,
            computed.clearance,
            computed.d1_m,
            computed.d2_m,
            computed.path_loss_db,
            now()
        )
        on conflict on constraint mesh_los_cache_pkey do update
            set clearance = excluded.clearance,
                d1_m = excluded.d1_m,
                d2_m = excluded.d2_m,
                path_loss_db = excluded.path_loss_db,
                distance_m = excluded.distance_m,
                computed_at = now();
    end if;

    return computed.clearance;
end;
$$;

create or replace function h3_visibility_metrics(
    h3_a h3index,
    h3_b h3index,
    mast_height_src double precision,
    mast_height_dst double precision,
    frequency_hz double precision
)
    returns table (clearance double precision, path_loss_db double precision)
    language plpgsql
    parallel restricted
as
$$
declare
    norm_src       h3index;
    norm_dst       h3index;
    norm_mast_src  double precision;
    norm_mast_dst  double precision;
    norm_frequency double precision;
    cached_clearance double precision;
    cached_path_loss double precision;
    computed record;
begin
    if h3_a <= h3_b then
        norm_src := h3_a;
        norm_dst := h3_b;
        norm_mast_src := mast_height_src;
        norm_mast_dst := mast_height_dst;
    else
        norm_src := h3_b;
        norm_dst := h3_a;
        norm_mast_src := mast_height_dst;
        norm_mast_dst := mast_height_src;
    end if;

    norm_frequency := frequency_hz;

    select
        mlc.clearance,
        mlc.path_loss_db
    into cached_clearance, cached_path_loss
    from mesh_los_cache mlc
    where mlc.src_h3 = norm_src
      and mlc.dst_h3 = norm_dst
      and mlc.mast_height_src = norm_mast_src
      and mlc.mast_height_dst = norm_mast_dst
      and mlc.frequency_hz = norm_frequency;

    if cached_clearance is not null then
        return query select cached_clearance, cached_path_loss;
        return;
    end if;

    select *
    into computed
    from h3_visibility_clearance_compute(
        norm_src,
        norm_dst,
        norm_mast_src,
        norm_mast_dst,
        norm_frequency
    );

    if computed.clearance is null then
        return;
    end if;

    if computed.distance_m > 0 then
        insert into mesh_los_cache (
            src_h3,
            dst_h3,
            mast_height_src,
            mast_height_dst,
            frequency_hz,
            distance_m,
            clearance,
            d1_m,
            d2_m,
            path_loss_db,
            computed_at
        )
        values (
            norm_src,
            norm_dst,
            norm_mast_src,
            norm_mast_dst,
            norm_frequency,
            computed.distance_m,
            computed.clearance,
            computed.d1_m,
            computed.d2_m,
            computed.path_loss_db,
            now()
        )
        on conflict on constraint mesh_los_cache_pkey do update
            set clearance = excluded.clearance,
                d1_m = excluded.d1_m,
                d2_m = excluded.d2_m,
                path_loss_db = excluded.path_loss_db,
                distance_m = excluded.distance_m,
                computed_at = now();
    end if;

    return query select computed.clearance, computed.path_loss_db;
end;
$$;
