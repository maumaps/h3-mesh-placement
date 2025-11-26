set client_min_messages = warning;

drop function if exists h3_visibility_clearance_compute(h3index, h3index, double precision, double precision, double precision);
drop function if exists h3_visibility_clearance(h3index, h3index, double precision, double precision, double precision);

-- Pure computation helper that evaluates Fresnel clearance and path loss without touching the cache
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
    src_height     double precision;
    dst_height     double precision;
    total_distance double precision;
    line_geom      geometry;
    doc_samples    integer;
    clearance_val  double precision;
    worst_d1       double precision;
    worst_d2       double precision;
    effective_radius constant double precision := (4.0 / 3.0) * 6371000.0;
    speed_of_light   constant double precision := 299792458.0;
    wavelength     double precision;
begin
    if h3_a is null or h3_b is null then
        return;
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

    select ele
    into src_ele
    from mesh_surface_h3_r8
    where h3 = norm_src;

    if not found or src_ele is null then
        raise exception 'missing elevation for % when computing clearance', norm_src::text;
    end if;

    select ele
    into dst_ele
    from mesh_surface_h3_r8
    where h3 = norm_dst;

    if not found or dst_ele is null then
        raise exception 'missing elevation for % when computing clearance', norm_dst::text;
    end if;

    src_height := src_ele + norm_mast_src;
    dst_height := dst_ele + norm_mast_dst;

    if norm_src = norm_dst then
        return query
        select
            least(src_height - src_ele, dst_height - dst_ele),
            null::double precision,
            0::double precision,
            0::double precision,
            0::double precision;
    end if;

    total_distance := ST_Distance(norm_src::geography, norm_dst::geography);

    if total_distance <= 0 then
        return;
    end if;

    wavelength := speed_of_light / norm_frequency;
    line_geom := ST_MakeLine(norm_src::geometry, norm_dst::geometry);

    with path as (
        select h3_grid_path_cells(norm_src, norm_dst) as h3
    ),
    samples as (
        select
            p.h3,
            greatest(
                least(
                    ST_LineLocatePoint(line_geom, ST_PointOnSurface(ms.geom))::double precision,
                    1
                ),
                0
            ) as frac,
            ms.ele
        from path p
        join mesh_surface_h3_r8 ms
          on ms.h3 = p.h3
    ),
    calc as (
        select
            s.h3,
            s.frac,
            s.ele,
            total_distance * s.frac as d1,
            total_distance * (1 - s.frac) as d2,
            (src_height + (dst_height - src_height) * s.frac)
            - (
                s.ele
                + ((total_distance * s.frac) * (total_distance * (1 - s.frac))) / (2 * effective_radius)
                + sqrt(wavelength * (total_distance * s.frac) * (total_distance * (1 - s.frac)) / total_distance)
            ) as clearance_value
        from samples s
    ),
    stats as (
        select count(*)::integer as sample_count
        from calc
    ),
    worst as (
        select
            c.clearance_value,
            c.d1,
            c.d2
        from calc c
        order by c.clearance_value asc
        limit 1
    )
    select
        stats.sample_count,
        worst.clearance_value,
        worst.d1,
        worst.d2
    into doc_samples, clearance_val, worst_d1, worst_d2
    from stats, worst;

    if doc_samples is null or doc_samples = 0 then
        raise exception 'no samples available between % and % for clearance computation', norm_src::text, norm_dst::text;
    end if;

    if clearance_val is null then
        raise exception 'failed to compute clearance between % and % due to missing data', norm_src::text, norm_dst::text;
    end if;

    if worst_d1 is null or worst_d2 is null then
        worst_d1 := total_distance / 2;
        worst_d2 := total_distance / 2;
    end if;

    return query
    select
        clearance_val,
        h3_path_loss(total_distance, norm_frequency, clearance_val, worst_d1, worst_d2),
        total_distance,
        worst_d1,
        worst_d2;
end;
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
