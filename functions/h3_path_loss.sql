set client_min_messages = warning;

drop function if exists h3_path_loss(double precision, double precision, double precision, double precision, double precision);
-- Estimate total path loss (FSPL + single knife-edge diffraction) given distance, frequency, and Fresnel clearance.
create or replace function h3_path_loss(
    distance_m double precision,
    frequency_hz double precision,
    clearance_m double precision,
    d1_m double precision default null,
    d2_m double precision default null
)
    returns double precision
    language sql
    immutable
    parallel safe
as
$$
    -- Validate input once, normalize section lengths, then compute FSPL and optional diffraction loss.
    with validated as (
        select
            case
                when distance_m is null or distance_m <= 0 then null
                else distance_m / 1000.0
            end as distance_km,
            case
                when frequency_hz is null or frequency_hz <= 0 then null
                else frequency_hz / 1000000.0
            end as freq_mhz,
            clearance_m,
            case
                when d1_m is null or d1_m <= 0 or d2_m is null or d2_m <= 0 then distance_m / 2000.0
                else d1_m / 1000.0
            end as d1_km,
            case
                when d1_m is null or d1_m <= 0 or d2_m is null or d2_m <= 0 then distance_m / 2000.0
                else d2_m / 1000.0
            end as d2_km
    ),
    base as (
        select
            v.distance_km,
            v.freq_mhz,
            v.clearance_m,
            v.d1_km,
            v.d2_km,
            (20 * log(v.distance_km) + 20 * log(v.freq_mhz) + 32.44) as fspl,
            17.32 * sqrt(v.d1_km * v.d2_km / (v.freq_mhz * (v.d1_km + v.d2_km))) as r1
        from validated v
        where v.distance_km is not null
          and v.freq_mhz is not null
          and v.clearance_m is not null
          and v.d1_km > 0
          and v.d2_km > 0
          and (v.d1_km + v.d2_km) > 0
    )
    select
        case
            when b.clearance_m < 0 and b.r1 > 0 then
                b.fspl + 6.9 + 20 * log(sqrt((sqrt(2) * abs(b.clearance_m) / b.r1 - 0.1)^2 + 1) + sqrt(2) * abs(b.clearance_m) / b.r1 - 0.1)
            else
                b.fspl
        end
    from base b;
$$;
