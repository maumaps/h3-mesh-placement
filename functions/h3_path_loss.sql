set client_min_messages = warning;

drop function if exists h3_path_loss(double precision, double precision, double precision, double precision, double precision);
-- Estimate total path loss (FSPL + single knife-edge diffraction) given distance, frequency, and Fresnel clearance
create or replace function h3_path_loss(
    distance_m double precision,
    frequency_hz double precision,
    clearance_m double precision,
    d1_m double precision default null,
    d2_m double precision default null
)
    returns double precision
    language plpgsql
    immutable
    parallel safe
as
$$
declare
    distance_km double precision;
    freq_mhz double precision;
    d1_km double precision;
    d2_km double precision;
    fspl double precision;
    r1 double precision;
    nu double precision;
    diffraction_loss double precision := 0;
begin
    if distance_m is null or distance_m <= 0 then
        raise exception 'distance_m must be positive in meters (got %)', distance_m;
    end if;

    if frequency_hz is null or frequency_hz <= 0 then
        raise exception 'frequency_hz must be positive in hertz (got %)', frequency_hz;
    end if;

    if clearance_m is null then
        raise exception 'clearance_m cannot be null when computing path loss';
    end if;

    distance_km := distance_m / 1000.0;
    freq_mhz := frequency_hz / 1000000.0;

    -- Free-space path loss in dB with distance in km and frequency in MHz.
    fspl := 20 * log10(distance_km) + 20 * log10(freq_mhz) + 32.44;

    if d1_m is null or d1_m <= 0 or d2_m is null or d2_m <= 0 then
        d1_km := distance_km / 2.0;
        d2_km := distance_km / 2.0;
    else
        d1_km := d1_m / 1000.0;
        d2_km := d2_m / 1000.0;
    end if;

    if d1_km <= 0 or d2_km <= 0 or (d1_km + d2_km) <= 0 then
        raise exception 'Invalid section distances (d1 %, d2 % kilometers)', d1_km, d2_km;
    end if;

    -- First Fresnel zone radius in meters, with distances in km and frequency in MHz.
    r1 := 17.32 * sqrt(d1_km * d2_km / (freq_mhz * (d1_km + d2_km)));

    if clearance_m < 0 then
        if r1 <= 0 then
            raise exception 'Fresnel radius must be positive when computing diffraction (got %)', r1;
        end if;

        nu := sqrt(2) * abs(clearance_m) / r1;

        if nu > 0 then
            diffraction_loss := 6.9 + 20 * log10(sqrt((nu - 0.1)^2 + 1) + nu - 0.1);
        end if;
    end if;

    return fspl + diffraction_loss;
end;
$$;
