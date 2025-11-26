set client_min_messages = warning;

drop function if exists h3_los_between_cells(h3index, h3index);

create or replace function h3_los_between_cells(h3_a h3index, h3_b h3index)
    returns boolean
    language plpgsql
    cost 100000
as
$$
declare
    norm_src      h3index;
    norm_dst      h3index;
    distance_m    double precision;
    clearance     double precision;
    max_distance  constant double precision := 70000;      -- hard cut at 70 km
    default_mast_height constant double precision := 28;   -- tower height above ground, meters
    default_frequency_hz constant double precision := 868e6; -- assume 868 MHz backhaul planning
begin
    if h3_a is null or h3_b is null then
        return false;
    end if;

    if h3_a = h3_b then
        return true;
    end if;

    norm_src := least(h3_a, h3_b);
    norm_dst := greatest(h3_a, h3_b);

    distance_m := ST_Distance(norm_src::geography, norm_dst::geography);

    if distance_m > max_distance then
        return false;
    end if;

    clearance := h3_visibility_clearance(
        norm_src,
        norm_dst,
        default_mast_height,
        default_mast_height,
        default_frequency_hz
    );

    return clearance > 0;
end;
$$;
