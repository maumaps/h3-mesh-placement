set client_min_messages = warning;

drop type if exists h3_visibility_metrics cascade;
-- Single-row LOS metric payload so hot batch code can call a scalar helper instead of a set-returning function.
create type h3_visibility_metrics as (
    clearance double precision,
    path_loss_db double precision,
    distance_m double precision,
    d1_m double precision,
    d2_m double precision
);
