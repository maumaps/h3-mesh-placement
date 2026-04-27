set client_min_messages = warning;

-- Create the shared coarse H3 claim table used by parallel cluster-slim workers.
create table if not exists mesh_route_cluster_slim_claims (
    iteration_label integer not null,
    claim_h3 h3index not null,
    source_id integer not null,
    target_id integer not null,
    worker_index integer not null,
    claim_stage text not null,
    claimed_at timestamptz not null default clock_timestamp(),
    primary key (iteration_label, claim_h3),
    check (claim_stage in ('approx', 'exact'))
);

comment on table mesh_route_cluster_slim_claims is
    'Coarse H3 corridor locks for cluster-slim workers; rows prevent parallel pgRouting work on mutually interfering LOS corridors.';

comment on column mesh_route_cluster_slim_claims.claim_h3 is
    'Coarse H3 parent cell reserved before or after routing a candidate corridor.';
