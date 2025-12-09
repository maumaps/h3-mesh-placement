set client_min_messages = warning;

-- Track per-run outcomes for cluster slim candidates so repeated iterations skip resolved pairs.
create table if not exists mesh_route_cluster_slim_failures (
    source_id integer not null,
    target_id integer not null,
    status text not null check (status in ('failed', 'completed')),
    reason text,
    last_attempt_at timestamptz not null default now(),
    attempt_count integer not null default 1,
    primary key (source_id, target_id)
);
