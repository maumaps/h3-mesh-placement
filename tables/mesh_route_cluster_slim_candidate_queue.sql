set client_min_messages = warning;

-- Create the shared candidate queue drained by cluster-slim workers with SKIP LOCKED.
create table if not exists mesh_route_cluster_slim_candidate_queue (
    iteration_label integer not null,
    pair_id integer not null,
    source_id integer not null,
    target_id integer not null,
    source_h3 h3index not null,
    target_h3 h3index not null,
    cluster_hops integer not null,
    distance_m double precision not null,
    average_hop_length double precision not null,
    source_node_id integer not null,
    target_node_id integer not null,
    seed_endpoint_count integer not null,
    status text not null default 'queued',
    worker_index integer,
    claimed_at timestamptz,
    attempted_at timestamptz,
    reason text,
    primary key (iteration_label, source_id, target_id),
    unique (iteration_label, pair_id),
    check (status in (
        'queued',
        'routing',
        'claim_conflict',
        'exact_claim_conflict',
        'failed',
        'completed'
    ))
);

comment on table mesh_route_cluster_slim_candidate_queue is
    'Ranked over-limit visibility pairs for one cluster-slim iteration, claimed by workers before expensive pgRouting.';

comment on column mesh_route_cluster_slim_candidate_queue.status is
    'Iteration-local candidate state; claim conflicts are retried by rebuilding the queue in the next outer iteration.';

create index if not exists mesh_route_cluster_slim_candidate_queue_status_idx
    on mesh_route_cluster_slim_candidate_queue (iteration_label, status, pair_id);
