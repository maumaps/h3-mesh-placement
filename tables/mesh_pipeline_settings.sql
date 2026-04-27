set client_min_messages = warning;

-- Create the single user-editable pipeline configuration table.
-- Edit the values below, then run `make db/table/mesh_pipeline_settings`
-- before restarting placement stages. Values stay text so Makefile shell
-- wrappers and SQL procedures can read the same config without migrations.
create table if not exists mesh_pipeline_settings (
    setting text primary key,
    value text not null,
    updated_at timestamptz not null default now()
);

-- Ensure boolean-typed settings only accept 'true' or 'false'.
alter table mesh_pipeline_settings
    drop constraint if exists mesh_pipeline_settings_bool_check;
alter table mesh_pipeline_settings
    add constraint mesh_pipeline_settings_bool_check check (
        setting not in (
            'enable_coarse', 'enable_population', 'enable_population_anchor_contract',
            'enable_generated_pair_contract', 'enable_route_segment_reroute',
            'enable_route_bridge', 'enable_cluster_slim', 'enable_greedy', 'enable_wiggle',
            'enable_install_priority_plan'
        )
        or value in ('true', 'false')
    );

-- Drop renamed settings so the user-facing config table does not keep stale aliases.
delete from mesh_pipeline_settings
where setting in ('placement_dedup_distance_m', 'population_cluster_factor');

-- Store all pipeline constants and placement stage switches in one place.
insert into mesh_pipeline_settings (setting, value)
values
    ('h3_res', '8'),
    ('max_los_distance_m', '100000'),
    ('refresh_radius_m', '100000'),
    ('min_tower_separation_m', '0'),
    ('generated_tower_merge_distance_m', '10000'),
    ('mast_height_m', '28'),
    ('frequency_hz', '868000000'),
    ('los_batch_limit', '50000'),
    ('los_parallel_jobs', '0'),
    ('enable_coarse', 'false'),
    ('coarse_resolution', '4'),
    ('enable_population', 'true'),
    ('enable_population_anchor_contract', 'true'),
    ('population_anchor_contract_distance_m', '0'),
    ('enable_generated_pair_contract', 'true'),
    ('enable_route_segment_reroute', 'true'),
    ('route_segment_reroute_candidate_limit', '512'),
    ('route_segment_reroute_max_moves', '32'),
    ('population_anchor_min_count', '7'),
    ('population_anchor_max_count', '7'),
    ('population_anchor_source', 'population'),
    ('population_building_weight', '1.0'),
    ('population_nearby_population_weight', '1.0'),
    ('population_cluster_weight_metric', 'population'),
    ('population_existing_anchor_weight', '1000000'),
    ('population_anchor_cluster_oversampling', '2'),
    ('enable_route_bridge', 'true'),
    ('enable_cluster_slim', 'true'),
    ('cluster_slim_iterations', '0'),
    ('cluster_slim_claim_resolution', '4'),
    ('cluster_slim_claim_disk', '1'),
    ('enable_greedy', 'false'),
    ('greedy_iterations', '100'),
    ('enable_wiggle', 'true'),
    ('wiggle_iterations', '0'),
    ('wiggle_parallel_workers', '8'),
    ('wiggle_candidate_limit', '256'),
    ('enable_install_priority_plan', 'true'),
    ('install_priority_phase1_cost', 'hop_count'),
    ('install_priority_phase2_metric', 'population')
on conflict (setting) do update
set value = excluded.value,
    updated_at = now();
