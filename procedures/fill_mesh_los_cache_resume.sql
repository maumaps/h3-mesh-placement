-- Resume is now handled by the same make target as the normal fill:
-- `make db/procedure/fill_mesh_los_cache`
--
-- The make recipe runs prepare only when needed, then commits one batch per
-- psql invocation, then finalizes the route graph after the queue is empty.
do
$$
begin
    raise exception 'Run `make db/procedure/fill_mesh_los_cache`; resume now happens through committed batch scripts instead of a single long DO block';
end;
$$;
