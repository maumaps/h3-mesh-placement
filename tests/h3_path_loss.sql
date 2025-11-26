set client_min_messages = warning;

-- Validate h3_path_loss() against hand calculations.
begin;

do
$$
declare
    expected_fspl double precision;
    actual_fspl double precision;
    expected_diffraction double precision;
    actual_diffraction double precision;
    tolerance constant double precision := 1e-6;
begin
    -- Positive clearance: should match FSPL only (1 km, 868 MHz).
    expected_fspl := 20 * log10(1.0) + 20 * log10(868.0) + 32.44;
    actual_fspl := h3_path_loss(1000, 868e6, 5);

    if abs(actual_fspl - expected_fspl) > tolerance then
        raise exception 'FSPL mismatch: expected % dB, got % dB for 1km/868MHz LOS',
            expected_fspl, actual_fspl;
    end if;

    -- Negative clearance: ensure diffraction adds loss (56 km, -2 m clearance).
    expected_fspl := 20 * log10(56.0) + 20 * log10(868.0) + 32.44;

    -- Midpoint assumption -> r1
    expected_diffraction := 6.9 + 20 * log10(
        sqrt(
            (sqrt(2) * 2.0 / (17.32 * sqrt(28.0 * 28.0 / (868.0 * 56.0))) - 0.1)^2 + 1
        ) + sqrt(2) * 2.0 / (17.32 * sqrt(28.0 * 28.0 / (868.0 * 56.0))) - 0.1
    );

    actual_diffraction := h3_path_loss(56000, 868e6, -2) - expected_fspl;

    if actual_diffraction <= 0 then
        raise exception 'Expected positive diffraction loss for obstructed path, got % dB',
            actual_diffraction;
    end if;
end;
$$;

rollback;
