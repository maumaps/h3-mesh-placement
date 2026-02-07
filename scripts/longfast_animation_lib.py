"""
Helper utilities for LongFast animation rendering.
These helpers are intentionally read-only for cache safety.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple

try:
    import mapnik
except ImportError as exc:
    raise SystemExit(
        "Mapnik is required. Install mapnik and the Python bindings."
    ) from exc

try:
    import psycopg2
except ImportError as exc:
    raise SystemExit(
        "psycopg2 is required for database access."
    ) from exc


@dataclass(frozen=True)
class DbConfig:
    """Container for PostGIS connection details."""

    dbname: str
    host: str
    port: int
    user: str
    password: str


@dataclass(frozen=True)
class RadioConfig:
    """LoRa radio parameters used for airtime estimation."""

    spreading_factor: int
    bandwidth_khz: int
    coding_rate: int
    payload_bytes: int
    preamble_symbols: int
    crc_enabled: int
    implicit_header: int


@dataclass(frozen=True)
class AnimationConfig:
    """Animation controls and tier thresholds."""

    fps: int
    hop_limit: int
    wave_min_s: float
    wave_max_s: float
    jitter_s: float
    tier_thresholds_db: List[float]


def open_readonly_connection(config: DbConfig):
    """Open a read-only connection to PostGIS."""

    conn_kwargs = {
        "dbname": config.dbname,
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
    }
    # Drop empty connection fields so libpq can fall back to defaults.
    if not config.dbname:
        conn_kwargs.pop("dbname")
    if not config.host:
        conn_kwargs.pop("host")
    if not config.user:
        conn_kwargs.pop("user")
    if not config.password:
        conn_kwargs.pop("password")
    conn = psycopg2.connect(**conn_kwargs)
    conn.set_session(readonly=True, autocommit=True)

    return conn


def fetch_bbox(conn) -> List[float]:
    """Fetch the georgia convex hull extent."""

    # Read the bounding box of the convex hull for framing.
    query = """
        select
            ST_XMin(geom) as minx,
            ST_YMin(geom) as miny,
            ST_XMax(geom) as maxx,
            ST_YMax(geom) as maxy
        from georgia_convex_hull;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        if not row:
            raise RuntimeError("georgia_convex_hull is empty.")

    return [float(row[0]), float(row[1]), float(row[2]), float(row[3])]


def fetch_seed_h3(conn, seed_name: str) -> str:
    """Fetch the seed H3 cell by name."""

    # Resolve the seed tower name from the initial nodes table.
    query = """
        select h3
        from mesh_initial_nodes_h3_r8
        where name ilike %s
        limit 1;
    """
    with conn.cursor() as cur:
        cur.execute(query, (f"%{seed_name}%",))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Seed name '{seed_name}' not found in mesh_initial_nodes_h3_r8.")

    return row[0]


def fetch_towers(
    conn,
    exclude_sources: Sequence[str] | None = None,
) -> Dict[str, int]:
    """Fetch tower H3 cells keyed by H3 string."""

    # Load tower H3 cells for adjacency and rendering.
    if exclude_sources:
        query = """
            select h3, tower_id
            from mesh_towers
            where not (source = any(%s::text[]));
        """
        params = (list(exclude_sources),)
    else:
        query = """
            select h3, tower_id
            from mesh_towers;
        """
        params = None
    with conn.cursor() as cur:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        rows = cur.fetchall()

    return {row[0]: int(row[1]) for row in rows}


def fetch_tower_adjacency(conn, towers: Iterable[str]) -> Dict[str, List[str]]:
    """Fetch tower-to-tower adjacency using cached LOS."""

    # Collect tower H3 values for matching.
    tower_list = list(towers)

    if not tower_list:
        return {}

    # Load cached tower-to-tower links from mesh_los_cache.
    query = """
        with towers as (
            select value::h3index as h3
            from unnest(%s::text[]) as value
        ),
        tower_pairs as (
            select
                c.src_h3,
                c.dst_h3
            from mesh_los_cache c
            join towers t_src on t_src.h3 = c.src_h3
            join towers t_dst on t_dst.h3 = c.dst_h3
            where c.clearance > 0
              and c.distance_m <= 70000
              and c.mast_height_src = 28
              and c.mast_height_dst = 28
              and c.frequency_hz = 868000000
            union all
            select
                c.dst_h3 as src_h3,
                c.src_h3 as dst_h3
            from mesh_los_cache c
            join towers t_src on t_src.h3 = c.dst_h3
            join towers t_dst on t_dst.h3 = c.src_h3
            where c.clearance > 0
              and c.distance_m <= 70000
              and c.mast_height_src = 28
              and c.mast_height_dst = 28
              and c.frequency_hz = 868000000
        )
        select src_h3, dst_h3
        from tower_pairs;
    """
    adjacency: Dict[str, List[str]] = {h3: [] for h3 in tower_list}

    with conn.cursor() as cur:
        cur.execute(query, (tower_list,))
        for src_h3, dst_h3 in cur.fetchall():
            adjacency[src_h3].append(dst_h3)
            adjacency[dst_h3].append(src_h3)

    return adjacency


def fetch_visible_cells(
    conn,
    towers: Iterable[str],
) -> Dict[str, List[Tuple[str, float]]]:
    """Fetch visible cells for each tower using cached LOS."""

    tower_list = list(towers)

    if not tower_list:
        return {}

    # Load cached tower-to-cell visibility pairs without mutating cache.
    query = """
        with towers as (
            select value::h3index as h3
            from unnest(%s::text[]) as value
        ),
        tower_pairs as (
            select
                c.src_h3,
                c.dst_h3,
                c.path_loss_db
            from mesh_los_cache c
            join towers t_src on t_src.h3 = c.src_h3
            left join towers t_dst on t_dst.h3 = c.dst_h3
            where c.clearance > 0
              and c.distance_m <= 70000
              and c.mast_height_src = 28
              and c.mast_height_dst = 28
              and c.frequency_hz = 868000000
              and t_dst.h3 is null
            union all
            select
                c.dst_h3 as src_h3,
                c.src_h3 as dst_h3,
                c.path_loss_db
            from mesh_los_cache c
            join towers t_src on t_src.h3 = c.dst_h3
            left join towers t_dst on t_dst.h3 = c.src_h3
            where c.clearance > 0
              and c.distance_m <= 70000
              and c.mast_height_src = 28
              and c.mast_height_dst = 28
              and c.frequency_hz = 868000000
              and t_dst.h3 is null
        )
        select src_h3, dst_h3, path_loss_db
        from tower_pairs;
    """

    visible: Dict[str, List[Tuple[str, float]]] = {h3: [] for h3 in tower_list}

    with conn.cursor() as cur:
        cur.execute(query, (tower_list,))
        for src_h3, dst_h3, path_loss_db in cur.fetchall():
            visible[src_h3].append((dst_h3, float(path_loss_db)))

    return visible


def fetch_tower_visibility(
    conn,
    towers: Iterable[str],
) -> Dict[str, List[Tuple[str, float]]]:
    """Fetch tower-to-tower visibility with path loss for each tower."""

    tower_list = list(towers)

    if not tower_list:
        return {}

    query = """
        with towers as (
            select value::h3index as h3
            from unnest(%s::text[]) as value
        ),
        tower_pairs as (
            select
                c.src_h3,
                c.dst_h3,
                c.path_loss_db
            from mesh_los_cache c
            join towers t_src on t_src.h3 = c.src_h3
            join towers t_dst on t_dst.h3 = c.dst_h3
            where c.clearance > 0
              and c.distance_m <= 70000
              and c.mast_height_src = 28
              and c.mast_height_dst = 28
              and c.frequency_hz = 868000000
            union all
            select
                c.dst_h3 as src_h3,
                c.src_h3 as dst_h3,
                c.path_loss_db
            from mesh_los_cache c
            join towers t_src on t_src.h3 = c.dst_h3
            join towers t_dst on t_dst.h3 = c.src_h3
            where c.clearance > 0
              and c.distance_m <= 70000
              and c.mast_height_src = 28
              and c.mast_height_dst = 28
              and c.frequency_hz = 868000000
        )
        select src_h3, dst_h3, path_loss_db
        from tower_pairs;
    """

    visibility: Dict[str, List[Tuple[str, float]]] = {h3: [] for h3 in tower_list}

    with conn.cursor() as cur:
        cur.execute(query, (tower_list,))
        for src_h3, dst_h3, path_loss_db in cur.fetchall():
            visibility[src_h3].append((dst_h3, float(path_loss_db)))

    return visibility


def fetch_population(conn, cells: Iterable[str]) -> Dict[str, float]:
    """Fetch population counts for the specified H3 cells."""

    cell_list = list(cells)
    if not cell_list:
        return {}

    # Aggregate population for the requested H3 cells.
    query = """
        with cells as (
            select value::h3index as h3
            from unnest(%s::text[]) as value
        )
        select p.h3, p.population
        from population_h3_r8 p
        join cells c on c.h3 = p.h3;
    """

    population: Dict[str, float] = {}

    with conn.cursor() as cur:
        cur.execute(query, (cell_list,))
        for h3_value, population_value in cur.fetchall():
            population[h3_value] = float(population_value)

    return population


def estimate_airtime_seconds(radio: RadioConfig) -> float:
    """Estimate LoRa airtime in seconds using Semtech's formula."""

    # Convert bandwidth to Hz and compute symbol duration.
    bandwidth_hz = radio.bandwidth_khz * 1000
    symbol_duration = (2**radio.spreading_factor) / bandwidth_hz

    # Low data rate optimization is enabled for long symbol times.
    low_data_rate_opt = 1 if symbol_duration > 0.016 else 0

    # Convert CR 4/5..4/8 into the LoRa formula coding rate value.
    cr_value = max(min(radio.coding_rate, 8), 5) - 4

    # Compute payload symbols following the LoRa airtime formula.
    numerator = (
        8 * radio.payload_bytes
        - 4 * radio.spreading_factor
        + 28
        + 16 * radio.crc_enabled
        - 20 * radio.implicit_header
    )
    denominator = 4 * (radio.spreading_factor - 2 * low_data_rate_opt)
    payload_term = max(math.ceil(numerator / denominator) * (cr_value + 4), 0)
    payload_symbols = 8 + payload_term

    preamble_symbols = radio.preamble_symbols + 4.25

    return (preamble_symbols + payload_symbols) * symbol_duration


def build_tier_thresholds(conn, tier_thresholds_db: str) -> List[float]:
    """Build path loss tier thresholds."""

    if tier_thresholds_db:
        return [float(value) for value in tier_thresholds_db.split(",")]

    # Compute quantile-based thresholds from cached path loss values.
    query = """
        select percentile_cont(array[0.15, 0.3, 0.45, 0.6, 0.75, 0.9]) within group (order by path_loss_db)
        from mesh_los_cache
        where clearance > 0
          and distance_m <= 70000
          and mast_height_src = 28
          and mast_height_dst = 28
          and frequency_hz = 868000000;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        if not row or row[0] is None:
            return [112.0, 118.0, 124.0, 130.0, 136.0, 142.0]

    return [float(value) for value in row[0]]


def assign_tier(path_loss_db: float, thresholds: Sequence[float]) -> int:
    """Assign a tier index based on path loss thresholds."""

    for index, threshold in enumerate(thresholds):
        if path_loss_db <= threshold:
            return index

    return len(thresholds)


def build_schedule(
    waves: List[List[str]],
    airtime_s: float,
    config: AnimationConfig,
) -> List[Tuple[str, float, float, int]]:
    """Build a transmission schedule with airtime and jitter."""

    schedule: List[Tuple[str, float, float, int]] = []
    current_time = 0.0

    for hop_index, wave in enumerate(waves, start=1):
        wave_start = current_time
        wave_time = 0.0

        for tower_h3 in wave:
            duration = airtime_s + random.uniform(0.0, config.jitter_s)
            start_time = current_time
            end_time = start_time + duration
            schedule.append((tower_h3, start_time, end_time, hop_index))
            current_time = end_time
            wave_time = current_time - wave_start

        if wave_time < config.wave_min_s:
            current_time += config.wave_min_s - wave_time
        elif wave_time < config.wave_max_s:
            current_time += random.uniform(0.0, config.wave_max_s - wave_time)

    return schedule


def build_waves(
    seed_h3: str,
    adjacency: Dict[str, List[str]],
    hop_limit: int,
) -> List[List[str]]:
    """Build BFS waves up to the hop limit."""

    waves: List[List[str]] = []
    visited = {seed_h3}
    current_wave = [seed_h3]

    # `hop_limit` is the maximum hop index to include, where hop 0 is the seed itself.
    # That means we want up to `hop_limit + 1` waves (0..hop_limit).
    for _ in range(hop_limit + 1):
        waves.append(current_wave)
        next_wave: List[str] = []

        for tower_h3 in current_wave:
            for neighbor in adjacency.get(tower_h3, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    next_wave.append(neighbor)

        if not next_wave:
            break

        current_wave = next_wave

    return waves


def build_h3_array_literal(values: Sequence[str]) -> str:
    """Build a safe SQL array literal for H3 text values."""

    if not values:
        return "array[]::text[]"

    escaped = [value.replace("'", "''") for value in values]
    items = ",".join(f"'{value}'" for value in escaped)

    return f"array[{items}]::text[]"


def build_h3_polygon_layer_sql(h3_values: Sequence[str]) -> str:
    """Build a SQL statement for an H3 polygon geometry layer."""

    array_literal = build_h3_array_literal(h3_values)
    return f"""
        select
            h3_cell_to_boundary_geometry(h3::h3index) as geom
        from unnest({array_literal}) as h3
    """


def build_h3_point_layer_sql(h3_values: Sequence[str]) -> str:
    """Build a SQL statement for an H3 point geometry layer."""

    array_literal = build_h3_array_literal(h3_values)
    return f"""
        select
            (h3::h3index)::geometry as geom
        from unnest({array_literal}) as h3
    """


def build_h3_link_layer_sql(pairs: Sequence[Tuple[str, str]]) -> str:
    """Build a SQL statement for line geometries between H3 point pairs."""

    if not pairs:
        return """
            select
                null::geometry as geom
            where false
        """

    values = ", ".join(f"('{src}','{dst}')" for src, dst in pairs)
    return f"""
        select
            ST_MakeLine(src_h3::h3index::geometry, dst_h3::h3index::geometry) as geom
        from (values {values}) as pairs(src_h3, dst_h3)
    """


def make_postgis_layer(
    config: DbConfig,
    name: str,
    style: str,
    table: str,
    geometry_field: str = "geom",
    extent: str | None = None,
) -> mapnik.Layer:
    """Create a PostGIS-backed Mapnik layer."""

    datasource_kwargs = {
        "dbname": config.dbname,
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "table": table,
        "geometry_field": geometry_field,
        "srid": 4326,
    }
    if not config.dbname:
        datasource_kwargs.pop("dbname")
    if not config.host:
        datasource_kwargs.pop("host")
    if not config.user:
        datasource_kwargs.pop("user")
    if not config.password:
        datasource_kwargs.pop("password")
    if extent:
        datasource_kwargs["extent"] = extent

    datasource = mapnik.PostGIS(**datasource_kwargs)

    layer = mapnik.Layer(name)
    layer.srs = "+proj=longlat +datum=WGS84 +no_defs"
    layer.datasource = datasource
    layer.styles.append(style)

    return layer


def build_label_layer(
    label_text: str,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    width_px: int,
    height_px: int,
    srs: str,
) -> mapnik.Layer:
    """Build a memory layer for the frame label."""

    # Prepare a memory datasource to hold the label point.
    context = mapnik.Context()
    context.push("label")
    ds = mapnik.MemoryDatasource()

    # Place labels inside the panel with pixel-based padding for alignment.
    span_x = maxx - minx
    span_y = maxy - miny
    margin_px = 48
    pad_px = 14
    panel_px = int(min(width_px, height_px) * 0.26)
    panel_width = span_x * (panel_px / width_px)
    panel_height = span_y * (panel_px / height_px)
    margin_x = span_x * (margin_px / width_px)
    margin_y = span_y * (margin_px / height_px)
    pad_x = span_x * (pad_px / width_px)
    pad_y = span_y * (pad_px / height_px)
    panel_left = minx + margin_x
    panel_bottom = miny + margin_y
    panel_top = panel_bottom + panel_height
    x = panel_left + pad_x
    y = panel_top - pad_y

    feature = mapnik.Feature(context, 1)
    feature.geometry = mapnik.Geometry.from_wkt(f"POINT({x} {y})")
    feature["label"] = label_text
    ds.add_feature(feature)

    layer = mapnik.Layer("frame_label")
    layer.srs = srs
    layer.datasource = ds
    layer.styles.append("frame-label")

    return layer


def build_label_panel_layer(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    width_px: int,
    height_px: int,
    srs: str,
) -> mapnik.Layer:
    """Build a memory layer for the label background panel."""

    # Configure a memory datasource for the label panel polygon.
    context = mapnik.Context()
    ds = mapnik.MemoryDatasource()

    # Size and place a square panel using pixel-based dimensions.
    span_x = maxx - minx
    span_y = maxy - miny
    margin_px = 48
    panel_px = int(min(width_px, height_px) * 0.26)
    panel_width = span_x * (panel_px / width_px)
    panel_height = span_y * (panel_px / height_px)
    margin_x = span_x * (margin_px / width_px)
    margin_y = span_y * (margin_px / height_px)
    panel_left = minx + margin_x
    panel_bottom = miny + margin_y
    panel_right = panel_left + panel_width
    panel_top = panel_bottom + panel_height

    polygon_wkt = (
        "POLYGON(("
        f"{panel_left} {panel_bottom}, "
        f"{panel_right} {panel_bottom}, "
        f"{panel_right} {panel_top}, "
        f"{panel_left} {panel_top}, "
        f"{panel_left} {panel_bottom}"
        "))"
    )

    feature = mapnik.Feature(context, 1)
    feature.geometry = mapnik.Geometry.from_wkt(polygon_wkt)
    ds.add_feature(feature)

    layer = mapnik.Layer("frame_label_panel")
    layer.srs = srs
    layer.datasource = ds
    layer.styles.append("frame-label-panel")

    return layer
