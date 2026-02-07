#!/usr/bin/env python3
"""
Render a static Mapnik PNG from PostGIS data.
This script is read-only and does not modify cached LOS data.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import List

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


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the renderer."""

    parser = argparse.ArgumentParser(
        description="Render a static Mapnik PNG of the mesh surface."
    )
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", ""))
    parser.add_argument("--host", default=os.getenv("PGHOST", ""))
    parser.add_argument("--port", default=int(os.getenv("PGPORT", "5432")))
    parser.add_argument("--user", default=os.getenv("PGUSER", ""))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--style", default="mapnik/styles/mesh_style.xml")
    parser.add_argument("--output", default="data/out/visuals/mesh_surface.png")
    parser.add_argument("--width", type=int, default=1920)
    parser.add_argument("--height", type=int, default=1080)

    return parser.parse_args()


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


def expand_bbox(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    padding_ratio: float,
) -> List[float]:
    """Expand a lon/lat bounding box by a padding ratio on each side."""

    span_x = maxx - minx
    span_y = maxy - miny
    pad_x = span_x * padding_ratio
    pad_y = span_y * padding_ratio

    # Clamp to valid lon/lat ranges to avoid invalid projection bounds.
    padded_minx = max(-180.0, minx - pad_x)
    padded_maxx = min(180.0, maxx + pad_x)
    padded_miny = max(-90.0, miny - pad_y)
    padded_maxy = min(90.0, maxy + pad_y)

    return [padded_minx, padded_miny, padded_maxx, padded_maxy]


def fetch_population_breaks(conn) -> List[float]:
    """Fetch population quantile breaks for color bucketing."""

    # Compute population quantiles for density bucket coloring.
    query = """
        select percentile_cont(array[0.2, 0.4, 0.6, 0.8]) within group (order by population)
        from population_h3_r8
        where population > 0;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        if not row or row[0] is None:
            return [0, 0, 0, 0]

    return [float(value) for value in row[0]]


def build_population_sql(breaks: List[float]) -> str:
    """Build SQL for the population density layer."""

    b1, b2, b3, b4 = breaks
    return f"""
        select
            h3_cell_to_boundary_geometry(h3) as geom,
            population,
            case
                when population <= {b1} then '#0b1a2d'
                when population <= {b2} then '#13263b'
                when population <= {b3} then '#1a3148'
                when population <= {b4} then '#24425c'
                else '#2f5673'
            end as density_color
        from population_h3_r8
        where population > 0
    """


def build_country_border_sql() -> str:
    """Build SQL for the Georgia-Armenia border line."""

    return """
        with admin_polygons as (
            select
                lower(
                    coalesce(
                        nullif(tags ->> 'name:en', ''),
                        nullif(tags ->> 'short_name', ''),
                        nullif(tags ->> 'int_name', ''),
                        nullif(tags ->> 'name', '')
                    )
                ) as normalized_name,
                tags,
                ST_Multi(geog::geometry) as geom
            from osm_for_mesh_placement
            where tags ? 'boundary'
              and tags ->> 'boundary' = 'administrative'
              and tags ->> 'admin_level' = '2'
              and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon')
        ),
        georgia as (
            select geom
            from admin_polygons
            where normalized_name in (
                'georgia',
                'sakartvelo',
                'republic of georgia'
            )
        ),
        armenia as (
            select geom
            from admin_polygons
            where lower(coalesce(tags ->> 'ISO3166-1:alpha2', '')) = 'am'
               or lower(coalesce(tags ->> 'int_name', '')) = 'armenia'
               or lower(coalesce(tags ->> 'name:en', '')) = 'armenia'
               or tags ->> 'wikidata' = 'Q399'
        ),
        georgia_boundary as (
            select ST_Boundary(ST_UnaryUnion(geom)) as geom
            from georgia
        ),
        armenia_boundary as (
            select ST_Boundary(ST_UnaryUnion(geom)) as geom
            from armenia
        )
        select ST_Intersection(gb.geom, ab.geom) as geom
        from georgia_boundary gb
        join armenia_boundary ab on true
        where ST_Intersection(gb.geom, ab.geom) is not null
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

    # Build a PostGIS datasource using the provided SQL table or subquery.
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


def main() -> None:
    """Render the static mesh surface visualization."""

    args = parse_args()

    config = DbConfig(
        dbname=args.dbname,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    print("Opening read-only database connection.")
    conn = open_readonly_connection(config)

    print("Loading Mapnik style.")
    # Load map styles from the base Mapnik XML.
    map_obj = mapnik.Map(args.width, args.height)
    mapnik.load_map(map_obj, args.style)
    map_obj.srs = "+proj=utm +zone=38 +datum=WGS84 +units=m +no_defs"

    print("Fetching map extent.")
    # Frame the map to the convex hull extent.
    try:
        minx, miny, maxx, maxy = fetch_bbox(conn)
    except Exception as exc:  # pragma: no cover - runtime environment dependent
        raise RuntimeError(
            "Failed to read georgia_convex_hull. Set PGDATABASE/--dbname to the pipeline database."
        ) from exc
    minx, miny, maxx, maxy = expand_bbox(minx, miny, maxx, maxy, 0.04)
    extent = f"{minx},{miny},{maxx},{maxy}"
    src_proj = mapnik.Projection("+proj=longlat +datum=WGS84 +no_defs")
    dst_proj = mapnik.Projection(map_obj.srs)
    transform = mapnik.ProjTransform(src_proj, dst_proj)
    bbox_4326 = mapnik.Box2d(minx, miny, maxx, maxy)
    bbox_3857 = transform.forward(bbox_4326)
    map_obj.zoom_to_box(bbox_3857)

    print("Building population density layer.")
    # Build the population density layer.
    population_breaks = fetch_population_breaks(conn)
    population_sql = build_population_sql(population_breaks)
    border_sql = build_country_border_sql()

    map_obj.layers.append(
        make_postgis_layer(
            config,
            name="population_density",
            style="population-density",
            table=f"({population_sql}) as population_density",
            extent=extent,
        )
    )

    print("Adding boundary and tower layers.")
    # Add a boundary outline for context.
    map_obj.layers.append(
        make_postgis_layer(
            config,
            name="georgia_boundary",
            style="boundary-outline",
            table="georgia_boundary",
            extent=extent,
        )
    )
    map_obj.layers.append(
        make_postgis_layer(
            config,
            name="georgia_armenia_border",
            style="country-border",
            table=f"({border_sql}) as georgia_armenia_border",
            extent=extent,
        )
    )

    # Add towers for context with per-source styling.
    map_obj.layers.append(
        make_postgis_layer(
            config,
            name="mesh_towers",
            style="tower-points",
            table="(select h3, source, h3::geometry as geom from mesh_towers) as towers",
            extent=extent,
        )
    )

    print("Rendering static map.")
    # Render to file.
    mapnik.render_to_file(map_obj, args.output, "png")

    print(f"Rendered {args.output}")


if __name__ == "__main__":
    main()
