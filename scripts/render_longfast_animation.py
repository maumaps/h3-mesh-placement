#!/usr/bin/env python3
"""
Render a LongFast-style propagation animation using cached LOS data.
This script is read-only and never recomputes or mutates LOS caches.
"""

from __future__ import annotations

import argparse
import os
import random
import re
import shutil
import subprocess
from dataclasses import dataclass

import mapnik

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:  # pragma: no cover - optional dependency
    Image = None
    ImageDraw = None
    ImageFont = None


from longfast_animation_lib import (
    AnimationConfig,
    DbConfig,
    RadioConfig,
    assign_tier,
    build_h3_link_layer_sql,
    build_h3_point_layer_sql,
    build_h3_polygon_layer_sql,
    build_tier_thresholds,
    build_waves,
    estimate_airtime_seconds,
    fetch_bbox,
    fetch_population,
    fetch_seed_h3,
    fetch_tower_visibility,
    fetch_towers,
    fetch_visible_cells,
    make_postgis_layer,
    open_readonly_connection,
)


@dataclass(frozen=True)
class LegendTheme:
    """Constants for the Pillow-rendered legend panel."""

    margin_px: int = 44

    panel_padding_px: int = 12
    panel_corner_radius_px: int = 10
    panel_min_width_px: int = 260
    panel_target_width_ratio: float = 0.24

    # Layout safety: keep descenders inside the box.
    bottom_descender_guard_px: int = 14

    # Base font sizes (may be scaled down if content does not fit).
    title_font_px: int = 28
    body_font_px: int = 22

    # Colors (RGBA)
    panel_fill_rgba: tuple[int, int, int, int] = (11, 18, 32, 210)
    panel_outline_rgba: tuple[int, int, int, int] = (59, 74, 99, 220)
    text_primary_rgba: tuple[int, int, int, int] = (248, 250, 252, 255)
    text_muted_rgba: tuple[int, int, int, int] = (148, 163, 184, 255)
    text_body_rgba: tuple[int, int, int, int] = (203, 213, 225, 255)
    text_hop_label_rgba: tuple[int, int, int, int] = (160, 174, 192, 255)
    text_hop_value_rgba: tuple[int, int, int, int] = (226, 232, 240, 255)

    # Spacing defaults (in pixels; some are computed from measured font metrics).
    base_gap_px: int = 4
    section_multiplier: float = 3.0

    # Swatch tuning
    swatch_min_px: int = 10
    swatch_gap_px: int = 8
    glow_outline_rgba: tuple[int, int, int, int] = (254, 240, 138, 220)
    glow_fill_rgba: tuple[int, int, int, int] = (249, 115, 22, 200)
    glow_outline_width_ratio: float = 0.18


@dataclass(frozen=True)
class WordmarkTheme:
    """Sizing/placement for the bottom-right wordmark overlay."""

    max_width_ratio: float = 0.18
    max_height_ratio: float = 0.12
    margin_ratio: float = 0.02


@dataclass(frozen=True)
class MapExtentTheme:
    """Asymmetric padding for map extent so the legend/wordmark don't overlap the map."""

    left_ratio: float = 0.24
    right_ratio: float = 0.08
    bottom_ratio: float = 0.04
    top_ratio: float = 0.04


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the animation renderer."""

    parser = argparse.ArgumentParser(
        description="Render a LongFast propagation animation using cached LOS data."
    )
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", ""))
    parser.add_argument("--host", default=os.getenv("PGHOST", ""))
    parser.add_argument("--port", default=int(os.getenv("PGPORT", "5432")))
    parser.add_argument("--user", default=os.getenv("PGUSER", ""))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    parser.add_argument("--style", default="mapnik/styles/mesh_style.xml")
    parser.add_argument("--output-dir", default="data/out/visuals/longfast")
    parser.add_argument("--width", type=int, default=1920)
    parser.add_argument("--height", type=int, default=1080)
    parser.add_argument("--fps", type=int, default=24)
    parser.add_argument("--max-frames", type=int, default=0)
    parser.add_argument("--seed-name", default="Komzpa")
    parser.add_argument("--hop-limit", type=int, default=12)
    parser.add_argument("--wave-min-seconds", type=float, default=0.5)
    parser.add_argument("--wave-max-seconds", type=float, default=1.0)
    parser.add_argument("--jitter-seconds", type=float, default=0.2)
    parser.add_argument("--spreading-factor", type=int, default=11)
    parser.add_argument("--bandwidth-khz", type=int, default=250)
    parser.add_argument("--coding-rate", type=int, default=5)
    parser.add_argument("--payload-bytes", type=int, default=160)
    parser.add_argument("--tier-thresholds-db", default="")
    parser.add_argument(
        "--wordmark-path",
        default="docs/assets/maumap_logo_inline_with_background.png",
    )
    parser.add_argument("--wordmark-height", type=int, default=120)
    parser.add_argument("--no-video", action="store_true")
    parser.add_argument(
        "--assert-sample",
        action="store_true",
        help="Run layout/progress assertions for a known-good sample dataset.",
    )

    return parser.parse_args()


def load_font(
    size: int,
    monospace: bool = False,
    bold: bool = False,
) -> "ImageFont.ImageFont":
    """Load a readable sans-serif font for overlays."""

    # Prefer a system font for consistent typography.
    if ImageFont is None:  # pragma: no cover - optional dependency
        raise RuntimeError("Pillow font support is unavailable.")
    if monospace:
        candidates = (
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
        )
    elif bold:
        candidates = (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        )
    else:
        candidates = (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        )
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def measure_text(draw: "ImageDraw.ImageDraw", text: str, font) -> tuple[int, int]:
    """Measure text width/height in pixels."""

    if hasattr(draw, "textbbox"):
        box = draw.textbbox((0, 0), text, font=font)
        return box[2] - box[0], box[3] - box[1]
    return draw.textsize(text, font=font)


def build_fine_thresholds(thresholds: list[float]) -> list[float]:
    """Interleave midpoints between thresholds to add more color stops."""

    if len(thresholds) < 2:
        return thresholds[:]

    fine: list[float] = []
    for index in range(len(thresholds) - 1):
        fine.append(thresholds[index])
        midpoint = (thresholds[index] + thresholds[index + 1]) / 2
        fine.append(midpoint)
    fine.append(thresholds[-1])

    return fine


def format_number(value: float) -> str:
    """Format a numeric value with thousand separators."""

    return f"{int(round(value)):,}"


def rasterize_wordmark(svg_path: str, output_dir: str, height_px: int) -> str | None:
    """Rasterize the SVG wordmark into a cached PNG if needed."""

    if not svg_path:
        return None
    if not os.path.exists(svg_path):
        raise FileNotFoundError(f"Wordmark file not found: {svg_path}")

    if svg_path.lower().endswith(".png"):
        return svg_path

    if shutil.which("convert") is None:
        raise RuntimeError("ImageMagick `convert` is required to rasterize SVG wordmarks.")

    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(svg_path))[0]
    raster_path = os.path.join(output_dir, f"{base_name}_h{height_px}.png")
    if os.path.exists(raster_path):
        return raster_path

    command = [
        "convert",
        svg_path,
        "-background",
        "none",
        "-alpha",
        "set",
        "-resize",
        f"x{height_px}",
        raster_path,
    ]
    subprocess.run(command, check=True)

    return raster_path


def expand_bbox(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    padding_ratio: float,
) -> tuple[float, float, float, float]:
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

    return padded_minx, padded_miny, padded_maxx, padded_maxy


def expand_bbox_asymmetric(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    left_ratio: float,
    right_ratio: float,
    bottom_ratio: float,
    top_ratio: float,
) -> tuple[float, float, float, float]:
    """Expand a lon/lat bounding box with asymmetric padding ratios."""

    span_x = maxx - minx
    span_y = maxy - miny
    pad_left = span_x * left_ratio
    pad_right = span_x * right_ratio
    pad_bottom = span_y * bottom_ratio
    pad_top = span_y * top_ratio

    padded_minx = max(-180.0, minx - pad_left)
    padded_maxx = min(180.0, maxx + pad_right)
    padded_miny = max(-90.0, miny - pad_bottom)
    padded_maxy = min(90.0, maxy + pad_top)

    return padded_minx, padded_miny, padded_maxx, padded_maxy


def fetch_population_breaks(conn) -> list[float]:
    """Fetch quantile breaks for population density coloring."""

    # Compute quantiles so the density palette uses the full range.
    query = """
        select percentile_cont(array[0.2, 0.4, 0.6, 0.8])
            within group (order by population)
        from population_h3_r8
        where population > 0;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        if not row or row[0] is None:
            return [1.0, 10.0, 50.0, 200.0]

    return [float(value) for value in row[0]]


def build_population_layer_sql(breaks: list[float]) -> str:
    """Build SQL for the population density layer."""

    b1, b2, b3, b4 = breaks
    return f"""
        select
            h3_cell_to_boundary_geometry(h3) as geom,
            population,
            case
                when population <= {b1} then '#162640'
                when population <= {b2} then '#1b2f4b'
                when population <= {b3} then '#223656'
                when population <= {b4} then '#293f62'
                else '#334a72'
            end as density_color
        from population_h3_r8
        where population > 0
    """


def build_elevation_layer_sql() -> str:
    """Build SQL for elevation shading where population is zero."""

    return """
        with stats as (
            select
                min(ele) as min_ele,
                max(ele) as max_ele
            from mesh_surface_h3_r8
            where (population is null or population = 0)
              and ele is not null
        ),
        vals as (
            select
                h3,
                ele,
                case
                    when stats.max_ele is null or stats.min_ele is null then 0
                    when stats.max_ele = stats.min_ele then 0
                    else (ele - stats.min_ele) / (stats.max_ele - stats.min_ele)
                end as t
            from mesh_surface_h3_r8
            cross join stats
            where (population is null or population = 0)
              and ele is not null
        )
        select
            h3_cell_to_boundary_geometry(h3) as geom,
            '#' ||
            lpad(to_hex(greatest(0, least(255, round(22 * (1 - t))::int))), 2, '0') ||
            lpad(to_hex(greatest(0, least(255, round(32 * (1 - t))::int))), 2, '0') ||
            lpad(to_hex(greatest(0, least(255, round(58 * (1 - t))::int))), 2, '0') as elev_color
        from vals
    """


def build_roads_layer_sql() -> str:
    """Build SQL for road coverage overlay."""

    return """
        select
            h3_cell_to_boundary_geometry(h3) as geom
        from mesh_surface_h3_r8
        where has_road
    """


def build_country_border_sql() -> str:
    """Build SQL for the Georgia-Armenia border line."""

    return """
        with admin_polygons as (
            -- Identify admin-level 2 polygons for Georgia and Armenia.
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
            -- Extract Georgia polygon candidates by name.
            select geom
            from admin_polygons
            where normalized_name in (
                'georgia',
                'sakartvelo',
                'republic of georgia'
            )
        ),
        armenia as (
            -- Extract Armenia polygon candidates by ISO/name/wikidata hints.
            select geom
            from admin_polygons
            where lower(coalesce(tags ->> 'ISO3166-1:alpha2', '')) = 'am'
               or lower(coalesce(tags ->> 'int_name', '')) = 'armenia'
               or lower(coalesce(tags ->> 'name:en', '')) = 'armenia'
               or tags ->> 'wikidata' = 'Q399'
        ),
        georgia_boundary as (
            -- Build boundary lines for Georgia.
            select ST_Boundary(ST_Union(geom)) as geom
            from georgia
        ),
        armenia_boundary as (
            -- Build boundary lines for Armenia.
            select ST_Boundary(ST_Union(geom)) as geom
            from armenia
        )
        -- Return the shared Georgia-Armenia border.
        select ST_Intersection(gb.geom, ab.geom) as geom
        from georgia_boundary gb
        join armenia_boundary ab on true
        where ST_Intersection(gb.geom, ab.geom) is not null
    """


def overlay_legend(
    frame_path: str,
    stats: list[tuple[str, str]],
    hop_rows: list[tuple[str, str]],
    tier_labels: list[str],
    tier_colors: list[tuple[int, int, int]],
    node_colors: list[tuple[str, tuple[int, int, int]]],
    show_glow: bool,
    max_value_text: str,
    width_px: int,
    height_px: int,
    title_text: str,
    subtitle_text: str,
    wordmark_image: "Image.Image | None",
    assert_within_panel: bool = False,
) -> None:
    """Overlay a legend panel and stats text using Pillow."""

    if Image is None or ImageDraw is None or ImageFont is None:  # pragma: no cover
        if stats:
            print("Pillow is missing; skipping legend overlay.")
        return

    # Load the rendered frame as an RGBA image for compositing.
    image = Image.open(frame_path).convert("RGBA")
    draw = ImageDraw.Draw(image)
    theme = LegendTheme()
    stat_rows = stats or []
    hop_rows_present = hop_rows
    highlight_label = "Population reached"

    def build_layout(title_size: int, body_size: int, section_multiplier: float = 3.0) -> dict:
        subtitle_font_ratio = 0.9
        header_font_ratio = 1.05
        highlight_font_ratio = 1.8

        title_font = load_font(title_size, bold=True)
        subtitle_font = load_font(int(round(body_size * subtitle_font_ratio)))
        header_font = load_font(int(round(body_size * header_font_ratio)), bold=True)
        body_font = load_font(body_size)
        mono_font = load_font(body_size, monospace=True)
        highlight_font = load_font(int(round(body_size * highlight_font_ratio)), monospace=True)

        # Build legend rows with tier colors and labels.
        legend_rows = list(zip(tier_labels, tier_colors))

        title_w, title_h = measure_text(draw, title_text, title_font)
        subtitle_w, subtitle_h = measure_text(draw, subtitle_text, subtitle_font)
        sample_h = measure_text(draw, "Ag", body_font)[1]
        base_gap = theme.base_gap_px

        stat_label_w = 0
        stat_value_w = 0
        stat_block_h = 0
        stat_rows = stats or []
        for label, value in stat_rows:
            label_w, _ = measure_text(draw, label, body_font)
            if label == highlight_label:
                # Always reserve width for the maximum population value so the panel doesn't jump.
                value_sample = max_value_text or value
                value_w, value_h = measure_text(draw, value_sample, highlight_font)
            else:
                # Reserve width for a large-ish timer so it doesn't reflow later.
                value_sample = "999.9s" if value.endswith("s") else value
                value_w, value_h = measure_text(draw, value_sample, mono_font)
            stat_label_w = max(stat_label_w, label_w)
            stat_value_w = max(stat_value_w, value_w)
            stat_block_h += max(sample_h, value_h)
        if stat_rows:
            stat_block_h += 0

        for label, value in hop_rows:
            if label or value:
                label_w, _ = measure_text(draw, label, body_font)
                stat_label_w = max(stat_label_w, label_w)
        # Keep panel width stable even while hop rows are blank (we suppress labels until values appear).
        if hop_rows:
            max_hop_label = f"Hop {len(hop_rows) - 1}"
            hop_label_w, _ = measure_text(draw, max_hop_label, body_font)
            stat_label_w = max(stat_label_w, hop_label_w)

        if max_value_text:
            max_value_w, max_value_h = measure_text(draw, max_value_text, highlight_font)
            stat_value_w = max(stat_value_w, max_value_w)
            stat_block_h = max(stat_block_h, max(sample_h, max_value_h))

        stat_block_w = stat_label_w + stat_value_w + (12 if stat_rows else 0)
        hop_rows_present = hop_rows
        hop_block_h = sample_h * len(hop_rows_present) if hop_rows_present else 0

        node_title = "Nodes"
        node_title_w, node_title_h = measure_text(draw, node_title, header_font)
        swatch_size = max(int(round(sample_h * 0.6)), theme.swatch_min_px)
        node_row_h = max(sample_h, swatch_size)
        node_row_w = 0
        for label, _ in node_colors:
            label_w, _ = measure_text(draw, label, body_font)
            node_row_w = max(node_row_w, swatch_size + 8 + label_w)

        legend_title = "Signal quality"
        legend_title_w, legend_title_h = measure_text(draw, legend_title, header_font)
        swatch_gap = theme.swatch_gap_px
        legend_row_h = max(swatch_size, sample_h)
        legend_row_w = 0
        for label, _ in legend_rows:
            label_w, _ = measure_text(draw, label, body_font)
            legend_row_w = max(legend_row_w, swatch_size + swatch_gap + label_w)

        panel_pad = theme.panel_padding_px
        panel_w = max(
            title_w,
            subtitle_w,
            stat_block_w,
            node_title_w,
            node_row_w,
            legend_title_w,
            legend_row_w,
        ) + panel_pad * 2
        stat_gap_count = max(len(stat_rows), 1) - 1 if stat_rows else 0
        hop_gap_count = max(len(hop_rows_present), 1) - 1 if hop_rows_present else 0
        node_gap_count = max(len(node_colors), 1) - 1 if node_colors else 0
        legend_gap_count = max(len(legend_rows), 1) - 1 if legend_rows else 0
        section_count = 3 + (1 if hop_rows_present else 0)
        fixed_h = (
            title_h
            + subtitle_h
            + stat_block_h
            + hop_block_h
            + node_title_h
            + node_row_h * len(node_colors)
            + (node_row_h if show_glow else 0)
            + legend_title_h
            + legend_row_h * len(legend_rows)
        )
        gap_factor = (
            stat_gap_count
            + hop_gap_count
            + node_gap_count
            + legend_gap_count
            + section_count * section_multiplier
        )
        base_content_h = fixed_h + base_gap * gap_factor + panel_pad * 2

        return {
            "title_font": title_font,
            "body_font": body_font,
            "mono_font": mono_font,
            "highlight_font": highlight_font,
            "subtitle_font": subtitle_font,
            "header_font": header_font,
            "title_text": title_text,
            "title_w": title_w,
            "title_h": title_h,
            "subtitle_text": subtitle_text,
            "subtitle_w": subtitle_w,
            "subtitle_h": subtitle_h,
            "sample_h": sample_h,
            "base_gap": base_gap,
            "section_multiplier": section_multiplier,
            "stat_block_w": stat_block_w,
            "stat_block_h": stat_block_h,
            "hop_block_h": hop_block_h,
            "node_title": node_title,
            "node_title_w": node_title_w,
            "node_title_h": node_title_h,
            "node_row_h": node_row_h,
            "legend_title": legend_title,
            "legend_title_w": legend_title_w,
            "legend_title_h": legend_title_h,
            "legend_row_h": legend_row_h,
            "legend_rows": legend_rows,
            "swatch_size": swatch_size,
            "swatch_gap": swatch_gap,
            "panel_pad": panel_pad,
            "panel_w": panel_w,
            "base_content_h": base_content_h,
            "stat_gap_count": stat_gap_count,
            "hop_gap_count": hop_gap_count,
            "node_gap_count": node_gap_count,
            "legend_gap_count": legend_gap_count,
            "gap_factor": gap_factor,
            "fixed_h": fixed_h,
            "section_count": section_count,
        }

    # Layout knobs (keep them named to avoid "mystery constants" in rendering math).
    min_scale_ratio = 0.6
    title_min_px = 16
    body_min_px = 12
    scaled_title_min_px = 18
    scaled_body_min_px = 14
    section_multiplier_candidates = (2.4, 2.0, 1.6)
    min_line_gap_px = 2.0

    margin = theme.margin_px
    # Keep a small safety cushion so glyph descenders never clip at the bottom.
    # `build_layout()` uses a fixed padding, so we can target the true inner height.
    panel_target_h = max(
        height_px
        - margin * 2
        - theme.panel_padding_px * 2
        - theme.bottom_descender_guard_px,
        0,
    )
    panel_target_w = max(
        int(width_px * theme.panel_target_width_ratio),
        theme.panel_min_width_px,
    )
    layout = build_layout(
        theme.title_font_px,
        theme.body_font_px,
        section_multiplier=theme.section_multiplier,
    )
    if layout["base_content_h"] > panel_target_h:
        scale = max(panel_target_h / max(layout["base_content_h"], 1), min_scale_ratio)
        scaled_title = max(scaled_title_min_px, int(round(theme.title_font_px * scale)))
        scaled_body = max(scaled_body_min_px, int(round(theme.body_font_px * scale)))
        layout = build_layout(scaled_title, scaled_body)
        while layout["base_content_h"] > panel_target_h and scaled_body > body_min_px:
            scaled_title = max(title_min_px, scaled_title - 1)
            scaled_body = max(body_min_px, scaled_body - 1)
            layout = build_layout(scaled_title, scaled_body)
    if layout["panel_w"] > panel_target_w:
        scaled_title = layout["title_font"].size
        scaled_body = layout["body_font"].size
        while layout["panel_w"] > panel_target_w and scaled_body > body_min_px:
            scaled_title = max(title_min_px, scaled_title - 1)
            scaled_body = max(body_min_px, scaled_body - 1)
            layout = build_layout(scaled_title, scaled_body)
    if layout["base_content_h"] > panel_target_h:
        scaled_title = layout["title_font"].size
        scaled_body = layout["body_font"].size
        for section_multiplier in section_multiplier_candidates:
            layout = build_layout(scaled_title, scaled_body, section_multiplier=section_multiplier)
            if layout["base_content_h"] <= panel_target_h:
                break
    if layout["base_content_h"] > panel_target_h:
        scaled_title = layout["title_font"].size
        scaled_body = layout["body_font"].size
        while layout["base_content_h"] > panel_target_h and scaled_body > body_min_px:
            scaled_title = max(title_min_px, scaled_title - 1)
            scaled_body = max(body_min_px, scaled_body - 1)
            layout = build_layout(scaled_title, scaled_body)

    title_font = layout["title_font"]
    body_font = layout["body_font"]
    mono_font = layout["mono_font"]
    highlight_font = layout["highlight_font"]
    subtitle_font = layout["subtitle_font"]
    header_font = layout["header_font"]
    title_text = layout["title_text"]
    title_w = layout["title_w"]
    title_h = layout["title_h"]
    subtitle_text = layout["subtitle_text"]
    subtitle_w = layout["subtitle_w"]
    subtitle_h = layout["subtitle_h"]
    sample_h = layout["sample_h"]
    base_gap = layout["base_gap"]
    section_multiplier = layout["section_multiplier"]
    stat_block_w = layout["stat_block_w"]
    stat_block_h = layout["stat_block_h"]
    hop_block_h = layout["hop_block_h"]
    node_title = layout["node_title"]
    node_title_h = layout["node_title_h"]
    node_title_w = layout["node_title_w"]
    node_row_h = layout["node_row_h"]
    legend_title = layout["legend_title"]
    legend_title_h = layout["legend_title_h"]
    legend_title_w = layout["legend_title_w"]
    legend_row_h = layout["legend_row_h"]
    legend_rows = layout["legend_rows"]
    swatch_size = layout["swatch_size"]
    swatch_gap = layout["swatch_gap"]
    panel_pad = layout["panel_pad"]
    panel_w = layout["panel_w"]
    base_content_h = layout["base_content_h"]
    gap_factor = layout["gap_factor"]
    fixed_h = layout["fixed_h"]

    # Anchor panel to full height on the left.
    panel_left = margin
    panel_top = margin
    panel_h = max(height_px - margin * 2, 0)
    panel_right = panel_left + panel_w
    panel_bottom = panel_top + panel_h
    min_section_gap_px = 6
    section_gap_scale = 1.5
    section_gap = max(min_section_gap_px, int(round(base_gap * section_gap_scale)))
    available_h = max(panel_h - panel_pad * 2, 0)
    extra_space = max(available_h - fixed_h, 0)
    required_gap = 0.0
    if gap_factor > 0:
        required_gap = extra_space / gap_factor
    line_gap = required_gap
    max_line_gap_ratio = 0.9
    max_line_gap = max(int(round(sample_h * max_line_gap_ratio)), base_gap)
    line_gap = min(line_gap, max_line_gap)
    # If content doesn't fit, allow gaps to shrink below `base_gap`.
    if fixed_h + line_gap * max(gap_factor, 1) > available_h and gap_factor > 0:
        line_gap = max((available_h - fixed_h) / gap_factor, 0.0)
    line_gap = max(line_gap, min_line_gap_px)
    # Quantize gaps to integers to avoid fractional drift that can clip glyph descenders.
    line_gap_int = int(round(line_gap))
    section_gap_cap_ratio = 2.2
    section_gap_int = int(
        round(
            min(
                line_gap_int * section_multiplier,
                max(sample_h, line_gap_int * section_gap_cap_ratio),
            )
        )
    )
    stat_gap = line_gap_int
    hop_gap = line_gap_int
    node_gap = line_gap_int
    legend_gap = line_gap_int

    # Draw panel background.
    panel_fill = theme.panel_fill_rgba
    panel_outline = theme.panel_outline_rgba
    if hasattr(draw, "rounded_rectangle"):
        draw.rounded_rectangle(
            [panel_left, panel_top, panel_right, panel_bottom],
            radius=theme.panel_corner_radius_px,
            fill=panel_fill,
            outline=panel_outline,
            width=1,
        )
    else:
        draw.rectangle(
            [panel_left, panel_top, panel_right, panel_bottom],
            fill=panel_fill,
            outline=panel_outline,
            width=1,
        )

    # Draw title and stats text.
    cursor_x = int(panel_left + panel_pad)
    cursor_y = int(panel_top + panel_pad)
    # Assertions should validate we stay within the visible panel box.
    # Keep a consistent bottom inset so glyph descenders never hang outside the panel.
    panel_limit_bottom = int(panel_bottom - panel_pad)
    draw.text((cursor_x, cursor_y), title_text, font=title_font, fill=theme.text_primary_rgba)
    cursor_y += int(title_h) + max(line_gap_int, 4)
    draw.text((cursor_x, cursor_y), subtitle_text, font=subtitle_font, fill=theme.text_muted_rgba)
    cursor_y += int(subtitle_h) + section_gap_int

    if assert_within_panel:
        assert panel_left < panel_right, f"Panel left/right invalid: {panel_left=} {panel_right=}"
        assert panel_top < panel_bottom, f"Panel top/bottom invalid: {panel_top=} {panel_bottom=}"

    if stat_rows:
        value_right = int(panel_right - panel_pad)
        for index, (label, value) in enumerate(stat_rows):
            draw.text((cursor_x, cursor_y), label, font=body_font, fill=theme.text_body_rgba)
            if label == highlight_label:
                value_w, value_h = measure_text(draw, value, highlight_font)
                draw.text(
                    (value_right - value_w, cursor_y - max(value_h - sample_h, 0) * 0.3),
                    value,
                    font=highlight_font,
                    fill=theme.text_primary_rgba,
                )
                if assert_within_panel:
                    assert value_right - value_w >= panel_left, (
                        f"Highlight value left out of panel: {value_right - value_w}"
                    )
                    assert cursor_y + value_h <= panel_limit_bottom, (
                        f"Highlight value bottom out of panel: {cursor_y + value_h} > {panel_limit_bottom}"
                    )
                cursor_y += int(max(sample_h, value_h))
            else:
                value_w, _ = measure_text(draw, value, mono_font)
                draw.text(
                    (value_right - value_w, cursor_y),
                    value,
                    font=mono_font,
                    fill=theme.text_primary_rgba,
                )
                if assert_within_panel:
                    assert value_right - value_w >= panel_left, (
                        f"Stat value left out of panel: {value_right - value_w}"
                    )
                    assert cursor_y + sample_h <= panel_limit_bottom, (
                        f"Stat value bottom out of panel: {cursor_y + sample_h} > {panel_limit_bottom}"
                    )
                cursor_y += sample_h
            if index < len(stat_rows) - 1:
                cursor_y += stat_gap

    if hop_rows_present:
        cursor_y += section_gap_int
        value_right = int(panel_right - panel_pad)
        for index, (label, value) in enumerate(hop_rows_present):
            if label and value:
                draw.text(
                    (cursor_x, cursor_y),
                    label,
                    font=body_font,
                    fill=theme.text_hop_label_rgba,
                )
                value_w, _ = measure_text(draw, value, mono_font)
                draw.text(
                    (value_right - value_w, cursor_y),
                    value,
                    font=mono_font,
                    fill=theme.text_hop_value_rgba,
                )
            cursor_y += sample_h
            if index < len(hop_rows_present) - 1:
                cursor_y += hop_gap

    cursor_y += section_gap_int

    draw.text((cursor_x, cursor_y), node_title, font=header_font, fill=theme.text_body_rgba)
    cursor_y += node_title_h + node_gap

    for index, (label, color) in enumerate(node_colors):
        label_w, label_h = measure_text(draw, label, body_font)
        text_y = cursor_y + max(int((node_row_h - label_h) / 2), 0)
        swatch_top = cursor_y + max(int((node_row_h - swatch_size) / 2), 0)
        swatch_left = cursor_x
        swatch_right = swatch_left + swatch_size
        swatch_bottom = swatch_top + swatch_size
        draw.rectangle(
            [swatch_left, swatch_top, swatch_right, swatch_bottom],
            fill=color + (255,),
            outline=None,
        )
        draw.text((swatch_right + 8, text_y), label, font=body_font, fill=theme.text_body_rgba)
        if assert_within_panel:
            assert swatch_right <= panel_right, f"Node swatch right out of panel: {swatch_right} > {panel_right}"
            assert swatch_bottom <= panel_limit_bottom, f"Node swatch bottom out of panel: {swatch_bottom} > {panel_limit_bottom}"
            assert text_y + label_h <= panel_limit_bottom, f"Node label bottom out of panel: {text_y + label_h} > {panel_limit_bottom}"
        cursor_y += node_row_h
        if index < len(node_colors) - 1 or show_glow:
            cursor_y += node_gap

    if show_glow:
        swatch_top = cursor_y + max(int((node_row_h - swatch_size) / 2), 0)
        swatch_left = cursor_x
        swatch_right = swatch_left + swatch_size
        swatch_bottom = swatch_top + swatch_size
        # Outer glow ring + inner bright core.
        draw.ellipse(
            [swatch_left, swatch_top, swatch_right, swatch_bottom],
            fill=theme.glow_fill_rgba,
            outline=theme.glow_outline_rgba,
            width=max(1, int(round(swatch_size * theme.glow_outline_width_ratio))),
        )
        glow_label = "On air"
        _, glow_label_h = measure_text(draw, glow_label, body_font)
        glow_text_y = cursor_y + max(int((node_row_h - glow_label_h) / 2), 0)
        draw.text((swatch_right + 8, glow_text_y), glow_label, font=body_font, fill=theme.text_body_rgba)
        if assert_within_panel:
            assert glow_text_y + glow_label_h <= panel_limit_bottom, f"Glow label bottom out of panel: {glow_text_y + glow_label_h} > {panel_limit_bottom}"
            assert swatch_bottom <= panel_limit_bottom, f"Glow swatch bottom out of panel: {swatch_bottom} > {panel_limit_bottom}"
        cursor_y += node_row_h

    cursor_y += section_gap_int
    draw.text((cursor_x, cursor_y), legend_title, font=header_font, fill=theme.text_body_rgba)
    cursor_y += legend_title_h + legend_gap

    for index, (label, color) in enumerate(legend_rows):
        label_w, label_h = measure_text(draw, label, body_font)
        text_y = cursor_y + max(int((legend_row_h - label_h) / 2), 0)
        swatch_top = cursor_y + max(int((legend_row_h - swatch_size) / 2), 0)
        swatch_left = cursor_x
        swatch_right = swatch_left + swatch_size
        swatch_bottom = swatch_top + swatch_size
        draw.rectangle(
            [swatch_left, swatch_top, swatch_right, swatch_bottom],
            fill=color + (255,),
            outline=None,
        )
        draw.text((swatch_right + swatch_gap, text_y), label, font=body_font, fill=theme.text_body_rgba)
        if assert_within_panel:
            assert swatch_bottom <= panel_limit_bottom, f"Legend swatch bottom out of panel: {swatch_bottom} > {panel_limit_bottom}"
            assert text_y + label_h <= panel_limit_bottom, f"Legend label bottom out of panel: {text_y + label_h} > {panel_limit_bottom}"
        cursor_y += legend_row_h
        if index < len(legend_rows) - 1:
            cursor_y += legend_gap

    if wordmark_image:
        logo_w, logo_h = wordmark_image.size
        wordmark_margin = max(margin, int(width_px * WordmarkTheme().margin_ratio))
        logo_x = max(width_px - logo_w - wordmark_margin, 0)
        logo_y = max(height_px - logo_h - wordmark_margin, 0)
        image.alpha_composite(wordmark_image, (logo_x, logo_y))

    image.save(frame_path)


def select_next_transmitter(
    eligible: list[str],
    received_quality: dict[str, float],
) -> str | None:
    """Pick the next transmitter based on worst received quality."""

    if not eligible:
        return None

    return max(
        eligible,
        key=lambda tower_h3: (received_quality.get(tower_h3, 0.0), tower_h3),
    )


def simulate_propagation(
    seed_h3: str,
    tower_visibility: dict[str, list[tuple[str, float]]],
    visible_cells: dict[str, list[tuple[str, float]]],
    population: dict[str, float],
    tower_hops: dict[str, int],
    animation: AnimationConfig,
    airtime_s: float,
    max_frames: int,
    rng: random.Random,
) -> tuple[int, float]:
    """Simulate propagation to estimate total frames and max population."""

    # Track message state without rendering.
    received_cells: dict[str, float] = {}
    received_towers = {seed_h3}
    received_quality = {seed_h3: 0.0}
    transmitted_towers: set[str] = set()
    active_transmissions: list[dict[str, object]] = []

    max_population = 0.0
    frame = 0

    while True:
        frame_time = frame / animation.fps

        # Finalize transmissions that ended in this frame.
        finished = [t for t in active_transmissions if frame_time >= t["end"]]
        active_transmissions = [t for t in active_transmissions if frame_time < t["end"]]
        for transmission in finished:
            for cell_h3, path_loss_db in transmission["cell_losses"]:
                if cell_h3 not in received_cells or path_loss_db < received_cells[cell_h3]:
                    received_cells[cell_h3] = path_loss_db
            transmitted_towers.add(transmission["tower_h3"])
            for neighbor, path_loss_db in transmission["audible_pairs"]:
                if neighbor not in received_towers:
                    received_towers.add(neighbor)
                    received_quality[neighbor] = path_loss_db
                else:
                    received_quality[neighbor] = max(
                        received_quality.get(neighbor, path_loss_db),
                        path_loss_db,
                    )

        # Determine blocked towers that can hear active transmissions.
        blocked_towers = {
            neighbor
            for transmission in active_transmissions
            for neighbor in transmission["audible_towers"]
        }
        active_towers = {transmission["tower_h3"] for transmission in active_transmissions}

        # Start at most one new transmitter per frame after the initial frame.
        # Frame 0 is intentionally idle.
        if frame > 0:
            eligible = [
                tower_h3
                for tower_h3 in received_towers
                if tower_h3 in tower_hops
                and tower_h3 not in transmitted_towers
                and tower_h3 not in active_towers
                and tower_h3 not in blocked_towers
            ]
            next_tower = select_next_transmitter(eligible, received_quality)
            if next_tower:
                tower_visible_cells = visible_cells.get(next_tower, [])
                audible_pairs = tower_visibility.get(next_tower, [])
                duration = airtime_s + rng.uniform(0.0, animation.jitter_s)
                active_transmissions.append(
                    {
                        "tower_h3": next_tower,
                        "end": frame_time + duration,
                        "cell_losses": tower_visible_cells,
                        "audible_pairs": audible_pairs,
                        "audible_towers": [neighbor for neighbor, _ in audible_pairs],
                    }
                )

        cumulative_population = sum(
            population.get(h3, 0.0) for h3 in received_cells
        )
        max_population = max(max_population, cumulative_population)

        frame += 1
        if max_frames and frame >= max_frames:
            break
        if (
            frame > 0
            and not active_transmissions
            and all(
                tower_h3 in transmitted_towers or tower_h3 not in tower_hops
                for tower_h3 in received_towers
            )
        ):
            break

    return frame, max_population


def main() -> None:
    """Render the LongFast propagation animation."""

    args = parse_args()

    config = DbConfig(
        dbname=args.dbname,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )

    radio = RadioConfig(
        spreading_factor=args.spreading_factor,
        bandwidth_khz=args.bandwidth_khz,
        coding_rate=args.coding_rate,
        payload_bytes=args.payload_bytes,
        preamble_symbols=8,
        crc_enabled=1,
        implicit_header=0,
    )

    animation = AnimationConfig(
        fps=args.fps,
        hop_limit=args.hop_limit,
        wave_min_s=args.wave_min_seconds,
        wave_max_s=args.wave_max_seconds,
        jitter_s=args.jitter_seconds,
        tier_thresholds_db=[],
    )

    os.makedirs(args.output_dir, exist_ok=True)

    conn = open_readonly_connection(config)

    # Frame the map extent using the convex hull.
    try:
        minx, miny, maxx, maxy = fetch_bbox(conn)
    except Exception as exc:  # pragma: no cover - runtime environment dependent
        raise RuntimeError(
            "Failed to read georgia_convex_hull. Set PGDATABASE/--dbname to the pipeline database."
        ) from exc
    extent_theme = MapExtentTheme()
    minx, miny, maxx, maxy = expand_bbox_asymmetric(
        minx,
        miny,
        maxx,
        maxy,
        left_ratio=extent_theme.left_ratio,
        right_ratio=extent_theme.right_ratio,
        bottom_ratio=extent_theme.bottom_ratio,
        top_ratio=extent_theme.top_ratio,
    )
    extent = f"{minx},{miny},{maxx},{maxy}"

    # Resolve the seed tower and full tower list.
    seed_h3 = fetch_seed_h3(conn, args.seed_name)
    towers = fetch_towers(conn, exclude_sources=["greedy"])

    if seed_h3 not in towers:
        raise RuntimeError("Seed H3 not found in mesh_towers.")

    print("Loading tower visibility and cached LOS data.")
    tower_visibility = fetch_tower_visibility(conn, towers.keys())
    adjacency = {
        tower_h3: [neighbor for neighbor, _ in neighbors]
        for tower_h3, neighbors in tower_visibility.items()
    }
    visible_cells = fetch_visible_cells(conn, towers.keys())
    total_links = sum(len(neighbors) for neighbors in adjacency.values()) // 2
    total_visibility = sum(len(cells) for cells in visible_cells.values())
    print(f"Loaded {len(towers)} towers and {total_links} tower links.")
    print(f"Loaded {total_visibility} cached tower-to-cell visibility pairs.")

    print("Computing BFS waves.")
    waves = build_waves(seed_h3, adjacency, animation.hop_limit)
    print(f"Computed {len(waves)} hop waves.")
    tower_hops: dict[str, int] = {}
    for hop_index, wave in enumerate(waves):
        for tower_h3 in wave:
            tower_hops[tower_h3] = hop_index

    print("Estimating airtime.")
    airtime_s = estimate_airtime_seconds(radio)
    print(f"Estimated airtime {airtime_s:.3f}s per transmission.")

    print("Computing path loss tiers.")
    thresholds = build_tier_thresholds(conn, args.tier_thresholds_db)
    if len(thresholds) != 6:
        raise RuntimeError(
            "Tier thresholds must include exactly six values to match 7 tier anchors."
        )
    fine_thresholds = build_fine_thresholds(thresholds)
    print(f"Tier thresholds: {', '.join(f'{t:.1f}' for t in thresholds)}")
    print(f"Expanded to {len(fine_thresholds) + 1} signal tiers for smoother gradients.")

    animation = AnimationConfig(
        fps=animation.fps,
        hop_limit=animation.hop_limit,
        wave_min_s=animation.wave_min_s,
        wave_max_s=animation.wave_max_s,
        jitter_s=animation.jitter_s,
        tier_thresholds_db=fine_thresholds,
    )

    print("Preparing population lookup.")
    all_cells = {
        cell_h3
        for tower_cells in visible_cells.values()
        for cell_h3, _ in tower_cells
    }
    population = fetch_population(conn, all_cells)
    print(f"Loaded population for {len(population)} cells.")
    if not population:
        print("Warning: population_h3_r8 returned no rows; animation counters will show zero.")

    print("Computing population density breaks.")
    population_breaks = fetch_population_breaks(conn)
    population_layer_sql = build_population_layer_sql(population_breaks)
    elevation_layer_sql = build_elevation_layer_sql()
    roads_layer_sql = build_roads_layer_sql()
    border_sql = build_country_border_sql()

    received_cells: dict[str, float] = {}
    hop_reached: dict[int, set[str]] = {}
    received_towers = {seed_h3}
    received_quality = {seed_h3: 0.0}
    transmitted_towers: set[str] = set()
    received_version = 0
    seen_links: set[tuple[str, str]] = set()
    seen_links_version = 0
    active_transmissions: list[dict[str, object]] = []

    towers_sql = """
        (
            select h3, source, h3::geometry as geom
            from mesh_towers
            where source <> 'greedy'
        ) as towers
    """

    print("Simulating propagation to estimate total frames and max population.")
    rng_seed = 42
    sim_rng = random.Random(rng_seed)
    sim_total_frames, max_population = simulate_propagation(
        seed_h3=seed_h3,
        tower_visibility=tower_visibility,
        visible_cells=visible_cells,
        population=population,
        tower_hops=tower_hops,
        animation=animation,
        airtime_s=airtime_s,
        # Always simulate the full run to get a stable max population estimate,
        # even when we only render the first `--max-frames` frames.
        max_frames=0,
        rng=sim_rng,
    )
    total_frames = max(sim_total_frames, 1)
    if args.max_frames:
        total_frames = min(total_frames, args.max_frames)
    print(f"Estimated total frames: {total_frames}")
    print(f"Estimated max population reached: {format_number(max_population)}")

    legend_tier_labels = [
        "very strong",
        "strong",
        "good",
        "fair",
        "weak",
        "very weak",
        "barely",
    ]
    anchor_colors = [
        (255, 247, 209),
        (255, 229, 154),
        (255, 209, 102),
        (251, 191, 36),
        (245, 158, 11),
        (249, 115, 22),
        (234, 88, 12),
    ]

    total_population = sum(population.values())
    max_value_text = format_number(max(total_population, max_population))

    wordmark_image = None
    if Image is not None and args.wordmark_path:
        print("Preparing wordmark overlay.")
        wordmark_raster = rasterize_wordmark(
            args.wordmark_path,
            args.output_dir,
            args.wordmark_height,
        )
        if wordmark_raster:
            wordmark_image = Image.open(wordmark_raster).convert("RGBA")
            wordmark_theme = WordmarkTheme()
            max_wordmark_width = int(args.width * wordmark_theme.max_width_ratio)
            max_wordmark_height = int(args.height * wordmark_theme.max_height_ratio)
            current_w, current_h = wordmark_image.size
            scale = min(
                max_wordmark_width / max(current_w, 1),
                max_wordmark_height / max(current_h, 1),
                1.0,
            )
            if scale < 1.0:
                new_size = (int(current_w * scale), int(current_h * scale))
                wordmark_image = wordmark_image.resize(new_size, resample=Image.LANCZOS)

    print("Rendering frames.")
    render_rng = random.Random(rng_seed)
    last_state_key = None
    last_render_path = None
    needs_render = True
    last_active_count = 0

    for frame in range(total_frames):
        frame_time = frame / animation.fps
        if frame % max(int(animation.fps * 2), 1) == 0:
            print(f"Rendering frame {frame + 1}/{total_frames} (t={frame_time:.1f}s)")

        # Finalize any transmissions that have ended by this frame.
        finished_transmissions = [
            transmission
            for transmission in active_transmissions
            if frame_time >= transmission["end"]
        ]
        if finished_transmissions:
            active_transmissions = [
                transmission
                for transmission in active_transmissions
                if frame_time < transmission["end"]
            ]
        for transmission in finished_transmissions:
            newly_received = 0
            for cell_h3, tier, hop_index, path_loss_db in transmission["cell_tiers"]:
                if cell_h3 not in received_cells:
                    received_cells[cell_h3] = path_loss_db
                    hop_reached.setdefault(hop_index, set()).add(cell_h3)
                    continue
                if path_loss_db < received_cells[cell_h3]:
                    received_cells[cell_h3] = path_loss_db

            transmitted_towers.add(transmission["tower_h3"])
            received_version += 1

            for neighbor, path_loss_db in transmission["audible_pairs"]:
                if neighbor not in received_towers:
                    received_towers.add(neighbor)
                    received_quality[neighbor] = path_loss_db
                    newly_received += 1
                else:
                    received_quality[neighbor] = max(
                        received_quality.get(neighbor, path_loss_db),
                        path_loss_db,
                    )
                if neighbor not in transmitted_towers:
                    before_links = len(seen_links)
                    seen_links.add((transmission["tower_h3"], neighbor))
                    if len(seen_links) != before_links:
                        seen_links_version += 1
            print(
                "Completed transmission",
                transmission["tower_h3"],
                f"t={frame_time:.1f}s",
                f"new towers={newly_received}",
            )

        # Identify towers blocked by on-air transmissions.
        blocked_towers = {
            neighbor
            for transmission in active_transmissions
            for neighbor in transmission["audible_towers"]
        }
        active_towers = {transmission["tower_h3"] for transmission in active_transmissions}

        # Start at most one new transmitter per frame after the initial frame.
        if frame > 0:
            eligible = [
                tower_h3
                for tower_h3 in received_towers
                if tower_h3 in tower_hops
                and tower_h3 not in transmitted_towers
                and tower_h3 not in active_towers
                and tower_h3 not in blocked_towers
            ]
            next_tower = select_next_transmitter(eligible, received_quality)
            if next_tower:
                hop_index = 0 if next_tower == seed_h3 else tower_hops.get(next_tower, 0)
                tower_visible = visible_cells.get(next_tower, [])
                tiered: dict[int, list[str]] = {}
                cell_tiers = []

                # Bucket visible cells by path loss tier.
                for cell_h3, path_loss_db in tower_visible:
                    tier = assign_tier(path_loss_db, animation.tier_thresholds_db)
                    tiered.setdefault(tier, []).append(cell_h3)
                    cell_tiers.append((cell_h3, tier, hop_index, path_loss_db))

                duration = airtime_s + render_rng.uniform(0.0, animation.jitter_s)
                audible_pairs = tower_visibility.get(next_tower, [])
                active_transmissions.append(
                    {
                        "tower_h3": next_tower,
                        "start": frame_time,
                        "end": frame_time + duration,
                        "hop": hop_index,
                        "tiered": tiered,
                        "cell_tiers": cell_tiers,
                        "audible_pairs": audible_pairs,
                        "audible_towers": [neighbor for neighbor, _ in audible_pairs],
                    }
                )
                print(
                    "Starting transmission",
                    next_tower,
                    f"hop={hop_index}",
                    f"t={frame_time:.1f}s",
                    f"duration={duration:.2f}s",
                    f"audible={len(audible_pairs)}",
                )
        active_count = len(active_transmissions)
        if active_count != last_active_count and active_count > 1:
            print(
                "Concurrent transmissions",
                f"count={active_count}",
                f"t={frame_time:.1f}s",
            )
        last_active_count = active_count

        active_links = []
        for transmission in active_transmissions:
            for neighbor in transmission["audible_towers"]:
                if neighbor not in transmitted_towers:
                    active_links.append((transmission["tower_h3"], neighbor))
        active_links_key = tuple(sorted(active_links))
        state_key = (
            tuple(sorted(active_towers)),
            received_version,
            active_links_key,
            seen_links_version,
        )
        needs_render = state_key != last_state_key
        last_state_key = state_key

        map_obj = mapnik.Map(args.width, args.height)
        mapnik.load_map(map_obj, args.style)
        map_obj.srs = "+proj=utm +zone=38 +datum=WGS84 +units=m +no_defs"
        src_proj = mapnik.Projection("+proj=longlat +datum=WGS84 +no_defs")
        dst_proj = mapnik.Projection(map_obj.srs)
        transform = mapnik.ProjTransform(src_proj, dst_proj)
        bbox_4326 = mapnik.Box2d(minx, miny, maxx, maxy)
        bbox_3857 = transform.forward(bbox_4326)
        map_obj.zoom_to_box(bbox_3857)

        can_reuse = not needs_render and last_render_path and Image is not None
        if not can_reuse:
            map_obj.layers.append(
                make_postgis_layer(
                    config,
                    name="elevation_empty",
                    style="elevation-empty",
                    table=f"({elevation_layer_sql}) as elevation_empty",
                    extent=extent,
                )
            )
            map_obj.layers.append(
                make_postgis_layer(
                    config,
                    name="roads_overlay",
                    style="roads-overlay",
                    table=f"({roads_layer_sql}) as roads_overlay",
                    extent=extent,
                )
            )
            map_obj.layers.append(
                make_postgis_layer(
                    config,
                    name="population_density",
                    style="population-density-anim",
                    table=f"({population_layer_sql}) as population_density",
                    extent=extent,
                )
            )

        frame_layers = []

        # Build received and transmitting layers for each tier.
        active_tiered: dict[int, list[str]] = {}
        for transmission in active_transmissions:
            for tier_index, cells in transmission["tiered"].items():
                active_tiered.setdefault(tier_index, []).extend(cells)

        received_by_tier: dict[int, list[str]] = {}
        for cell_h3, path_loss_db in received_cells.items():
            tier_index = assign_tier(path_loss_db, animation.tier_thresholds_db)
            received_by_tier.setdefault(tier_index, []).append(cell_h3)

        for tier_index in range(len(animation.tier_thresholds_db) + 1):
            received_h3 = received_by_tier.get(tier_index, [])
            transmit_h3 = active_tiered.get(tier_index, [])

            received_sql = build_h3_polygon_layer_sql(received_h3)
            transmit_sql = build_h3_polygon_layer_sql(transmit_h3)

            frame_layers.append(
                make_postgis_layer(
                    config,
                    name=f"received_tier_{tier_index}",
                    style=f"received-tier-{tier_index + 1}",
                    table=f"({received_sql}) as received_tier_{tier_index}",
                    extent=extent,
                )
            )
            frame_layers.append(
                make_postgis_layer(
                    config,
                    name=f"transmit_tier_{tier_index}",
                    style=f"transmit-tier-{tier_index + 1}",
                    table=f"({transmit_sql}) as transmit_tier_{tier_index}",
                    extent=extent,
                )
            )

        # Compute counters for the label overlay.
        # Treat on-air coverage as "reached" for counters and hop breakdowns,
        # so the animation shows immediate impact starting on frame 1.
        effective_cells = set(received_cells.keys())
        effective_hop_reached: dict[int, set[str]] = {
            hop: set(cells) for hop, cells in hop_reached.items()
        }
        for transmission in active_transmissions:
            hop_index = int(transmission["hop"])
            hop_cells = effective_hop_reached.setdefault(hop_index, set())
            for cell_h3, _, _, _path_loss_db in transmission["cell_tiers"]:
                # For hop breakdowns, count each cell in the hop where it is first reached.
                if cell_h3 in received_cells or cell_h3 in effective_cells:
                    continue
                effective_cells.add(cell_h3)
                hop_cells.add(cell_h3)

        cumulative_population = sum(population.get(h3, 0.0) for h3 in effective_cells)
        stat_rows = [
            ("Time", f"{frame_time:.1f}s"),
            ("Population reached", format_number(cumulative_population)),
        ]
        hop_rows = []
        for hop in range(0, animation.hop_limit + 1):
            if hop in effective_hop_reached:
                hop_value = sum(population.get(h3, 0.0) for h3 in effective_hop_reached[hop])
                if hop_value > 0:
                    hop_rows.append((f"Hop {hop}", format_number(hop_value)))
                else:
                    hop_rows.append(("", ""))
            else:
                hop_rows.append(("", ""))

        if args.assert_sample:
            hop_values = {
                hop: sum(population.get(h3, 0.0) for h3 in cells)
                for hop, cells in effective_hop_reached.items()
            }
            pop_int = int(round(cumulative_population))
            if frame == 1:
                assert pop_int > 0, f"Expected population > 0 on frame 1, got {pop_int}"
                hop0_int = int(round(hop_values.get(0, 0.0)))
                assert hop0_int == pop_int, (
                    "Expected Hop 0 bucket to equal total population reached on frame 1. "
                    f"got hop0={hop0_int} pop={pop_int}"
                )
            if frame == 38 and total_frames > 38:
                assert hop_values.get(0, 0.0) > 0, "Expected Hop 0 to be present on frame 38"
                assert hop_values.get(1, 0.0) > 0, "Expected Hop 1 to be present on frame 38"

        # Build link pairs for active transmissions.
        link_pairs = active_links

        if not can_reuse:
            map_obj.layers.extend(frame_layers)
            if seen_links:
                past_sql = build_h3_link_layer_sql(sorted(seen_links))
                map_obj.layers.append(
                    make_postgis_layer(
                        config,
                        name="past_links",
                        style="link-lines-past",
                        table=f"({past_sql}) as past_links",
                        extent=extent,
                    )
                )
            if link_pairs:
                link_sql = build_h3_link_layer_sql(link_pairs)
                map_obj.layers.append(
                    make_postgis_layer(
                        config,
                        name="pending_links",
                        style="link-lines",
                        table=f"({link_sql}) as pending_links",
                        extent=extent,
                    )
                )
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
            map_obj.layers.append(
                make_postgis_layer(
                    config,
                    name="mesh_towers",
                    style="tower-points",
                    table=towers_sql,
                    extent=extent,
                )
            )
            if active_towers:
                active_sql = build_h3_point_layer_sql(sorted(active_towers))
                map_obj.layers.append(
                    make_postgis_layer(
                        config,
                        name="active_tower",
                        style="tower-active",
                        table=f"({active_sql}) as active_tower",
                        extent=extent,
                    )
                )

        # Render the frame with dynamic layers appended.
        frame_path = os.path.join(args.output_dir, f"frame_{frame:04d}.png")
        if can_reuse:
            base = Image.open(last_render_path).convert("RGBA")
            base.save(frame_path)
        else:
            mapnik.render_to_file(map_obj, frame_path, "png")
            last_render_path = frame_path

        overlay_legend(
            frame_path,
            stat_rows,
            hop_rows,
            legend_tier_labels,
            anchor_colors,
            [
                ("Existing sites", (248, 250, 252)),
                ("Proposed sites", (56, 189, 248)),
            ],
            True,
            max_value_text,
            args.width,
            args.height,
            "LoRa Mesh Signal Propagation",
            "Meshtastic, EU868 LongFast preset",
            wordmark_image,
            assert_within_panel=args.assert_sample,
        )

    # Clean up any leftover frames from previous runs so ffmpeg doesn't pick them up.
    # This is especially important when a render is restarted with a smaller `--max-frames`.
    frame_pattern = re.compile(r"^frame_(\d{4})\.png$")
    extra_frames: list[str] = []
    zero_sized: list[str] = []
    for name in os.listdir(args.output_dir):
        match = frame_pattern.match(name)
        if not match:
            continue
        idx = int(match.group(1))
        path = os.path.join(args.output_dir, name)
        try:
            size = os.path.getsize(path)
        except OSError:
            continue
        if size == 0:
            zero_sized.append(name)
        if idx >= total_frames:
            extra_frames.append(path)
    for path in sorted(extra_frames):
        try:
            os.remove(path)
        except OSError:
            pass
    if zero_sized:
        raise RuntimeError(
            "Found zero-sized frame files in output dir; remove them and re-run the renderer. "
            f"examples={sorted(zero_sized)[:3]}"
        )

    print(f"Rendered {total_frames} frames into {args.output_dir}")
    if not args.no_video:
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            print("Warning: ffmpeg not found; skipping MP4 assembly.")
        else:
            print("Assembling MP4 animation.")
            video_path = os.path.join(args.output_dir, "longfast.mp4")
            command = [
                ffmpeg,
                "-y",
                "-framerate",
                str(animation.fps),
                "-i",
                os.path.join(args.output_dir, "frame_%04d.png"),
                "-pix_fmt",
                "yuv420p",
                video_path,
            ]
            subprocess.run(command, check=True)
            print(f"Wrote video to {video_path}")

    print("Animation render complete.")


if __name__ == "__main__":
    main()
