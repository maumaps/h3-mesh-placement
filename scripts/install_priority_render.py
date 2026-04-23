"""
Rendering and presentation helpers for the installer-priority handout.
"""

from __future__ import annotations

from collections import defaultdict
from html import escape
from typing import Mapping, Sequence

from scripts.install_priority_graph import TowerRecord
from scripts.install_priority_maplibre import (
    cluster_map_id,
    normalize_rows,
    render_map_assets,
)
from scripts.install_priority_render_sections import (
    render_cluster_section,
    render_summary_section,
)


CSV_COLUMNS = [
    "cluster_key",
    "cluster_label",
    "cluster_install_rank",
    "is_next_for_cluster",
    "rollout_status",
    "installed",
    "tower_id",
    "label",
    "display_name",
    "display_type",
    "source",
    "impact_score",
    "impact_people_est",
    "impact_tower_count",
    "next_unlock_count",
    "backlink_count",
    "primary_previous_tower_id",
    "inter_cluster_neighbor_ids",
    "inter_cluster_connections",
    "blocked_reason",
    "previous_connections",
    "next_connections",
    "lon",
    "lat",
    "country_code",
    "country_name",
    "location_status",
    "location_en",
    "location_ru",
    "google_maps_url",
    "osm_url",
]


def format_connection_labels(
    tower_ids: Sequence[int],
    towers_by_id: Mapping[int, TowerRecord],
) -> str:
    """Convert tower ids into a deterministic, user-facing label list."""

    if not tower_ids:
        return ""

    return ", ".join(
        format_display_label(towers_by_id[tower_id])
        for tower_id in sorted(
            tower_ids,
            key=lambda tower_id: towers_by_id[tower_id].label.lower(),
        )
    )


def humanize_tower_code(source: str, tower_id: int, installed: bool) -> str:
    """Build a clean typed code for user-facing tables."""

    if installed and source == "seed":
        return "Installed Seed"
    if installed and source == "mqtt":
        return "Installed MQTT"

    source_titles = {
        "coarse": "Coarse",
        "route": "Route",
        "cluster_slim": "Cluster",
        "population": "Population",
        "bridge": "Bridge",
        "greedy": "Greedy",
        "seed": "Seed",
    }
    source_title = source_titles.get(source, source.replace("_", " ").title())

    return f"{source_title} {tower_id}"


def build_display_name(
    *,
    tower: TowerRecord,
    place_name: str | None,
    road_name: str | None,
) -> str:
    """Build a cleaner location-first display title for the handout."""

    normalized_place = (place_name or "").strip()
    normalized_road = (road_name or "").strip()

    if tower.installed and tower.source == "seed" and "#" not in tower.label:
        return tower.label

    if normalized_place and normalized_road and normalized_place != normalized_road:
        return f"{normalized_place} / {normalized_road}"
    if normalized_place:
        return normalized_place
    if normalized_road:
        return normalized_road

    return humanize_tower_code(tower.source, tower.tower_id, tower.installed)


def format_display_label(tower: TowerRecord) -> str:
    """Format one compact label for lists of connected towers."""

    display_name = (tower.display_name or "").strip()
    display_code = (tower.display_code or "").strip()

    if display_name and display_code and display_name != display_code:
        return f"{display_name} ({display_code})"
    if display_name:
        return display_name
    if display_code:
        return display_code

    return tower.label


def format_location_description(
    locale: str,
    road_name: str | None,
    place_name: str | None,
    admin_context: Mapping[str, str | None],
    lon: float,
    lat: float,
) -> str:
    """Build a field-friendly location description that avoids housenumber wording."""

    normalized_road = (road_name or "").strip()
    normalized_place = (place_name or "").strip()
    normalized_admin = [
        (admin_context.get(key) or "").strip()
        for key in ("city", "district", "province", "country")
    ]
    admin_parts = [
        item
        for index, item in enumerate(normalized_admin)
        if item and item not in normalized_admin[:index]
    ]
    pieces: list[str] = []

    if normalized_place:
        pieces.append(normalized_place)

    if normalized_road and normalized_road != normalized_place:
        if locale == "ru":
            pieces.append(f"рядом с {normalized_road}")
        else:
            pieces.append(f"near {normalized_road}")

    for admin_part in admin_parts:
        if admin_part not in pieces:
            pieces.append(admin_part)

    if pieces:
        return ", ".join(pieces)

    return f"{lat:.5f}, {lon:.5f}"


def google_maps_url(lon: float, lat: float) -> str:
    """Build a Google Maps deep link."""

    return f"https://maps.google.com/?q={lat:.6f},{lon:.6f}"


def osm_url(lon: float, lat: float) -> str:
    """Build an OpenStreetMap deep link."""

    return (
        "https://www.openstreetmap.org/"
        f"?mlat={lat:.6f}&mlon={lon:.6f}#map=14/{lat:.6f}/{lon:.6f}"
    )


def render_html_document(
    rows: Sequence[Mapping[str, object]],
    generated_at: str,
    geocoder_base_url: str,
    cluster_bound_features: Sequence[Mapping[str, object]] | None = None,
    seed_mqtt_points: Sequence[Mapping[str, object]] | None = None,
    seed_mqtt_links: Sequence[Mapping[str, object]] | None = None,
) -> str:
    """Render a compact, mobile-friendly HTML handout."""

    normalized_rows = normalize_rows(rows)
    grouped_rows: dict[str, list[Mapping[str, object]]] = defaultdict(list)

    for row in normalized_rows:
        grouped_rows[str(row["cluster_label"])].append(row)

    summary_rows = [
        row
        for row in normalized_rows
        if bool(row["is_next_for_cluster"])
    ]

    html_parts = [
        "<!doctype html>",
        "<html lang='en'>",
        "<head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<link rel='icon' href='data:,'>",
        "<title>Installer Priority Handout</title>",
        "<style>",
        "body{font-family:'Trebuchet MS','Segoe UI',sans-serif;margin:0;background:linear-gradient(180deg,#efe7d7 0%,#f7f4ee 45%,#fcfbf8 100%);color:#1f2328;}",
        ".page{max-width:1360px;margin:0 auto;padding:20px;}",
        ".hero{background:linear-gradient(135deg,#0e3049 0%,#21516d 55%,#446d57 100%);color:#fff;border-radius:22px;padding:20px 22px;margin-bottom:18px;}",
        ".hero h1{margin:0 0 8px;font-size:1.9rem;}",
        ".hero p{margin:6px 0;line-height:1.45;}",
        ".summary,.cluster,.map-panel{background:#fff;border-radius:18px;padding:18px;margin-bottom:18px;box-shadow:0 10px 30px rgba(23,50,77,0.08);}",
        ".cluster h2,.summary h2,.map-panel h2{margin:0 0 10px;font-size:1.3rem;}",
        ".meta{color:#55606d;font-size:0.95rem;margin:6px 0 0;}",
        ".overview-map{height:480px;border-radius:16px;overflow:hidden;border:1px solid #d8d2c5;}",
        ".cluster-map{height:220px;border-radius:14px;overflow:hidden;border:1px solid #e2ddd3;margin:14px 0 10px;}",
        ".table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:14px;}",
        ".table-wrap:focus-visible,a:focus-visible{outline:3px solid #d97706;outline-offset:3px;}",
        "table{width:100%;border-collapse:collapse;min-width:980px;}",
        ".summary-table{min-width:760px;}",
        ".cluster-detail-table{min-width:980px;}",
        "caption{caption-side:top;padding:0 0 10px;text-align:left;}",
        "th,td{padding:10px 8px;border-bottom:1px solid #e6e1d8;vertical-align:top;text-align:left;}",
        "thead th{font-size:0.82rem;text-transform:uppercase;letter-spacing:0.03em;color:#5a6673;background:#fbfaf7;position:sticky;top:0;}",
        "tbody th{font-weight:700;color:#1f2328;background:transparent;position:static;text-transform:none;letter-spacing:0;}",
        ".name-header{font-size:1rem;text-transform:none;letter-spacing:0;background:transparent;color:#1f2328;}",
        "tr.next-row{background:#fff8de;}",
        "tr.installed-row{background:#f3f7fb;}",
        ".pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:0.82rem;font-weight:600;background:#e7edf4;color:#17324d;}",
        ".pill.next{background:#ffe28a;color:#5b4200;}",
        ".pill.planned{background:#e2f0df;color:#2f6130;}",
        ".pill.installed{background:#dce8f4;color:#1f4056;}",
        ".node-title{font-weight:700;line-height:1.3;}",
        ".node-subtitle{color:#63707d;font-size:0.86rem;margin-top:2px;}",
        ".maps{display:flex;flex-wrap:wrap;gap:8px;}",
        ".maps a{white-space:nowrap;}",
        ".legend{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0 0;}",
        ".legend span{display:inline-flex;align-items:center;gap:6px;color:#55606d;font-size:0.9rem;}",
        ".legend i{display:inline-block;width:10px;height:10px;border-radius:50%;}",
        ".legend .line-sample{width:18px;height:0;border-radius:0;background:transparent;border-top:3px dashed #7a8694;}",
        ".legend .bounds-sample{width:18px;height:10px;border-radius:3px;background:rgba(139,94,60,0.08);border:2px solid rgba(139,94,60,0.7);}",
        ".map-note{margin:10px 0 0;color:#55606d;font-size:0.92rem;line-height:1.45;}",
        ".map-fallback{display:none;padding:12px 0;color:#6d5f50;font-size:0.95rem;}",
        ".order-marker{width:20px;height:20px;border-radius:50%;display:flex;align-items:center;justify-content:center;font:700 11px/1 'Trebuchet MS','Segoe UI',sans-serif;color:#fff;border:2px solid rgba(255,255,255,0.95);box-shadow:0 1px 6px rgba(0,0,0,0.25);pointer-events:none;}",
        ".order-marker.overview{width:18px;height:18px;font-size:10px;}",
        ".order-marker.cluster{width:12px;height:12px;font-size:7px;border-width:1px;box-shadow:0 1px 3px rgba(0,0,0,0.18);}",
        ".order-marker.installed{background:#27548a;}",
        ".order-marker.next{background:#d97706;}",
        ".order-marker.planned{background:#4b8b3b;}",
        ".cluster-cards{display:none;}",
        ".cluster-card-list{list-style:none;margin:0;padding:0;display:grid;gap:12px;}",
        ".cluster-card{border:1px solid #e6e1d8;border-radius:16px;padding:14px;background:#fbfaf7;box-shadow:0 4px 14px rgba(23,50,77,0.05);}",
        ".cluster-card.next-row{background:#fff8de;}",
        ".cluster-card.installed-row{background:#f3f7fb;}",
        ".cluster-card-header{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:8px;}",
        ".cluster-rank{margin:0;font-weight:700;color:#17324d;}",
        ".cluster-card-title{margin:0 0 4px;font-size:1.02rem;line-height:1.35;}",
        ".cluster-card-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px 14px;margin:12px 0;}",
        ".cluster-card-grid dt{font-size:0.76rem;text-transform:uppercase;letter-spacing:0.03em;color:#5a6673;}",
        ".cluster-card-grid dd{margin:4px 0 0;line-height:1.4;}",
        ".sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0;}",
        "a{color:#0d5ea8;text-decoration:none;}",
        "a:hover{text-decoration:underline;}",
        ".maplibregl-popup-content{max-width:280px;font:14px/1.4 'Trebuchet MS','Segoe UI',sans-serif;}",
        "@media (max-width: 920px){.page{padding:14px}.hero,.summary,.cluster,.map-panel{padding:14px}.hero h1{font-size:1.6rem}.overview-map{height:360px}.cluster-map{height:220px}.cluster-table-wrap{display:none}.cluster-cards{display:block}}",
        "@media (max-width: 640px){.overview-map{height:300px}.cluster-map{height:220px}.cluster-card{padding:12px}.cluster-card-grid{grid-template-columns:1fr}.legend span{font-size:0.84rem}.summary-table{min-width:680px}}",
        "</style>",
        "</head>",
        "<body>",
        "<div class='page'>",
        "<section class='hero'>",
        "<h1>Installer Priority Handout</h1>",
        "<p>Separate next-step queues are shown per currently installed seed cluster.</p>",
        "<p>Impact is estimated as newly reachable people based on nearby populated localities behind each next step.</p>",
        f"<p>Generated at {escape(generated_at)}. Admin context comes from {escape(geocoder_base_url)} when available.</p>",
        "</section>",
        "<section class='map-panel'>",
        "<h2>Overview Map</h2>",
        "<div id='overview-map' class='overview-map' role='img' aria-label='Overview rollout map for all installer clusters'></div>",
        "<div id='map-fallback' class='map-fallback' role='status' aria-live='polite'>Interactive map could not load. The tables below still contain the full handout.</div>",
        "<div class='legend'>",
        "<span><i style='background:#27548a'></i>Installed seed</span>",
        "<span><i style='background:#d97706'></i>Next suggested node</span>",
        "<span><i style='background:#4b8b3b'></i>Planned node</span>",
        "<span><i class='bounds-sample'></i>Cluster bounds</span>",
        "<span><i class='line-sample'></i>Cheapest cluster connector</span>",
        "</div>",
        "<p class='map-note'>The overview map now shows every local order badge and an outline around each rollout cluster. Mini maps show the same order directly on the nodes. Follow the solid line from the installed seed toward rank 1, then 2, then onward. Dashed gray lines show the cheapest visible connector between rollout clusters. Use the fullscreen button when a team needs to inspect one route in detail.</p>",
        "</section>",
    ]
    html_parts.extend(render_summary_section(summary_rows))

    for cluster_label in sorted(grouped_rows, key=str.lower):
        cluster_rows = grouped_rows[cluster_label]
        installed_labels = [
            str(row["display_name"])
            for row in cluster_rows
            if bool(row["installed"])
        ]
        next_rows = [
            row
            for row in cluster_rows
            if bool(row["is_next_for_cluster"])
        ]
        next_label = (
            str(next_rows[0]["display_name"])
            if next_rows
            else "No reachable next node"
        )
        blocked_rows = [
            row for row in cluster_rows if str(row["rollout_status"]) == "blocked"
        ]
        cluster_dom_id = cluster_map_id(str(cluster_rows[0]["cluster_key"]))
        html_parts.extend(
            render_cluster_section(
                cluster_label=cluster_label,
                cluster_rows=cluster_rows,
                cluster_dom_id=cluster_dom_id,
                installed_labels=installed_labels,
                next_label=next_label,
                blocked_count=len(blocked_rows),
            )
        )

    html_parts.extend(
        render_map_assets(
            normalized_rows,
            cluster_bound_features=cluster_bound_features,
            mqtt_points=seed_mqtt_points,
            seed_mqtt_links=seed_mqtt_links,
        )
    )
    html_parts.extend(["</div>", "</body>", "</html>"])

    return "\n".join(html_parts)
