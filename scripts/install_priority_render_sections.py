"""
HTML section helpers for the installer-priority handout.
"""

from __future__ import annotations

from html import escape
from typing import Mapping, Sequence


def summarize_connection_list(raw_text: object, *, max_items: int = 3) -> str:
    """Shorten long unlock lists so the HTML stays readable on smaller screens."""

    normalized_text = str(raw_text or "").strip()

    if not normalized_text:
        return ""

    items = [item.strip() for item in normalized_text.split(", ") if item.strip()]

    if len(items) <= max_items:
        return ", ".join(items)

    remaining_count = len(items) - max_items

    return f"{', '.join(items[:max_items])} + {remaining_count} more"


def render_summary_section(summary_rows: Sequence[Mapping[str, object]]) -> list[str]:
    """Render the top summary table with accessible table semantics."""

    html_parts = [
        "<section class='summary' aria-label='Next Node Per Cluster'>",
        "<h2>Next Node Per Cluster</h2>",
        "<div class='table-wrap summary-wrap' role='region' aria-label='Next Node Per Cluster' tabindex='0'>",
        "<table class='summary-table'>",
        "<caption class='sr-only'>Next suggested node for each currently active rollout cluster.</caption>",
        "<thead><tr><th scope='col'>Cluster</th><th scope='col'>Next node</th><th scope='col'>Est. New Reach</th><th scope='col'>Unlocks</th><th scope='col'>Connects to now</th><th scope='col'>Location</th><th scope='col'>Maps</th></tr></thead>",
        "<tbody>",
    ]

    for row in sorted(summary_rows, key=lambda item: str(item["cluster_label"]).lower()):
        node_label = str(row["display_name"])
        html_parts.extend(
            [
                "<tr>",
                f"<th scope='row'>{escape(str(row['cluster_label']))}</th>",
                "<td>"
                f"<div class='node-title'>{escape(node_label)}</div>"
                f"<div class='node-subtitle'>{escape(str(row['display_type']))}</div>"
                "</td>",
                f"<td>{escape(str(row['impact_people_est']))}</td>",
                f"<td>{escape(str(row['impact_tower_count']))} towers</td>",
                f"<td>{escape(str(row['previous_connections']))}</td>",
                f"<td>{escape(str(row['location_en']))}</td>",
                "<td class='maps'>"
                f"{_map_links_html(row, node_label)}"
                "</td>",
                "</tr>",
            ]
        )

    html_parts.extend(["</tbody></table></div></section>"])

    return html_parts


def render_cluster_section(
    *,
    cluster_label: str,
    cluster_rows: Sequence[Mapping[str, object]],
    cluster_dom_id: str,
    installed_labels: Sequence[str],
    next_label: str,
    blocked_count: int,
) -> list[str]:
    """Render one cluster section with desktop table and mobile cards."""

    heading_id = f"{cluster_dom_id}-heading"
    table_region_id = f"{cluster_dom_id}-table"
    cards_region_id = f"{cluster_dom_id}-cards"
    html_parts = [
        "<section class='cluster' aria-labelledby='{heading_id}'>".format(
            heading_id=escape(heading_id)
        ),
        f"<h2 id='{escape(heading_id)}'>{escape(cluster_label)}</h2>",
        f"<p class='meta'>Installed seeds: {escape(', '.join(installed_labels) or 'None')}.</p>",
        f"<p class='meta'>Next suggested node: {escape(next_label)}.</p>",
        (
            f"<p class='meta'>Blocked later in this queue: {blocked_count}. "
            "These towers stay attached to this rollout queue, "
            "but they still need a visible path from an installed seed before they can be installed.</p>"
            if blocked_count
            else ""
        ),
        (
            f"<div id='{escape(cluster_dom_id)}' class='cluster-map' role='img' "
            f"aria-label='Map for {escape(cluster_label)} rollout cluster'></div>"
        ),
        (
            f"<div class='table-wrap cluster-table-wrap' id='{escape(table_region_id)}' "
            f"role='region' aria-labelledby='{escape(heading_id)}' tabindex='0'>"
        ),
        "<table class='cluster-detail-table'>",
        f"<caption class='sr-only'>Detailed rollout order for {escape(cluster_label)}.</caption>",
        "<thead><tr><th scope='col'>Rank</th><th scope='col'>Status</th><th scope='col'>Name</th><th scope='col'>Type</th><th scope='col'>Est. New Reach</th><th scope='col'>Unlocks</th><th scope='col'>Connects to now</th><th scope='col'>Location EN</th><th scope='col'>Location RU</th><th scope='col'>Maps</th></tr></thead>",
        "<tbody>",
    ]

    for row in cluster_rows:
        row_classes = _row_classes(row)
        node_label = str(row["display_name"])
        blocked_reason = str(row["blocked_reason"]).strip()
        rank_text = _rank_text(row)
        html_parts.extend(
            [
                f"<tr class='{escape(row_classes)}'>",
                f"<td><span class='sr-only'>Rank </span>{escape(rank_text)}</td>",
                (
                    "<td>"
                    f"<span class='pill {escape(str(row['rollout_status']))}' "
                    f"aria-label='Rollout status {escape(str(row['rollout_status']))}'>"
                    f"{escape(str(row['rollout_status']))}</span></td>"
                ),
                "<th scope='row' class='name-header'>"
                f"<div class='node-title'>{escape(node_label)}</div>"
                + (
                    f"<div class='node-subtitle blocked-note'>{escape(blocked_reason)}</div>"
                    if blocked_reason
                    else ""
                )
                + "</th>",
                f"<td><div class='node-subtitle'>{escape(str(row['display_type']))}</div></td>",
                f"<td>{escape(str(row['impact_people_est']))}</td>",
                (
                    "<td>"
                    f"{escape(summarize_connection_list(row['next_connections']))}"
                    f"<div class='node-subtitle'>{escape(str(row['impact_tower_count']))} downstream towers</div>"
                    "</td>"
                ),
                f"<td>{escape(str(row['previous_connections']))}</td>",
                f"<td>{escape(str(row['location_en']))}</td>",
                f"<td>{escape(str(row['location_ru']))}</td>",
                "<td class='maps'>"
                f"{_map_links_html(row, node_label)}"
                "</td>",
                "</tr>",
            ]
        )

    html_parts.extend(
        [
            "</tbody></table></div>",
            (
                f"<div class='cluster-cards' id='{escape(cards_region_id)}' "
                f"aria-labelledby='{escape(heading_id)}'>"
            ),
            "<ul class='cluster-card-list'>",
        ]
    )

    for row in cluster_rows:
        row_classes = _row_classes(row)
        node_label = str(row["display_name"])
        blocked_reason = str(row["blocked_reason"]).strip()
        rank_text = _rank_text(row)
        html_parts.extend(
            [
                f"<li class='cluster-card {escape(row_classes)}'>",
                "<div class='cluster-card-header'>",
                f"<p class='cluster-rank'>Rank {escape(rank_text)}</p>",
                (
                    f"<span class='pill {escape(str(row['rollout_status']))}' "
                    f"aria-label='Rollout status {escape(str(row['rollout_status']))}'>"
                    f"{escape(str(row['rollout_status']))}</span>"
                ),
                "</div>",
                f"<h3 class='cluster-card-title'>{escape(node_label)}</h3>",
                f"<p class='node-subtitle'>{escape(str(row['display_type']))}</p>",
                (
                    f"<p class='node-subtitle blocked-note'>{escape(blocked_reason)}</p>"
                    if blocked_reason
                    else ""
                ),
                "<dl class='cluster-card-grid'>",
                "<div><dt>Est. New Reach</dt>"
                f"<dd>{escape(str(row['impact_people_est']))}</dd></div>",
                "<div><dt>Unlocks</dt>"
                f"<dd>{escape(summarize_connection_list(row['next_connections'])) or 'None'}</dd></div>",
                "<div><dt>Downstream towers</dt>"
                f"<dd>{escape(str(row['impact_tower_count']))}</dd></div>",
                "<div><dt>Connects to now</dt>"
                f"<dd>{escape(str(row['previous_connections']) or 'None')}</dd></div>",
                "<div><dt>Location EN</dt>"
                f"<dd>{escape(str(row['location_en']))}</dd></div>",
                "<div><dt>Location RU</dt>"
                f"<dd>{escape(str(row['location_ru']))}</dd></div>",
                "</dl>",
                f"<div class='maps'>{_map_links_html(row, node_label)}</div>",
                "</li>",
            ]
        )

    html_parts.extend(["</ul></div></section>"])

    return html_parts


def _map_links_html(row: Mapping[str, object], node_label: str) -> str:
    """Build accessible map links for one row."""

    google_url = escape(str(row["google_maps_url"]))
    osm_target = escape(str(row["osm_url"]))
    label = escape(node_label)

    return (
        f"<a href='{google_url}'>Google<span class='sr-only'> map for {label}</span></a>"
        f"<a href='{osm_target}'>OSM<span class='sr-only'> map for {label}</span></a>"
    )


def _rank_text(row: Mapping[str, object]) -> str:
    """Format the cluster-local rank."""

    if row["cluster_install_rank"] in (None, ""):
        return ""

    return str(row["cluster_install_rank"])


def _row_classes(row: Mapping[str, object]) -> str:
    """Build deterministic CSS classes for one rendered row/card."""

    css_classes: list[str] = []

    if bool(row["is_next_for_cluster"]):
        css_classes.append("next-row")
    if bool(row["installed"]):
        css_classes.append("installed-row")

    return " ".join(css_classes)
