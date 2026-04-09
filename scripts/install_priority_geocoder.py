"""
Hosted reverse-geocoder helpers for the installer-priority export.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable
from urllib import error, request

try:
    from scripts.install_priority_graph import PlanRow
except ModuleNotFoundError:
    from install_priority_graph import PlanRow  # type: ignore[no-redef]


def fetch_geocoder_reverse(
    lon: float,
    lat: float,
    locale: str,
    geocoder_base_url: str,
    radius_m: int,
    timeout_s: int,
) -> tuple[dict[str, Any] | None, str]:
    """Call the hosted geocoder reverse endpoint with browser-like headers."""

    payload = json.dumps(
        {
            "lon": lon,
            "lat": lat,
            "locale": locale,
            "radius_m": radius_m,
            "debug": False,
        }
    ).encode("utf-8")
    geocoder_url = geocoder_base_url.rstrip("/") + "/v1/reverse"
    geocoder_request = request.Request(
        geocoder_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; maumap-install-priority/1.0)",
        },
        method="POST",
    )

    try:
        with request.urlopen(geocoder_request, timeout=timeout_s) as response:
            return json.loads(response.read().decode("utf-8")), "ok"
    except error.HTTPError as exc:
        return None, f"http_{exc.code}"
    except error.URLError as exc:
        return None, f"url_error:{exc.reason}"
    except TimeoutError:
        return None, "timeout"


def fetch_geocoder_batch(
    plan_rows: Iterable[PlanRow],
    geocoder_base_url: str,
    radius_m: int,
    timeout_s: int,
) -> dict[tuple[float, float, str], tuple[dict[str, Any] | None, str]]:
    """Fetch reverse-geocoder results in parallel and dedupe repeated points."""

    request_keys = sorted(
        {
            (round(plan_row.lon, 6), round(plan_row.lat, 6), locale)
            for plan_row in plan_rows
            for locale in ("en", "ru")
        }
    )
    results: dict[tuple[float, float, str], tuple[dict[str, Any] | None, str]] = {}

    if not request_keys:
        return results

    max_workers = min(4, len(request_keys))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(
                fetch_geocoder_reverse,
                lon=lon,
                lat=lat,
                locale=locale,
                geocoder_base_url=geocoder_base_url,
                radius_m=radius_m,
                timeout_s=timeout_s,
            ): (lon, lat, locale)
            for lon, lat, locale in request_keys
        }

        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                results[key] = future.result()
            except Exception as exc:  # pragma: no cover - defensive safety net
                results[key] = (None, f"exception:{type(exc).__name__}")

    return results


def extract_admin_context(payload: dict[str, Any] | None) -> dict[str, str | None]:
    """Extract admin-oriented reverse-geocoder fields without housenumber noise."""

    if not payload:
        return {
            "city": None,
            "district": None,
            "province": None,
            "country": None,
        }

    exact_data = payload.get("exact") or {}
    approx_data = payload.get("approx") or {}

    return {
        "city": exact_data.get("addr:city") or approx_data.get("addr:city"),
        "district": exact_data.get("addr:district") or approx_data.get("addr:district"),
        "province": exact_data.get("addr:province") or approx_data.get("addr:province"),
        "country": exact_data.get("addr:country") or approx_data.get("addr:country"),
    }
