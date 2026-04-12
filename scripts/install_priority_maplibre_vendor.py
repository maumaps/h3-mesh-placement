"""Load vendored MapLibre assets for the standalone installer handout."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def read_maplibre_assets() -> tuple[str, str]:
    """Return vendored MapLibre CSS and JS for inlining into the handout."""

    vendor_dir = Path(__file__).resolve().parent.parent / "vendor" / "maplibre-gl"
    css_path = vendor_dir / "maplibre-gl.css"
    js_path = vendor_dir / "maplibre-gl.js"

    if not css_path.exists() or not js_path.exists():
        raise FileNotFoundError(
            "Vendored MapLibre assets are missing. "
            f"Expected both {css_path} and {js_path} to exist."
        )

    return css_path.read_text(encoding="utf-8"), js_path.read_text(encoding="utf-8")

