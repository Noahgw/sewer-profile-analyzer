"""
Sewer Profile Analyzer — Solara Web App

Reactive GIS-style interface with fine-grained updates.
No full-page reruns — only changed components re-render.

Run with: solara run sol.py
"""

import solara
import pandas as pd
import geopandas as gpd
import tempfile
import os
import sys
import io
import zipfile
import json
import base64
import plotly.graph_objects as go
import ipyleaflet
import ipyvuetify as v
from pathlib import Path
from collections import Counter

# Add parent dir so we can import src/ modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.network_builder import build_network
from src.profile_analyzer import run_full_analysis, trace_profile, ProfileIssue

# ── Local modules ──
from ingest_sol import (
    auto_detect_fields, get_required_fields, load_field_config,
    ingest_gdf, read_shapefile_from_path, read_shapefile_from_bytes,
)
from map_sol import (
    build_leaflet_map, ISSUE_COLORS, ISSUE_DISPLAY_NAMES,
    _last_map_widget, _ensure_wgs84, _get_center_zoom,
)
from fix_toolkit_sol import (
    LedgerEntry, apply_group, undo_last_group, get_current_value,
    get_all_edits, ledger_summary, get_strategies, compute_fix,
    junction_invert_from_lowest_pipe,
    CONNECTIVITY_STRATEGIES, compute_connectivity_entries,
)
from file_upload_widget import FileUploadWidget


# ════════════════════════════════════════════════════════════
# REACTIVE STATE
# ════════════════════════════════════════════════════════════

# File data (GeoDataFrames keyed by feature type)
gdfs = solara.reactive({})
mappings = solara.reactive({})
ingestion = solara.reactive({})
network = solara.reactive(None)
analysis = solara.reactive(None)

# Map interaction state
map_selection = solara.reactive(set())
inspected_feature = solara.reactive(None)

# Fix state
edit_ledger = solara.reactive([])
preview_entries = solara.reactive(None)
min_slope_setting = solara.reactive(0.005)  # m/m — user-configurable
# Pending connectivity fix: {strategy_key, issue, base_entries, connectivity_entries, description}
pending_connectivity_fix = solara.reactive(None)

# UI state
show_profile = solara.reactive(False)
analysis_running = solara.reactive(False)

# Settings
snap_tolerance = solara.reactive(1.0)
invert_tolerance = solara.reactive(0.01)
min_depth = solara.reactive(0.6)
max_depth = solara.reactive(10.0)

# Layer visibility
vis_pipes = solara.reactive(True)
vis_junctions = solara.reactive(True)
vis_arrows = solara.reactive(True)
vis_pumps = solara.reactive(True)
vis_storage = solara.reactive(True)

# Filter
filter_types = solara.reactive([])

# CRS
selected_crs = solara.reactive(None)

# Field units — keyed by "feature_type.field_name", e.g. "pipes.diameter" -> "m"
field_units = solara.reactive({})

# Which fields support unit selection and their available options
# The first option is the default, internal unit is always metric (m/mm)
FIELD_UNIT_OPTIONS = {
    "diameter":     ["mm", "m", "in", "ft"],
    "us_invert":    ["m", "ft"],
    "ds_invert":    ["m", "ft"],
    "length":       ["m", "ft"],
    "rim_elev":     ["m", "ft"],
    "invert_elev":  ["m", "ft"],
}

def _convert_to_metric(value, field_name, feature_type):
    """Convert a field value from its configured unit to metric (m for lengths/elevations, mm for diameter)."""
    if value is None:
        return None
    val = float(value)
    key = f"{feature_type}.{field_name}"
    unit = field_units.value.get(key)
    if unit is None:
        # Use default (first option)
        unit = FIELD_UNIT_OPTIONS.get(field_name, ["m"])[0]

    if field_name == "diameter":
        # Target: mm
        if unit == "mm":
            return val
        elif unit == "m":
            return val * 1000.0
        elif unit == "in":
            return val * 25.4
        elif unit == "ft":
            return val * 304.8
    else:
        # Target: m (for elevations and lengths)
        if unit == "m":
            return val
        elif unit == "ft":
            return val * 0.3048
    return val

def _metric_to_data_unit(value_m, field_name, feature_type):
    """Convert a metric threshold (m) to the user's configured data unit for comparison with raw data."""
    if value_m is None:
        return None
    key = f"{feature_type}.{field_name}"
    unit = field_units.value.get(key)
    if unit is None:
        unit = FIELD_UNIT_OPTIONS.get(field_name, ["m"])[0]
    if unit == "ft":
        return value_m / 0.3048
    return value_m  # already metric


# Path to test data (used by Load Test Data button)
_test_data_dir = Path(__file__).resolve().parent.parent / "data" / "Langford"


# ════════════════════════════════════════════════════════════
# CUSTOM CSS
# ════════════════════════════════════════════════════════════

custom_css = """
<style>
/* ═══ LIGHT BUBBLE THEME ═══ */

:root {
    --bg-deep: #f0f2f7;
    --bg-surface: #ffffff;
    --bg-card: #ffffff;
    --bg-hover: rgba(74, 112, 235, 0.06);
    --bg-sidebar: #1e293b;
    --bg-sidebar-surface: rgba(30,41,59,0.95);
    --border-subtle: rgba(0,0,0,0.08);
    --border-glow: rgba(74, 112, 235, 0.3);
    --accent: #4a70eb;
    --accent-bright: #5b82f7;
    --accent-soft: rgba(74, 112, 235, 0.1);
    --text-primary: #1a202c;
    --text-secondary: #4a5568;
    --text-muted: #718096;
    --text-sidebar: #e2e8f0;
    --text-sidebar-secondary: #94a3b8;
    --text-sidebar-muted: #64748b;
    --danger: #e53e3e;
    --warning: #dd6b20;
    --success: #38a169;
    --info: #3182ce;
    --bubble-radius: 16px;
    --bubble-radius-sm: 12px;
}

/* --- Global --- */
.v-application {
    background: var(--bg-deep) !important;
    color: var(--text-primary) !important;
    font-family: 'Inter', 'SF Pro Display', -apple-system, BlinkMacSystemFont, sans-serif !important;
}
body { background: var(--bg-deep) !important; }

/* --- Sidebar (stays dark) --- */
.v-navigation-drawer {
    background: linear-gradient(180deg, #1e293b 0%, #1a2332 100%) !important;
    border-right: 1px solid rgba(255,255,255,0.06) !important;
    color: var(--text-sidebar) !important;
    padding-bottom: 60px !important;
}
.v-navigation-drawer .v-input .v-label { color: var(--text-sidebar-muted) !important; }
.v-navigation-drawer .v-input--checkbox .v-label { color: var(--text-sidebar) !important; }
.v-navigation-drawer .v-text-field input { color: var(--text-sidebar) !important; }
.v-navigation-drawer .v-select .v-select__selection { color: var(--text-sidebar) !important; }
.v-navigation-drawer .v-select .v-input__icon .v-icon { color: var(--text-sidebar-muted) !important; }
.v-navigation-drawer .v-btn--outlined { border-color: rgba(255,255,255,0.12) !important; color: var(--text-sidebar-secondary) !important; }
.v-navigation-drawer .v-btn--outlined:hover { background: rgba(74,112,235,0.15) !important; border-color: var(--accent-bright) !important; color: #fff !important; }
.v-navigation-drawer hr { border-color: rgba(255,255,255,0.08) !important; }

/* --- Tab content panels --- */
.v-window, .v-window__container, .v-window-item {
    background: transparent !important;
}
.v-tabs-items { background: transparent !important; }

/* --- Cards (light bubble) --- */
.v-card {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-subtle) !important;
    border-radius: var(--bubble-radius) !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.04) !important;
    transition: border-color 0.25s ease, box-shadow 0.25s ease !important;
}
.v-card:hover {
    border-color: var(--border-glow) !important;
    box-shadow: 0 4px 20px rgba(74,112,235,0.1), 0 2px 6px rgba(0,0,0,0.06) !important;
}

/* --- Tabs (main content) --- */
.v-tabs { background: transparent !important; }
.v-tab {
    color: var(--text-muted) !important;
    font-weight: 600 !important;
    letter-spacing: 0.3px !important;
    text-transform: none !important;
    border-radius: var(--bubble-radius-sm) var(--bubble-radius-sm) 0 0 !important;
    transition: color 0.2s, background 0.2s !important;
}
.v-tab:hover { color: var(--text-primary) !important; background: var(--bg-hover) !important; }
.v-tab--active { color: var(--accent) !important; background: var(--accent-soft) !important; }
.v-tabs-slider { background: var(--accent) !important; border-radius: 2px !important; }

/* --- Sidebar tabs override --- */
.v-navigation-drawer .v-tab { color: var(--text-sidebar-muted) !important; }
.v-navigation-drawer .v-tab:hover { color: var(--text-sidebar) !important; background: rgba(255,255,255,0.05) !important; }
.v-navigation-drawer .v-tab--active { color: var(--accent-bright) !important; background: rgba(74,112,235,0.15) !important; }

/* --- Buttons --- */
.v-btn--outlined {
    border-color: var(--border-subtle) !important;
    color: var(--text-secondary) !important;
    border-radius: var(--bubble-radius-sm) !important;
    transition: all 0.2s ease !important;
}
.v-btn--outlined:hover {
    background: var(--accent-soft) !important;
    border-color: var(--accent) !important;
    color: var(--accent) !important;
}
.v-btn.primary, .v-btn.v-btn--contained.primary {
    background: linear-gradient(135deg, #4a70eb 0%, #5b82f7 100%) !important;
    border-radius: var(--bubble-radius-sm) !important;
    box-shadow: 0 3px 12px rgba(74,112,235,0.3) !important;
    text-transform: none !important;
    font-weight: 600 !important;
}
.v-btn.primary:hover { box-shadow: 0 5px 20px rgba(74,112,235,0.4) !important; }

/* --- Text fields / Selects (main content) --- */
.v-text-field .v-input__slot, .v-select .v-input__slot {
    background: #f7f8fc !important;
    border-radius: var(--bubble-radius-sm) !important;
}
.v-text-field input, .v-select .v-select__selection { color: var(--text-primary) !important; }
.v-input .v-label { color: var(--text-muted) !important; }
.v-text-field > .v-input__control > .v-input__slot::before { border-color: var(--border-subtle) !important; }
.v-text-field > .v-input__control > .v-input__slot::after { border-color: var(--accent) !important; }

/* --- Sidebar text fields override --- */
.v-navigation-drawer .v-text-field .v-input__slot,
.v-navigation-drawer .v-select .v-input__slot {
    background: rgba(15,23,42,0.6) !important;
}

/* --- Data tables --- */
.v-data-table {
    background: #fff !important;
    color: var(--text-primary) !important;
    border-radius: var(--bubble-radius) !important;
    overflow: hidden !important;
    border: 1px solid var(--border-subtle) !important;
}
.v-data-table th {
    background: #f7f8fc !important;
    color: var(--text-secondary) !important;
    border-bottom: 1px solid rgba(0,0,0,0.08) !important;
    font-weight: 600 !important; letter-spacing: 0.5px !important;
    text-transform: uppercase !important; font-size: 10px !important;
}
.v-data-table td { border-bottom: 1px solid rgba(0,0,0,0.04) !important; }
.v-data-table tr:hover td { background: var(--bg-hover) !important; }
.v-data-table .v-data-footer { background: #f7f8fc !important; color: var(--text-secondary) !important; }

/* --- Expansion panels --- */
.v-expansion-panel { background: var(--bg-card) !important; color: var(--text-primary) !important; border-radius: var(--bubble-radius-sm) !important; }

/* --- Alerts --- */
.v-alert {
    background: var(--bg-card) !important;
    border-radius: var(--bubble-radius-sm) !important;
    border: 1px solid var(--border-subtle) !important;
    color: var(--text-primary) !important;
}
.v-navigation-drawer .v-alert {
    background: rgba(15,23,42,0.4) !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
}

/* --- Chips --- */
.v-chip { border-radius: 20px !important; }

/* --- Tooltips / Menus / Dropdowns --- */
.v-tooltip__content {
    background: #fff !important; color: var(--text-primary) !important;
    border: 1px solid var(--border-subtle) !important;
    border-radius: var(--bubble-radius-sm) !important;
    box-shadow: 0 8px 30px rgba(0,0,0,0.12) !important;
}
.v-menu__content {
    background: #fff !important;
    border: 1px solid var(--border-subtle) !important;
    border-radius: var(--bubble-radius-sm) !important;
    box-shadow: 0 8px 30px rgba(0,0,0,0.12) !important;
}
.v-list { background: transparent !important; }
.v-list-item { color: var(--text-primary) !important; border-radius: 8px !important; margin: 2px 4px !important; }
.v-list-item:hover { background: var(--bg-hover) !important; }
.v-select-list { background: #fff !important; }
/* Sidebar dropdown override */
.v-navigation-drawer .v-select-list,
.v-navigation-drawer .v-menu__content { background: #1e293b !important; }
.v-navigation-drawer .v-list-item { color: var(--text-sidebar) !important; }

/* --- Sliders --- */
.v-slider__thumb { background: var(--accent) !important; }
.v-slider__track-fill { background: var(--accent) !important; }
.v-slider__track-background { background: rgba(0,0,0,0.1) !important; }
.v-navigation-drawer .v-slider__track-background { background: rgba(255,255,255,0.15) !important; }

/* --- Checkboxes --- */
.v-input--checkbox .v-icon { color: var(--accent) !important; }

/* --- Scrollbars --- */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(0,0,0,0.15); border-radius: 10px; }
::-webkit-scrollbar-thumb:hover { background: rgba(0,0,0,0.3); }

/* --- Dividers --- */
hr { border-color: var(--border-subtle) !important; margin: 12px 0 !important; }

/* --- Progress bar --- */
.v-progress-linear { border-radius: 8px !important; overflow: hidden !important; }

/* --- Sheets (sidebar fix) --- */
.v-sheet { background: transparent !important; }
.v-navigation-drawer .v-sheet { color: var(--text-sidebar) !important; }

/* ═══ METRIC CARDS ═══ */
.metric-bar {
    display: flex; gap: 10px; margin-bottom: 14px; margin-top: 0.25rem;
}
.metric-card {
    flex: 1;
    background: #fff;
    color: var(--text-primary);
    border-radius: var(--bubble-radius);
    padding: 14px 16px;
    text-align: center;
    border: 1px solid var(--border-subtle);
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    transition: transform 0.2s, box-shadow 0.2s;
}
.metric-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(0,0,0,0.1);
}
.metric-card .value { font-size: 28px; font-weight: 700; letter-spacing: -0.5px; color: var(--text-primary); }
.metric-card .label {
    font-size: 10px; text-transform: uppercase;
    letter-spacing: 1.5px; color: var(--text-muted); margin-top: 4px; font-weight: 500;
}
.metric-card.high { border-bottom: 3px solid var(--danger); }
.metric-card.high .value { color: var(--danger); }
.metric-card.medium { border-bottom: 3px solid var(--warning); }
.metric-card.medium .value { color: var(--warning); }
.metric-card.low { border-bottom: 3px solid var(--info); }
.metric-card.stat { border-bottom: 3px solid var(--accent); }

/* ═══ SEVERITY BADGES ═══ */
.sev-badge {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: 11px; font-weight: 600; margin-right: 10px; min-width: 55px;
    text-align: center; letter-spacing: 0.5px;
}
.sev-HIGH { background: rgba(229,62,62,0.1); color: var(--danger); border: 1px solid rgba(229,62,62,0.25); }
.sev-MEDIUM { background: rgba(221,107,32,0.1); color: var(--warning); border: 1px solid rgba(221,107,32,0.25); }
.sev-LOW { background: rgba(49,130,206,0.1); color: var(--info); border: 1px solid rgba(49,130,206,0.25); }

/* ═══ SIDEBAR SECTION HEADERS ═══ */
.sidebar-section-btn {
    font-weight: 600 !important; font-size: 13px !important;
    padding: 0 !important; justify-content: flex-start !important;
    text-transform: none !important; color: var(--text-sidebar) !important;
    width: 100% !important; letter-spacing: 0.3px !important;
}
.sidebar-section-btn:hover { color: var(--accent-bright) !important; }

/* ═══ SELECTION LIST ═══ */
.selection-list {
    max-height: 180px; overflow-y: auto;
    background: rgba(15,23,42,0.4);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: var(--bubble-radius-sm);
    padding: 4px; margin-bottom: 8px;
}
.selection-item {
    display: flex; align-items: center; gap: 2px;
    padding: 4px 8px;
    border-bottom: 1px solid rgba(255,255,255,0.05);
    border-radius: 6px;
    transition: background 0.15s;
    color: var(--text-sidebar);
}
.selection-item:hover { background: rgba(255,255,255,0.05); }
.selection-item:last-child { border-bottom: none; }

/* ═══ ISSUE FILTER CHIPS ═══ */
.issue-chip {
    display: inline-flex; align-items: center; gap: 4px;
    padding: 4px 10px; border-radius: 20px;
    font-size: 11px; cursor: pointer; margin: 2px;
    transition: all 0.2s ease;
    font-weight: 500; letter-spacing: 0.3px;
}
.issue-chip:hover { transform: scale(1.05); }
.issue-chip.active { box-shadow: 0 2px 8px rgba(0,0,0,0.15); }
.issue-chip.inactive { opacity: 0.4; }

/* ═══ FEATURE INSPECTOR ═══ */
.attr-row {
    display: flex; justify-content: space-between; padding: 5px 0;
    font-size: 13px; border-bottom: 1px solid rgba(0,0,0,0.05);
}
.attr-row:last-child { border-bottom: none; }
.attr-key { color: var(--text-muted); }
.attr-val { font-weight: 500; color: var(--text-primary); }
.issue-dot { font-size: 16px; margin-right: 8px; }

/* ═══ LANDING PAGE ═══ */
.landing-step {
    text-align: center; padding: 36px 20px;
    min-height: 180px;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
}
.landing-step-num {
    display: inline-flex; align-items: center; justify-content: center;
    width: 40px; height: 40px; border-radius: 50%;
    background: var(--accent-soft); color: var(--accent);
    font-weight: 700; font-size: 17px; margin-bottom: 14px;
    border: 1px solid rgba(74,112,235,0.2);
}
</style>
"""


# ════════════════════════════════════════════════════════════
# HELPER COMPONENTS
# ════════════════════════════════════════════════════════════

@solara.component
def MetricBar(issues, stats):
    """Compact metric cards bar."""
    total_issues = len(issues)
    issue_types = len(set(i.issue_type for i in issues)) if issues else 0

    html = '<div class="metric-bar">'
    html += f'<div class="metric-card stat"><div class="value">{stats["total_edges"]}</div><div class="label">Pipes</div></div>'
    html += f'<div class="metric-card stat"><div class="value">{stats["total_nodes"]}</div><div class="label">Nodes</div></div>'
    html += f'<div class="metric-card stat"><div class="value">{stats["connected_components"]}</div><div class="label">Components</div></div>'
    html += f'<div class="metric-card high"><div class="value">{total_issues}</div><div class="label">Issues Found</div></div>'
    html += f'<div class="metric-card medium"><div class="value">{issue_types}</div><div class="label">Issue Types</div></div>'
    html += '</div>'
    solara.HTML(unsafe_innerHTML=html)


@solara.component
def FileUploader(label, feature_type, on_upload):
    """File upload with click-to-browse and drag-and-drop for shapefiles (.zip)."""
    file_info, set_file_info = solara.use_state(None)
    error_info, set_error_info = solara.use_state(None)
    trigger, set_trigger = solara.use_state(0)

    widget = FileUploadWidget.element(
        label=label,
        accept=".zip",
        on_upload_trigger=set_trigger,
    )

    def process_upload():
        if trigger == 0:
            return
        w = solara.get_widget(widget)
        if not w or not w.file_data:
            return
        try:
            data = bytes(w.file_data)
            name = w.file_name or "upload.zip"
            gdf = read_shapefile_from_bytes(data, name)
            crs = selected_crs.value
            if crs:
                gdf = gdf.set_crs(crs, allow_override=True)
            on_upload(feature_type, gdf)
            set_file_info(f"{name} — {len(gdf)} features loaded")
            set_error_info(None)
        except Exception as e:
            set_error_info(str(e))
            set_file_info(None)

    solara.use_effect(process_upload, [trigger])

    if file_info:
        solara.HTML(
            unsafe_innerHTML=f'<div style="font-size:12px;color:var(--success);margin:2px 0 6px 0;">'
            f'{file_info}</div>'
        )
    if error_info:
        solara.HTML(
            unsafe_innerHTML=f'<div style="font-size:12px;color:var(--danger);margin:2px 0 6px 0;">'
            f'{error_info}</div>'
        )


@solara.component
def FieldMappingPanel(gdf, feature_type):
    """Field mapping UI for a single feature type."""
    config = load_field_config()
    source_fields = [c for c in gdf.columns if c != "geometry"]
    auto_mapping = auto_detect_fields(source_fields, feature_type, config)
    required = get_required_fields(feature_type)

    # Initialize mappings from auto-detect on first render (via use_effect)
    def init_mapping():
        if feature_type not in mappings.value:
            m = mappings.value.copy()
            m[feature_type] = {k: v for k, v in auto_mapping.items()}
            mappings.set(m)
    solara.use_effect(init_mapping, [feature_type])

    ftype_mapping = mappings.value.get(feature_type, auto_mapping)

    def update_field(internal_name, value):
        m = mappings.value.copy()
        fm = m.get(feature_type, {}).copy()
        fm[internal_name] = value if value != "(unmapped)" else None
        m[feature_type] = fm
        mappings.set(m)

    def update_unit(field_name, unit_val):
        u = field_units.value.copy()
        u[f"{feature_type}.{field_name}"] = unit_val
        field_units.set(u)

    solara.Text(f"{feature_type.title()}", style={"fontWeight": "bold", "fontSize": "16px"})

    for internal, auto_val in auto_mapping.items():
        options = ["(unmapped)"] + source_fields
        current = ftype_mapping.get(internal, auto_val) or "(unmapped)"
        is_req = internal in required
        label = f"{'* ' if is_req else ''}{internal}"

        unit_options = FIELD_UNIT_OPTIONS.get(internal)
        if unit_options:
            # Field + unit selector side by side
            unit_key = f"{feature_type}.{internal}"
            current_unit = field_units.value.get(unit_key, unit_options[0])
            with solara.Row(style={"alignItems": "flex-end", "gap": "6px"}):
                with solara.Column(style={"flex": "3"}):
                    solara.Select(
                        label=label,
                        value=current,
                        values=options,
                        on_value=lambda v, iname=internal: update_field(iname, v),
                    )
                with solara.Column(style={"flex": "1", "minWidth": "65px"}):
                    solara.Select(
                        label="Unit",
                        value=current_unit,
                        values=unit_options,
                        on_value=lambda v, iname=internal: update_unit(iname, v),
                    )
        else:
            solara.Select(
                label=label,
                value=current,
                values=options,
                on_value=lambda v, iname=internal: update_field(iname, v),
            )


@solara.component
def FeatureInspector():
    """Right panel showing details of the inspected feature."""
    fid = inspected_feature.value
    if fid is None:
        solara.Text("Click a feature on the map to inspect it.",
                     style={"color": "var(--text-muted)", "fontSize": "13px"})
        return

    current_gdfs = gdfs.value
    feature_type = None
    feature_row = None

    # Look up in pipes
    if "pipes" in current_gdfs:
        pid_col = current_gdfs["pipes"].columns[0]
        match = current_gdfs["pipes"][current_gdfs["pipes"][pid_col].astype(str) == str(fid)]
        if len(match) > 0:
            feature_type = "pipes"
            feature_row = match.iloc[0]

    # Look up in junctions
    if feature_row is None and "junctions" in current_gdfs:
        jid_col = current_gdfs["junctions"].columns[0]
        match = current_gdfs["junctions"][current_gdfs["junctions"][jid_col].astype(str) == str(fid)]
        if len(match) > 0:
            feature_type = "junctions"
            feature_row = match.iloc[0]

    type_label = "Pipe" if feature_type == "pipes" else "Junction" if feature_type == "junctions" else "Feature"
    in_sel = str(fid) in map_selection.value

    # Header
    sel_text = " [SELECTED]" if in_sel else ""
    solara.Text(f"{type_label}: {fid}{sel_text}",
                style={"fontWeight": "bold", "fontSize": "16px"})

    # Action buttons
    with solara.Row():
        def toggle_selection():
            sel = map_selection.value.copy()
            if str(fid) in sel:
                sel.discard(str(fid))
            else:
                sel.add(str(fid))
            map_selection.set(sel)

        sel_label = "Remove from Selection" if in_sel else "Add to Selection"
        solara.Button(sel_label, on_click=toggle_selection, outlined=True)

    # Attributes
    if feature_row is not None:
        solara.Text("Attributes", style={"fontWeight": "bold", "marginTop": "12px"})
        attrs = {}
        for col in feature_row.index:
            if col != "geometry" and pd.notna(feature_row[col]):
                attrs[col] = str(feature_row[col])

        # Show as a simple table
        if attrs:
            rows_html = ""
            for k, v in attrs.items():
                rows_html += (
                    f'<div class="attr-row">'
                    f'<span class="attr-key">{k}</span>'
                    f'<span class="attr-val">{v}</span></div>'
                )
            solara.HTML(unsafe_innerHTML=rows_html)

    # Related issues
    if analysis.value:
        feature_issues = [
            i for i in analysis.value["issues"]
            if str(i.feature_id) == str(fid)
        ]
        if feature_issues:
            solara.Text("Issues", style={"fontWeight": "bold", "marginTop": "12px"})
            for issue in feature_issues:
                color = ISSUE_COLORS.get(issue.issue_type, "#999")
                display_name = ISSUE_DISPLAY_NAMES.get(issue.issue_type, issue.issue_type)
                solara.HTML(unsafe_innerHTML=(
                    f'<div style="margin:6px 0;padding:8px 10px;background:#f7f8fc;'
                    f'border-radius:10px;border:1px solid rgba(0,0,0,0.06);">'
                    f'<span class="issue-dot" style="color:{color};">&#9679;</span>'
                    f'<b style="color:var(--text-primary);">{display_name}</b> '
                    f'<span style="font-size:11px;color:var(--text-muted);">({issue.severity})</span>'
                    f'<div style="font-size:12px;color:var(--text-secondary);margin-left:24px;margin-top:2px;">'
                    f'{issue.message}</div></div>'
                ))

            # Fix Tools section
            solara.Text("Fix Tools", style={"fontWeight": "bold", "marginTop": "12px"})

            ledger = edit_ledger.value
            pending = pending_connectivity_fix.value

            for issue in feature_issues:
                strategies = get_strategies(issue.issue_type)
                if not strategies:
                    solara.HTML(unsafe_innerHTML=(
                        f'<div style="font-size:12px;color:var(--text-muted);margin:4px 0;">'
                        f'No automated fixes for {ISSUE_DISPLAY_NAMES.get(issue.issue_type, issue.issue_type)}</div>'
                    ))
                    continue

                fix_display_name = ISSUE_DISPLAY_NAMES.get(issue.issue_type, issue.issue_type)
                solara.Text(f"{fix_display_name}",
                            style={"fontSize": "13px", "fontWeight": "500", "marginTop": "8px"})

                for strat_key, strat_name, _fn in strategies:
                    def make_apply(issue_obj, s_key):
                        def apply():
                            import fix_toolkit_sol as ftk
                            ftk.MIN_SLOPE = min_slope_setting.value
                            G = network.value["graph"]
                            entries = compute_fix(s_key, issue_obj, G, ledger)
                            if not entries:
                                return

                            # For strategies that change inverts at junctions,
                            # check if connecting pipes need adjustment
                            if s_key in CONNECTIVITY_STRATEGIES:
                                conn_entries, conn_descs = compute_connectivity_entries(
                                    entries, issue_obj, G, ledger)
                                if conn_entries:
                                    # Store pending fix and show dialog
                                    pending_connectivity_fix.set({
                                        "base_entries": entries,
                                        "conn_entries": conn_entries,
                                        "conn_descs": conn_descs,
                                    })
                                    return

                            # No connectivity impact — apply directly
                            new_ledger = ledger.copy()
                            apply_group(new_ledger, entries)
                            edit_ledger.set(new_ledger)
                        return apply

                    solara.Button(
                        strat_name,
                        on_click=make_apply(issue, strat_key),
                        outlined=True,
                        style={"fontSize": "11px", "marginBottom": "4px", "width": "100%",
                               "justifyContent": "flex-start", "textTransform": "none"},
                    )

            # ── Connectivity confirmation dialog ──
            if pending:
                solara.HTML(tag="hr")
                solara.HTML(unsafe_innerHTML=(
                    '<div style="background:var(--bg-card);border:1px solid var(--accent);'
                    'border-radius:8px;padding:10px;margin:8px 0;">'
                    '<b style="color:var(--accent);">Adjust connecting pipes?</b>'
                    '<p style="font-size:12px;color:var(--text-secondary);margin:6px 0 4px;">'
                    'The fix will change inverts at a shared junction. '
                    'These connecting pipes can be updated to maintain connectivity:</p>'
                    + ''.join(
                        f'<div style="font-size:11px;color:var(--text-primary);padding:2px 0;">'
                        f'&bull; {d}</div>'
                        for d in pending["conn_descs"]
                    )
                    + '</div>'
                ))

                def apply_with_connectivity():
                    all_entries = pending["base_entries"] + pending["conn_entries"]
                    new_ledger = edit_ledger.value.copy()
                    apply_group(new_ledger, all_entries)
                    edit_ledger.set(new_ledger)
                    pending_connectivity_fix.set(None)

                def apply_without_connectivity():
                    new_ledger = edit_ledger.value.copy()
                    apply_group(new_ledger, pending["base_entries"])
                    edit_ledger.set(new_ledger)
                    pending_connectivity_fix.set(None)

                def cancel_fix():
                    pending_connectivity_fix.set(None)

                with solara.Row(style={"gap": "4px", "flexWrap": "wrap"}):
                    solara.Button("Yes, adjust pipes", on_click=apply_with_connectivity,
                                  color="primary", style={"fontSize": "11px", "textTransform": "none"})
                    solara.Button("No, fix only", on_click=apply_without_connectivity,
                                  outlined=True, style={"fontSize": "11px", "textTransform": "none"})
                    solara.Button("Cancel", on_click=cancel_fix,
                                  text=True, color="error", style={"fontSize": "11px", "textTransform": "none"})

            if ledger:
                solara.HTML(tag="hr")
                def do_undo():
                    new_ledger = ledger.copy()
                    undo_last_group(new_ledger)
                    edit_ledger.set(new_ledger)
                solara.Button("Undo Last Fix", on_click=do_undo, color="error", text=True,
                              style={"fontSize": "12px"})
                solara.Text(f"{len(ledger)} edit(s) applied",
                            style={"fontSize": "11px", "color": "var(--text-muted)"})


@solara.component
def IssuesSummaryPanel(issues):
    """Issues summary grouped by type."""
    if not issues:
        solara.Success("No issues found.")
        return

    from collections import Counter
    type_counts = Counter(i.issue_type for i in issues)
    sev_counts = Counter(i.severity for i in issues)

    solara.Text("Issues Summary", style={"fontWeight": "bold", "fontSize": "16px"})

    # By severity
    for sev in ["HIGH", "MEDIUM", "LOW"]:
        count = sev_counts.get(sev, 0)
        if count > 0:
            solara.HTML(unsafe_innerHTML=(
                f'<span class="sev-badge sev-{sev}">{sev}</span> '
                f'<span style="font-size:14px;">{count} issue(s)</span>'
            ))

    solara.HTML(tag="hr")

    # By type
    for itype, count in sorted(type_counts.items()):
        color = ISSUE_COLORS.get(itype, "#999")
        display = ISSUE_DISPLAY_NAMES.get(itype, itype.replace("_", " ").title())
        solara.HTML(unsafe_innerHTML=(
            f'<div style="display:flex;align-items:center;padding:6px 10px;margin:3px 0;'
            f'background:#f7f8fc;border-radius:10px;border:1px solid rgba(0,0,0,0.06);">'
            f'<span style="color:{color};font-size:16px;margin-right:10px;">&#9679;</span>'
            f'<span style="font-size:13px;color:var(--text-primary);">{display}: <b>{count}</b></span>'
            f'</div>'
        ))


@solara.component
def IssuesTable(issues):
    """Detailed issues table with zoom-to-issue."""
    page, set_page = solara.use_state(0)

    if not issues:
        solara.Info("No issues match current filters.")
        return

    per_page = 25
    total_pages = max(1, (len(issues) + per_page - 1) // per_page)

    # Clamp page
    if page >= total_pages:
        set_page(0)
        return

    page_issues = issues[page * per_page : (page + 1) * per_page]

    solara.Text(f"{len(issues)} issues total",
                style={"fontWeight": "bold", "fontSize": "14px", "marginBottom": "8px"})

    # Header
    solara.HTML(unsafe_innerHTML=(
        '<div style="display:grid;grid-template-columns:70px 140px 1fr 80px;gap:8px;padding:6px 8px;'
        'font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;'
        'color:var(--text-muted);border-bottom:2px solid var(--border-subtle);">'
        '<div>Severity</div><div>Type</div><div>Feature / Message</div><div></div>'
        '</div>'
    ))

    for iss in page_issues:
        color = ISSUE_COLORS.get(iss.issue_type, "#999")
        display = ISSUE_DISPLAY_NAMES.get(iss.issue_type, iss.issue_type)
        fid = str(iss.feature_id)
        sev_class = f"sev-{iss.severity}"

        with solara.Row(style={"display": "grid", "gridTemplateColumns": "70px 140px 1fr 80px",
                               "gap": "8px", "padding": "6px 8px", "alignItems": "center",
                               "borderBottom": "1px solid var(--border-subtle)",
                               "fontSize": "13px"}):
            solara.HTML(unsafe_innerHTML=f'<span class="sev-badge {sev_class}">{iss.severity}</span>')
            solara.HTML(unsafe_innerHTML=f'<span style="color:{color};">&#9679;</span> <span style="font-size:12px;">{display}</span>')
            solara.HTML(unsafe_innerHTML=(
                f'<div><b>{fid}</b>'
                f'<div style="font-size:11px;color:var(--text-muted);margin-top:2px;">{iss.message}</div>'
                f'</div>'
            ))

            def make_zoom(feature_id):
                def zoom():
                    inspected_feature.set(feature_id)
                    _zoom_to_feature(feature_id)
                return zoom

            solara.Button("Zoom", on_click=make_zoom(fid), text=True,
                          style={"fontSize": "11px", "minWidth": "50px"})

    # Pagination
    if total_pages > 1:
        with solara.Row(style={"justifyContent": "center", "gap": "12px",
                               "marginTop": "12px", "alignItems": "center"}):
            solara.Button("← Prev", on_click=lambda: set_page(max(0, page - 1)),
                          disabled=page == 0, text=True, style={"fontSize": "12px"})
            solara.Text(f"Page {page + 1} of {total_pages}",
                        style={"fontSize": "13px", "color": "var(--text-muted)"})
            solara.Button("Next →", on_click=lambda: set_page(min(total_pages - 1, page + 1)),
                          disabled=page >= total_pages - 1, text=True, style={"fontSize": "12px"})


@solara.component
def DataTable(feature_type):
    """Attribute table for pipes or junctions, filtered by map selection."""
    current_gdfs = gdfs.value
    if feature_type not in current_gdfs:
        solara.Info(f"No {feature_type} data loaded.")
        return

    display = current_gdfs[feature_type].drop(columns=["geometry"], errors="ignore")
    total = len(display)

    sel = map_selection.value
    if sel:
        id_col = display.columns[0]
        display = display[display[id_col].astype(str).isin(sel)]
        if len(display) > 0:
            solara.Info(f"Filtered to {len(display)} of {total} {feature_type} (map selection)")
        else:
            solara.Text(f"No {feature_type} in current selection ({total} total)",
                        style={"color": "#888"})

    solara.DataFrame(display, items_per_page=25)


@solara.component
def TestDataLoader(handle_upload):
    """Dev button to load Langford test data from disk."""
    loading, set_loading = solara.use_state(False)
    status, set_status = solara.use_state("")

    def do_load():
        set_loading(True)
        set_status("")
        try:
            pipes_gdf = read_shapefile_from_path(str(_test_data_dir / "Conduit.zip"))
            juncs_gdf = read_shapefile_from_path(str(_test_data_dir / "Junction.zip"))
            handle_upload("pipes", pipes_gdf)
            handle_upload("junctions", juncs_gdf)
            set_status(f"Loaded {len(pipes_gdf)} pipes, {len(juncs_gdf)} junctions")
        except Exception as e:
            set_status(f"Error: {e}")
        set_loading(False)

    solara.Button(
        "Loading..." if loading else "Load Test Data (Langford)",
        on_click=do_load,
        disabled=loading,
        color="secondary",
        text=True,
        style={"fontSize": "11px", "marginTop": "4px"},
    )
    if status:
        solara.Text(status, style={"fontSize": "11px", "color": "var(--success)" if "Error" not in status else "var(--danger)"})


# ════════════════════════════════════════════════════════════
# MAIN PAGE
# ════════════════════════════════════════════════════════════

@solara.component
def Page():
    solara.HTML(unsafe_innerHTML=custom_css)

    show_layers, set_show_layers = solara.use_state(True)
    show_filters, set_show_filters = solara.use_state(False)
    show_selection, set_show_selection = solara.use_state(True)

    # ── SIDEBAR ──
    with solara.Sidebar():
        solara.HTML(unsafe_innerHTML=(
            '<div style="padding:4px 0 8px 0;">'
            '<div style="font-size:20px;font-weight:700;letter-spacing:-0.3px;'
            'background:linear-gradient(135deg,#638cff,#7ba4ff,#a0c4ff);'
            '-webkit-background-clip:text;-webkit-text-fill-color:transparent;'
            'background-clip:text;">Sewer Profile Analyzer</div>'
            '<div style="font-size:11px;color:var(--text-muted);letter-spacing:1px;'
            'text-transform:uppercase;font-weight:500;">Network QA/QC Tool</div>'
            '</div>'
        ))

        # Upload Section
        with solara.lab.Tabs():
            with solara.lab.Tab("Upload"):
                def handle_upload(ftype, gdf):
                    g = gdfs.value.copy()
                    g[ftype] = gdf
                    gdfs.set(g)

                FileUploader("Pipes (.zip) *", "pipes", handle_upload)
                FileUploader("Junctions (.zip) *", "junctions", handle_upload)
                FileUploader("Pumps (.zip)", "pumps", handle_upload)
                FileUploader("Storage (.zip)", "storage", handle_upload)

                # Dev: Load test data from disk
                if _test_data_dir.exists():
                    TestDataLoader(handle_upload)

                # Show loaded counts
                if gdfs.value:
                    loaded = ", ".join(f"{k}: {len(v)}" for k, v in gdfs.value.items())
                    solara.Text(f"Loaded: {loaded}", style={"fontSize": "12px", "color": "var(--text-sidebar-secondary)"})

            with solara.lab.Tab("Fields"):
                if gdfs.value:
                    for ftype, gdf in gdfs.value.items():
                        FieldMappingPanel(gdf, ftype)
                        solara.HTML(tag="hr")
                else:
                    solara.Info("Upload data first")

            with solara.lab.Tab("Settings"):
                solara.Text("Analysis", style={"fontWeight": "bold", "fontSize": "14px", "marginBottom": "4px"})
                solara.SliderFloat("Snap Tolerance (m)", value=snap_tolerance, min=0.1, max=20.0, step=0.1)
                solara.SliderFloat("Invert Mismatch Tolerance (m)", value=invert_tolerance, min=0.001, max=0.5, step=0.001)
                solara.SliderFloat("Min Structure Depth (m)", value=min_depth, min=0.3, max=2.0, step=0.1)
                solara.SliderFloat("Max Structure Depth (m)", value=max_depth, min=5.0, max=20.0, step=0.5)

                solara.HTML(tag="hr")
                solara.Text("Fix Toolkit", style={"fontWeight": "bold", "fontSize": "14px", "marginBottom": "4px"})

                def _set_min_slope(val):
                    try:
                        v = float(val)
                        if v > 0:
                            min_slope_setting.set(v)
                    except (ValueError, TypeError):
                        pass

                solara.InputText(
                    label="Min Slope (m/m)",
                    value=str(min_slope_setting.value),
                    on_value=_set_min_slope,
                )

        solara.HTML(tag="hr")

        # Run button
        can_run = "pipes" in gdfs.value and "junctions" in gdfs.value

        if not can_run:
            solara.Info("Upload Pipes & Junctions to begin.")

        def run_analysis():
            analysis_running.set(True)
            try:
                ing = {}
                for ftype, gdf in gdfs.value.items():
                    overrides = mappings.value.get(ftype, {})
                    result = ingest_gdf(gdf, ftype, overrides=overrides)
                    ing[ftype] = result
                ingestion.set(ing)

                net = build_network(
                    ing["pipes"]["records"],
                    ing["junctions"]["records"],
                    ing.get("pumps", {}).get("records"),
                    ing.get("storage", {}).get("records"),
                    snap_tolerance=snap_tolerance.value,
                )
                network.set(net)

                # Convert metric thresholds to data units so analysis
                # compares apples-to-apples with raw shapefile values
                inv_tol = _metric_to_data_unit(invert_tolerance.value, "invert_elev", "junctions")
                mn_dep  = _metric_to_data_unit(min_depth.value, "invert_elev", "junctions")
                mx_dep  = _metric_to_data_unit(max_depth.value, "invert_elev", "junctions")

                thresholds = {
                    "invert_mismatch_tolerance_m": inv_tol,
                    "invert_mismatch_tolerance_ft": inv_tol,
                    "min_structure_depth_m": mn_dep,
                    "min_structure_depth_ft": mn_dep,
                    "max_structure_depth_m": mx_dep,
                    "max_structure_depth_ft": mx_dep,
                    "adverse_slope_severity_threshold": -0.01,
                }
                result = run_full_analysis(net, thresholds)
                analysis.set(result)

                # Set filter to include all types
                all_types = list(set(i.issue_type for i in result["issues"]))
                filter_types.set(all_types)

                # Clear stale state
                map_selection.set(set())
                inspected_feature.set(None)
                edit_ledger.set([])
                preview_entries.set(None)

            finally:
                analysis_running.set(False)

        solara.Button(
            "Run Analysis",
            on_click=run_analysis,
            disabled=not can_run or analysis_running.value,
            color="primary",
            style={"width": "100%"},
        )

        if analysis_running.value:
            solara.ProgressLinear(True)

        # Layer visibility (after analysis)
        if analysis.value:
            solara.HTML(tag="hr")
            solara.Button(
                f"{'▾' if show_layers else '▸'} Layer Visibility",
                on_click=lambda: set_show_layers(not show_layers),
                text=True,
                classes=["sidebar-section-btn"],
            )
            if show_layers:
                solara.Checkbox(label="Pipes", value=vis_pipes)
                solara.Checkbox(label="Junctions", value=vis_junctions)
                solara.Checkbox(label="Flow Arrows", value=vis_arrows)
                solara.Checkbox(label="Pumps", value=vis_pumps)
                solara.Checkbox(label="Storage", value=vis_storage)

            # Issue type filters
            solara.HTML(tag="hr")
            solara.Button(
                f"{'▾' if show_filters else '▸'} Issue Filters",
                on_click=lambda: set_show_filters(not show_filters),
                text=True,
                classes=["sidebar-section-btn"],
            )
            if show_filters:
                all_issue_types = list(set(i.issue_type for i in analysis.value["issues"]))
                active = filter_types.value

                def toggle_issue_type(itype):
                    current = filter_types.value.copy() if isinstance(filter_types.value, list) else list(filter_types.value)
                    if itype in current:
                        current.remove(itype)
                    else:
                        current.append(itype)
                    filter_types.set(current)

                issue_counts = Counter(i.issue_type for i in analysis.value["issues"])
                for itype in sorted(all_issue_types):
                    color = ISSUE_COLORS.get(itype, "#999")
                    display = ISSUE_DISPLAY_NAMES.get(itype, itype)
                    is_active = itype in active
                    count = issue_counts.get(itype, 0)

                    def make_toggle(it):
                        def toggle():
                            toggle_issue_type(it)
                        return toggle

                    solara.Button(
                        f"{'✓ ' if is_active else '○ '}{display} ({count})",
                        on_click=make_toggle(itype),
                        style={
                            "fontSize": "11px",
                            "width": "100%",
                            "justifyContent": "flex-start",
                            "textTransform": "none",
                            "marginBottom": "2px",
                            "borderLeft": f"4px solid {color}",
                            "background": f"{color}22" if is_active else "transparent",
                            "color": "var(--text-sidebar)" if is_active else "var(--text-sidebar-muted)",
                        },
                        text=True,
                    )

                with solara.Row(style={"gap": "4px", "marginTop": "4px"}):
                    solara.Button("All", on_click=lambda: filter_types.set(all_issue_types),
                                  outlined=True, style={"fontSize": "11px", "textTransform": "none"})
                    solara.Button("None", on_click=lambda: filter_types.set([]),
                                  outlined=True, style={"fontSize": "11px", "textTransform": "none"})

            solara.HTML(tag="hr")
            solara.Button(
                f"{'▾' if show_selection else '▸'} Selection",
                on_click=lambda: set_show_selection(not show_selection),
                text=True,
                classes=["sidebar-section-btn"],
            )
            if show_selection:
                solara.Text("Use the rectangle tool on the map to select features, or click individual features.",
                            style={"fontSize": "11px", "color": "var(--text-sidebar-muted)"})

                sel = map_selection.value
                if sel:
                    solara.Text(f"{len(sel)} feature(s) selected",
                                style={"fontWeight": "bold", "fontSize": "13px"})

                    # Selected features list with individual remove buttons
                    with solara.Column(classes=["selection-list"]):
                        for fid in sorted(sel):
                            with solara.Row(classes=["selection-item"]):
                                solara.Text(fid, style={"fontSize": "12px", "flex": "1",
                                                         "overflow": "hidden", "textOverflow": "ellipsis",
                                                         "whiteSpace": "nowrap"})

                                def make_remove(f):
                                    def remove():
                                        s = map_selection.value.copy()
                                        s.discard(f)
                                        map_selection.set(s)
                                    return remove
                                solara.Button("✕", on_click=make_remove(fid),
                                              text=True,
                                              style={"minWidth": "24px", "padding": "0",
                                                     "fontSize": "14px", "color": "var(--text-sidebar-muted)"})

                    with solara.Row(style={"gap": "8px"}):
                        solara.Button("Zoom to Selection",
                                      on_click=lambda: _zoom_to_selection(),
                                      outlined=True, style={"flex": "1"})
                        solara.Button("Clear All",
                                      on_click=lambda: map_selection.set(set()),
                                      outlined=True, style={"flex": "1"})

    # ── MAIN CONTENT ──
    if analysis.value is None:
        # Landing page
        solara.Title("Sewer Profile Analyzer")
        solara.Markdown(
            "Upload your sewer network shapefiles using the sidebar, "
            "map the fields, and click **Run Analysis** to get started."
        )
        with solara.Columns([1, 1, 1]):
            with solara.Card():
                solara.HTML(unsafe_innerHTML=(
                    '<div class="landing-step">'
                    '<div class="landing-step-num">1</div>'
                    '<div style="font-size:16px;font-weight:600;margin-bottom:6px;color:var(--text-primary);">Upload</div>'
                    '<div style="font-size:13px;color:var(--text-primary);opacity:0.8;">Add your Pipes and Junctions shapefiles (+ optional Pumps/Storage).</div>'
                    '</div>'
                ))
            with solara.Card():
                solara.HTML(unsafe_innerHTML=(
                    '<div class="landing-step">'
                    '<div class="landing-step-num">2</div>'
                    '<div style="font-size:16px;font-weight:600;margin-bottom:6px;color:var(--text-primary);">Map Fields</div>'
                    '<div style="font-size:13px;color:var(--text-primary);opacity:0.8;">Verify auto-detected field mappings match your data schema.</div>'
                    '</div>'
                ))
            with solara.Card():
                solara.HTML(unsafe_innerHTML=(
                    '<div class="landing-step">'
                    '<div class="landing-step-num">3</div>'
                    '<div style="font-size:16px;font-weight:600;margin-bottom:6px;color:var(--text-primary);">Analyze</div>'
                    '<div style="font-size:13px;color:var(--text-primary);opacity:0.8;">Run the analysis to detect profile issues across your network.</div>'
                    '</div>'
                ))
    else:
        # Analysis results
        solara.Title("Sewer Profile Analyzer")
        issues = analysis.value["issues"]
        stats = network.value["stats"]

        # Apply filters
        active_types = filter_types.value
        filtered = [i for i in issues if i.issue_type in active_types]

        # Split fixed/unfixed
        ledger = edit_ledger.value
        edited_ids = set(e.feature_id for e in ledger) if ledger else set()
        unfixed = [i for i in filtered if str(i.feature_id) not in edited_ids]
        fixed = [i for i in filtered if str(i.feature_id) in edited_ids]

        # Map + Detail panel
        with solara.Columns([3, 1]):
            # Map column
            with solara.Column():
                build_leaflet_map(
                    pipes_gdf=gdfs.value.get("pipes"),
                    junctions_gdf=gdfs.value.get("junctions"),
                    pumps_gdf=gdfs.value.get("pumps"),
                    storage_gdf=gdfs.value.get("storage"),
                    issues=unfixed,
                    network_result=network.value,
                    selected_ids=map_selection.value,
                    on_feature_click=lambda fid: inspected_feature.set(fid),
                    on_feature_select=lambda fid: _toggle_map_select(fid),
                    on_box_select=lambda fids: _add_box_selection(fids),
                    multi_select=True,
                    visible_layers={
                        "Pipes": vis_pipes.value,
                        "Junctions": vis_junctions.value,
                        "Flow Arrows": vis_arrows.value,
                        "Pumps": vis_pumps.value,
                        "Storage": vis_storage.value,
                    },
                )

            # Detail panel
            with solara.Column():
                if inspected_feature.value:
                    FeatureInspector()
                else:
                    IssuesSummaryPanel(filtered)

        # Below-map tabs
        with solara.lab.Tabs():
            with solara.lab.Tab("Issue Details"):
                IssuesTable(filtered)
            with solara.lab.Tab("Profile View"):
                ProfilePanel()
            with solara.lab.Tab("Pipes"):
                DataTable("pipes")
            with solara.lab.Tab("Junctions"):
                DataTable("junctions")
            with solara.lab.Tab("Network Info"):
                _render_network_info()


def _toggle_map_select(fid):
    """Toggle a feature ID in the map selection set."""
    sel = map_selection.value.copy()
    fid_str = str(fid)
    if fid_str in sel:
        sel.discard(fid_str)
    else:
        sel.add(fid_str)
    map_selection.set(sel)


def _add_box_selection(fids):
    """Add a set of feature IDs from a box-select to the selection."""
    if not fids:
        return
    sel = map_selection.value.copy()
    sel.update(str(f) for f in fids)
    map_selection.set(sel)


def _zoom_to_feature(fid):
    """Zoom the map to a single feature by ID."""
    current_gdfs_val = gdfs.value
    for ftype in ["pipes", "junctions", "pumps", "storage"]:
        if ftype not in current_gdfs_val:
            continue
        gdf = current_gdfs_val[ftype]
        id_col = gdf.columns[0]
        match = gdf[gdf[id_col].astype(str) == str(fid)]
        if len(match) > 0:
            match_wgs = _ensure_wgs84(match[["geometry"]].copy().set_crs(gdf.crs, allow_override=True))
            center, zoom = _get_center_zoom(match_wgs)
            # Zoom in closer for a single feature
            zoom = min(zoom + 2, 20)
            widget = _last_map_widget[0]
            if widget is not None:
                widget.center = center
                widget.zoom = zoom
            return


def _zoom_to_selection():
    """Zoom the map to fit all selected features."""
    sel = map_selection.value
    if not sel:
        return

    current_gdfs_val = gdfs.value
    geom_frames = []

    for ftype in ["pipes", "junctions", "pumps", "storage"]:
        if ftype not in current_gdfs_val:
            continue
        gdf = current_gdfs_val[ftype]
        id_col = gdf.columns[0]
        mask = gdf[id_col].astype(str).isin(sel)
        matched = gdf[mask]
        if len(matched) > 0:
            geom_frames.append(matched[["geometry"]].copy())

    if not geom_frames:
        return

    combined = gpd.GeoDataFrame(pd.concat(geom_frames, ignore_index=True))
    if current_gdfs_val.get("pipes") is not None:
        combined = combined.set_crs(current_gdfs_val["pipes"].crs, allow_override=True)
    combined_wgs = _ensure_wgs84(combined)
    center, zoom = _get_center_zoom(combined_wgs)

    widget = _last_map_widget[0]
    if widget is not None:
        widget.center = center
        widget.zoom = zoom


@solara.component
def ProfilePanel():
    """Profile view component that reactively updates when ledger or selection changes."""
    # Read reactive dependencies — this subscribes the component to changes
    current_ledger = edit_ledger.value
    sel = map_selection.value
    inspected = inspected_feature.value

    if not sel and not inspected:
        solara.Info("Select features on the map using the rectangle tool or by clicking to view their profile.")
        return

    target = list(sel) if sel else [inspected]
    fig = _build_profile(set(target))

    if fig:
        # FigurePlotly uses use_effect internally — it only re-runs when
        # dependencies change. We pass an explicit dependency list so
        # ledger edits, selection changes, and inspection changes all
        # trigger a figure refresh.
        ledger_ver = len(current_ledger) if current_ledger else 0
        sel_key = ",".join(sorted(sel)) if sel else ""
        insp_key = str(inspected) if inspected else ""
        solara.FigurePlotly(fig, dependencies=[ledger_ver, sel_key, insp_key])
    else:
        solara.Info("No pipe data found for selected features. Select pipes or junctions connected to pipes.")


def _build_profile(selected_ids):
    """Build InfoSWMM-style profile view with ground surface, pipe diameters, and labels."""
    if not network.value:
        return None

    G = network.value["graph"]
    issues = analysis.value["issues"] if analysis.value else []
    ledger = edit_ledger.value

    # Collect selected pipe edges
    # A pipe is included if its ID OR either connected node is in the selection
    pipe_edges = []
    for u, v, data in G.edges(data=True):
        pid = str(data.get("pipe_id", ""))
        if pid in selected_ids or str(u) in selected_ids or str(v) in selected_ids:
            pipe_edges.append((u, v, data))

    if not pipe_edges:
        return None

    # Build ordered chain
    adj_out = {}
    for u, v, data in pipe_edges:
        adj_out.setdefault(u, []).append((v, data))

    all_dst = {v for _, v, _ in pipe_edges}
    all_src = {u for u, _, _ in pipe_edges}
    start_candidates = all_src - all_dst
    if not start_candidates:
        start_candidates = all_src
    start = min(start_candidates, key=str)

    visited = set()
    current = start
    ordered_edges = []
    while current in adj_out and current not in visited:
        visited.add(current)
        next_node, edata = adj_out[current][0]
        ordered_edges.append((current, next_node, edata))
        current = next_node

    if not ordered_edges:
        ordered_edges = [(u, v, d) for u, v, d in pipe_edges[:20]]

    # Build profile data
    cumulative = 0.0
    profile_nodes = []
    profile_pipes = []

    for i, (u, v, edata) in enumerate(ordered_edges):
        u_data = G.nodes[u] if hasattr(G, 'nodes') else G._nodes.get(u, {})
        v_data = G.nodes[v] if hasattr(G, 'nodes') else G._nodes.get(v, {})

        if i == 0:
            profile_nodes.append((cumulative, str(u), u_data))

        pipe_len_raw = edata.get("length", 100) or 100
        pipe_len = _convert_to_metric(float(pipe_len_raw), "length", "pipes")
        end_station = cumulative + pipe_len
        profile_pipes.append((cumulative, end_station, edata))
        profile_nodes.append((end_station, str(v), v_data))
        cumulative = end_station

    issue_map = {}
    for iss in issues:
        fid = str(iss.feature_id)
        issue_map.setdefault(fid, []).append(iss)

    BG = "#ffffff"
    GRID_COLOR = "#e8ecf0"
    TEXT_COLOR = "#4a5568"

    total_range = cumulative if cumulative > 0 else 1.0
    mh_half_w = 0.5  # 1m total manhole width (0.5m each side)

    fig = go.Figure()

    # Unit conversion helper — converts profile values to metric
    def cv_pipe(field, val):
        return _convert_to_metric(val, field, "pipes")
    def cv_junc(field, val):
        return _convert_to_metric(val, field, "junctions")

    # ── Ground Surface Line ──
    # Connect rim elevations across all manholes
    ground_x, ground_y = [], []
    for sta, nid, ndata in profile_nodes:
        rim = ndata.get("rim_elev")
        if rim is not None:
            ground_x.append(sta)
            ground_y.append(cv_junc("rim_elev", float(rim)))

    if len(ground_x) >= 2:
        # Fill from ground to a high value to show earth
        fig.add_trace(go.Scatter(
            x=ground_x, y=ground_y, mode="lines",
            line=dict(color="#8B7355", width=2.5),
            name="Ground Surface", showlegend=True,
            fill="tozeroy", fillcolor="rgba(139,115,85,0.08)",
        ))

    # Build lookup of manhole inverts by station for clamping pipe inverts
    node_inv_by_sta = {}
    for sta, nid, ndata in profile_nodes:
        inv = ndata.get("invert_elev")
        if inv is not None:
            node_inv_by_sta[sta] = cv_junc("invert_elev", float(inv))

    # ── Pipe Barrels ──
    # Draw as filled rectangles showing actual diameter
    for start_sta, end_sta, edata in profile_pipes:
        us_inv_orig = edata.get("us_invert")
        ds_inv_orig = edata.get("ds_invert")
        diameter_val = edata.get("diameter")
        pid = str(edata.get("pipe_id", ""))

        if us_inv_orig is None or ds_inv_orig is None:
            continue

        us_inv_raw = get_current_value(ledger, pid, "us_invert", float(us_inv_orig))
        ds_inv_raw = get_current_value(ledger, pid, "ds_invert", float(ds_inv_orig))
        us_inv = cv_pipe("us_invert", us_inv_raw)
        ds_inv = cv_pipe("ds_invert", ds_inv_raw)

        # Clamp pipe inverts so they never go below manhole invert
        us_mh_inv = node_inv_by_sta.get(start_sta)
        ds_mh_inv = node_inv_by_sta.get(end_sta)
        if us_mh_inv is not None:
            us_inv = max(us_inv, us_mh_inv)
        if ds_mh_inv is not None:
            ds_inv = max(ds_inv, ds_mh_inv)

        # Diameter: convert to mm using unit config, then to meters for drawing
        if diameter_val and float(diameter_val) > 0:
            dia_mm = cv_pipe("diameter", float(diameter_val))  # always returns mm
            dia_m = dia_mm / 1000.0
        else:
            dia_mm = 0.0
            dia_m = 0.3  # default 300mm

        pipe_issues = issue_map.get(pid, [])
        has_adverse = any(i.issue_type == "ADVERSE_SLOPE" for i in pipe_issues)
        color = "#FF4444" if has_adverse else "#4A90D9"

        # Offset pipe barrel to stop at manhole boundary
        # The pipe invert values (us_inv, ds_inv) are the elevations AT the manholes,
        # so we use them directly — no interpolation needed.
        pipe_start = start_sta + mh_half_w
        pipe_end = end_sta - mh_half_w

        # Skip offset if pipe would be zero/negative length
        if pipe_end <= pipe_start:
            pipe_start = start_sta
            pipe_end = end_sta

        us_crown = us_inv + dia_m
        ds_crown = ds_inv + dia_m

        # Outer barrel rectangle — inverts connect directly to manhole wall
        barrel_x = [pipe_start, pipe_end, pipe_end, pipe_start, pipe_start]
        barrel_y = [us_inv, ds_inv, ds_crown, us_crown, us_inv]

        r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
        fill_rgba = f"rgba({r},{g},{b},0.25)"

        fig.add_trace(go.Scatter(
            x=barrel_x, y=barrel_y, mode="lines",
            fill="toself", fillcolor=fill_rgba,
            line=dict(color=color, width=2),
            name=f"Pipe {pid}", showlegend=False,
            hoverinfo="text",
            hovertext=f"Pipe: {pid}<br>US Inv: {us_inv:.2f}m<br>DS Inv: {ds_inv:.2f}m" + (f"<br>Dia: {dia_mm:.0f}mm" if dia_mm else ""),
        ))

        # Pipe ID label at center
        mid_x = (start_sta + end_sta) / 2
        mid_y = (us_inv + ds_inv) / 2 + dia_m * 0.5
        slope = (ds_inv - us_inv) / max(end_sta - start_sta, 1) * 100  # %
        label_text = f"{pid}<br>{dia_mm:.0f}mm | {slope:.1f}%" if dia_mm else f"{pid}<br>{slope:.1f}%"

        fig.add_trace(go.Scatter(
            x=[mid_x], y=[mid_y + dia_m * 0.3],
            mode="text",
            text=[label_text],
            textfont=dict(size=9, color="#4a5568"),
            showlegend=False, hoverinfo="skip",
        ))

    # ── Manholes ──
    # Draw as vertical rectangles from invert to rim
    for sta, nid, ndata in profile_nodes:
        rim_raw = ndata.get("rim_elev")
        inv_raw = ndata.get("invert_elev")
        if rim_raw is None and inv_raw is None:
            # No elevation data at all — show a label only
            fig.add_trace(go.Scatter(
                x=[sta], y=[0], mode="text",
                text=[f"{nid}\n(no data)"],
                textfont=dict(size=8, color="#999"),
                showlegend=False, hoverinfo="skip",
            ))
            continue
        # If only one is missing, estimate from pipe inverts at this station
        if rim_raw is None:
            inv = cv_junc("invert_elev", float(inv_raw))
            rim = inv + 2.0  # assume 2m default depth
        elif inv_raw is None:
            rim = cv_junc("rim_elev", float(rim_raw))
            inv = rim - 2.0  # assume 2m default depth
        else:
            rim = cv_junc("rim_elev", float(rim_raw))
            inv = cv_junc("invert_elev", float(inv_raw))

        # Manhole walls
        mh_xs = [sta - mh_half_w, sta + mh_half_w, sta + mh_half_w,
                 sta - mh_half_w, sta - mh_half_w]
        mh_ys = [inv, inv, rim, rim, inv]

        depth = rim - inv
        node_issues = issue_map.get(nid, [])
        has_issue = len(node_issues) > 0
        mh_color = "#FF8C00" if has_issue else "#5588aa"
        mh_fill = "rgba(255,140,0,0.1)" if has_issue else "rgba(85,136,170,0.12)"

        fig.add_trace(go.Scatter(
            x=mh_xs, y=mh_ys, mode="lines",
            fill="toself", fillcolor=mh_fill,
            line=dict(color=mh_color, width=1.5),
            name=f"MH {nid}", showlegend=False,
            hoverinfo="text",
            hovertext=f"Node: {nid}<br>Rim: {rim:.2f}m<br>Inv: {inv:.2f}m<br>Depth: {depth:.2f}m",
        ))

        # Node ID label above rim
        fig.add_trace(go.Scatter(
            x=[sta], y=[rim + (rim - inv) * 0.05 + 0.3],
            mode="text",
            text=[nid],
            textfont=dict(size=8, color="#4a5568"),
            showlegend=False, hoverinfo="skip",
        ))

    # ── Issue markers on profile ──
    for start_sta, end_sta, edata in profile_pipes:
        pid = str(edata.get("pipe_id", ""))
        pipe_issues = issue_map.get(pid, [])
        if not pipe_issues:
            continue
        us_inv = edata.get("us_invert")
        ds_inv = edata.get("ds_invert")
        if us_inv is None or ds_inv is None:
            continue
        mid_x = (start_sta + end_sta) / 2
        mid_inv = (float(us_inv) + float(ds_inv)) / 2
        for iss in pipe_issues:
            iss_color = ISSUE_COLORS.get(iss.issue_type, "#FF4444")
            display = ISSUE_DISPLAY_NAMES.get(iss.issue_type, iss.issue_type)
            fig.add_trace(go.Scatter(
                x=[mid_x], y=[mid_inv - 0.5],
                mode="markers",
                marker=dict(size=10, color=iss_color, symbol="triangle-up",
                            line=dict(width=1, color="#fff")),
                showlegend=False, hoverinfo="text",
                hovertext=f"{display} ({iss.severity})<br>{iss.message}",
            ))

    fig.update_layout(
        plot_bgcolor=BG, paper_bgcolor=BG,
        font=dict(color=TEXT_COLOR, size=11),
        xaxis=dict(title="Station (m)", gridcolor=GRID_COLOR,
                   showgrid=True, zeroline=False),
        yaxis=dict(title="Elevation (m)", gridcolor=GRID_COLOR,
                   showgrid=True, zeroline=False),
        margin=dict(l=60, r=20, t=30, b=50),
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, font=dict(size=10)),
        hovermode="closest",
    )
    return fig


def _render_network_info():
    """Render network statistics and issue summary."""
    if not network.value:
        solara.Info("Run analysis first.")
        return

    stats = network.value["stats"]
    issues = analysis.value["issues"] if analysis.value else []
    total_issues = len(issues)
    issue_types = len(set(i.issue_type for i in issues)) if issues else 0

    with solara.Columns([1, 1, 1]):
        with solara.Column():
            solara.Text("Network", style={"fontWeight": "bold"})
            solara.Markdown(f"""
- Pipes: **{stats['total_edges']}**
- Nodes: **{stats['total_nodes']}**
- Components: **{stats['connected_components']}**
- Largest component: **{stats['largest_component_size']}** nodes
- Virtual nodes: **{stats['virtual_nodes_created']}**
""")
        with solara.Column():
            solara.Text("Issues", style={"fontWeight": "bold"})
            solara.Markdown(f"""
- Issues found: **{total_issues}**
- Issue types: **{issue_types}**
""")
            if issues:
                from collections import Counter
                sev_counts = Counter(i.severity for i in issues)
                for sev in ["HIGH", "MEDIUM", "LOW"]:
                    c = sev_counts.get(sev, 0)
                    if c > 0:
                        solara.Markdown(f"- {sev}: **{c}**")

        with solara.Column():
            solara.Text("Connectivity", style={"fontWeight": "bold"})
            if stats["source_nodes"]:
                solara.Markdown(f"- Source nodes: **{len(stats['source_nodes'])}**")
            if stats["dead_end_nodes"]:
                solara.Markdown(f"- Dead ends: **{len(stats['dead_end_nodes'])}**")
            if stats["orphan_nodes"]:
                solara.Markdown(f"- Orphan nodes: **{len(stats['orphan_nodes'])}**")
