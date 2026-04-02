"""
Sewer Profile Analyzer — Streamlit Web App

GIS-style interface: map-centered workspace with sidebar controls
and contextual issue details.

Run with: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import geopandas as gpd
import time
import tempfile
import os
import sys
import io
import zipfile
import json
import plotly.graph_objects as go
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from webapp.ingest_gpd import (
    auto_detect_fields, get_required_fields, load_field_config,
    ingest_gdf, read_shapefile_from_upload
)
from src.network_builder import build_network
from src.profile_analyzer import run_full_analysis, trace_profile
from webapp.map_builder import (
    build_pydeck_map, ISSUE_COLORS, ISSUE_DISPLAY_NAMES, get_feature_bounds,
    render_issues_summary_html,
)
from webapp.fix_toolkit import (
    LedgerEntry, apply_group, undo_last_group, get_current_value,
    get_all_edits, ledger_summary, get_strategies, compute_fix,
    junction_invert_from_lowest_pipe,
)

# ── Page Config ──
st.set_page_config(
    page_title="Sewer Profile Analyzer",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS for GIS-style layout ──
st.markdown("""
<style>
    /* Reduce default padding for map-first layout */
    .block-container { padding-top: 2.5rem; padding-bottom: 0; }

    /* Hide the default Streamlit header bar to reclaim space */
    header[data-testid="stHeader"] {
        height: 2rem;
    }

    /* Metric cards row */
    .metric-bar {
        display: flex; gap: 12px; margin-bottom: 12px; margin-top: 0.25rem;
    }
    .metric-card {
        flex: 1; background: #1a1a2e; color: #fff; border-radius: 8px;
        padding: 12px 16px; text-align: center;
    }
    .metric-card .value { font-size: 28px; font-weight: 700; }
    .metric-card .label { font-size: 11px; text-transform: uppercase;
        letter-spacing: 1px; color: #aaa; margin-top: 2px; }
    .metric-card.high { border-bottom: 3px solid #FF4444; }
    .metric-card.medium { border-bottom: 3px solid #FF8C00; }
    .metric-card.low { border-bottom: 3px solid #1E90FF; }
    .metric-card.info { border-bottom: 3px solid #4A90D9; }

    /* Issue summary table styling */
    .issue-row {
        display: flex; align-items: center; padding: 8px 12px;
        border-bottom: 1px solid #eee; font-size: 13px;
    }
    .issue-row:hover { background: #f5f7ff; }
    .sev-badge {
        display: inline-block; padding: 2px 8px; border-radius: 4px;
        font-size: 11px; font-weight: 600; margin-right: 10px; min-width: 55px;
        text-align: center;
    }
    .sev-HIGH { background: #FFE0E0; color: #CC0000; }
    .sev-MEDIUM { background: #FFF3E0; color: #CC6600; }
    .sev-LOW { background: #E0F0FF; color: #0066CC; }

    /* Hide streamlit footer and menu for cleaner look */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: #f8f9fb;
    }

</style>
""", unsafe_allow_html=True)


# ── Selection & Inspection State ──
if "map_selection" not in st.session_state:
    st.session_state["map_selection"] = set()
if "inspected_feature" not in st.session_state:
    st.session_state["inspected_feature"] = None
if "edit_ledger" not in st.session_state:
    st.session_state["edit_ledger"] = []
if "preview_entries" not in st.session_state:
    st.session_state["preview_entries"] = None
if "_last_sel_name" not in st.session_state:
    st.session_state["_last_sel_name"] = None
if "_show_profile" not in st.session_state:
    st.session_state["_show_profile"] = False

def _process_map_click():
    """Process pending pydeck click from session state."""
    _pending = st.session_state.get("main_map", None)
    if _pending and hasattr(_pending, "selection") and _pending.selection:
        _objs = _pending.selection.get("objects", {})
        _sel_list = []
        for _lo in _objs.values():
            _sel_list.extend(_lo)
        if _sel_list:
            _cname = _sel_list[0].get("name", "")
            if ": " in _cname:
                _cname = _cname.split(": ", 1)[1]
            if _cname and _cname != st.session_state.get("_last_sel_name"):
                st.session_state["_last_sel_name"] = _cname
                st.session_state["inspected_feature"] = _cname
                if st.session_state.get("multi_select_mode", False):
                    _sel = st.session_state.get("map_selection", set())
                    if _cname in _sel:
                        _sel.discard(_cname)
                    else:
                        _sel.add(_cname)
                    st.session_state["map_selection"] = _sel

# Process on full reruns (sidebar count etc.)
_process_map_click()


# ════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════

def upload_shapefile(label, key):
    files = st.file_uploader(
        label,
        type=["shp", "shx", "dbf", "prj", "cpg", "zip"],
        accept_multiple_files=True,
        key=key,
        help="Upload all shapefile components (.shp, .shx, .dbf, .prj) or a single .zip"
    )
    return files


def field_mapping_ui(gdf, feature_type, key_prefix):
    config = load_field_config()
    source_fields = [c for c in gdf.columns if c != "geometry"]
    auto_mapping = auto_detect_fields(source_fields, feature_type, config)
    required = get_required_fields(feature_type)

    final_mapping = {}
    cols = st.columns(2)
    for i, (internal, auto_val) in enumerate(auto_mapping.items()):
        col = cols[i % 2]
        options = ["(unmapped)"] + source_fields
        default_idx = 0
        if auto_val and auto_val in source_fields:
            default_idx = source_fields.index(auto_val) + 1
        is_req = internal in required
        label = f"{'* ' if is_req else ''}{internal}"
        with col:
            selected = st.selectbox(
                label, options, index=default_idx,
                key=f"{key_prefix}_{internal}",
            )
            final_mapping[internal] = selected if selected != "(unmapped)" else None
    return final_mapping


def render_metric_bar(issues, stats):
    """Render the compact metric cards bar."""
    total_issues = len(issues)
    # Count distinct issue types
    issue_types = len(set(i.issue_type for i in issues)) if issues else 0

    html = '<div class="metric-bar">'
    html += f'<div class="metric-card info"><div class="value">{stats["total_edges"]}</div><div class="label">Pipes</div></div>'
    html += f'<div class="metric-card info"><div class="value">{stats["total_nodes"]}</div><div class="label">Nodes</div></div>'
    html += f'<div class="metric-card info"><div class="value">{stats["connected_components"]}</div><div class="label">Components</div></div>'
    html += f'<div class="metric-card high"><div class="value">{total_issues}</div><div class="label">Issues Found</div></div>'
    html += f'<div class="metric-card medium"><div class="value">{issue_types}</div><div class="label">Issue Types</div></div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def build_profile_plotly(selected_ids, network, gdfs, issues, ledger=None):
    """Build an interactive Plotly profile view for selected pipes/junctions.

    Shows pipe inverts as filled bands with diameter, manhole rectangles from
    invert to rim, pipe penetration markers at manhole walls, and highlights
    issues. Returns a plotly.graph_objects.Figure.
    """
    G = network["graph"]

    # Collect all selected pipe edges and their connected nodes
    pipe_edges = []
    for u, v, data in G.edges(data=True):
        pid = str(data.get("pipe_id", ""))
        if pid in selected_ids or str(u) in selected_ids or str(v) in selected_ids:
            pipe_edges.append((u, v, data))

    if not pipe_edges:
        return None

    # Build ordered chain: try to connect pipes end-to-end
    edge_map = {}  # (u, v) -> data
    adj_out = {}   # node -> [(next_node, edge_data)]
    for u, v, data in pipe_edges:
        edge_map[(u, v)] = data
        adj_out.setdefault(u, []).append((v, data))

    # Find start node (one with no incoming edge in our selection)
    all_dst = {v for _, v, _ in pipe_edges}
    all_src = {u for u, _, _ in pipe_edges}
    start_candidates = all_src - all_dst
    if not start_candidates:
        start_candidates = all_src
    start = min(start_candidates, key=str)

    # Walk the chain
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
    profile_nodes = []  # (station, node_id, node_data)
    profile_pipes = []  # (start_station, end_station, edge_data)

    for i, (u, v, edata) in enumerate(ordered_edges):
        u_data = G.nodes[u] if hasattr(G, 'nodes') else G._nodes.get(u, {})
        v_data = G.nodes[v] if hasattr(G, 'nodes') else G._nodes.get(v, {})

        if i == 0:
            profile_nodes.append((cumulative, str(u), u_data))

        pipe_len = edata.get("length", 100) or 100
        end_station = cumulative + float(pipe_len)
        profile_pipes.append((cumulative, end_station, edata))
        profile_nodes.append((end_station, str(v), v_data))
        cumulative = end_station

    # Build issue lookup by feature_id
    issue_map = {}
    for iss in issues:
        fid = str(iss.feature_id)
        issue_map.setdefault(fid, []).append(iss)

    if ledger is None:
        ledger = []

    BG = "#0e1117"
    GRID_COLOR = "#2a2a3a"
    TEXT_COLOR = "#cccccc"
    AXIS_COLOR = "#888888"

    # Manhole visual half-width: 2% of total station range, min 3 ft
    total_range = cumulative if cumulative > 0 else 1.0
    mh_half_w = max(total_range * 0.015, 3.0)

    fig = go.Figure()

    # ── Ground surface (rim elevations) ──
    rim_stations = []
    rim_elevations = []
    for sta, nid, ndata in profile_nodes:
        rim = ndata.get("rim_elev")
        if rim is not None:
            rim_stations.append(sta)
            rim_elevations.append(float(rim))

    if len(rim_stations) >= 2:
        fig.add_trace(go.Scatter(
            x=rim_stations,
            y=rim_elevations,
            mode="lines",
            name="Ground Surface",
            line=dict(color="#8B6914", width=2),
            fill="toself",
            fillcolor="rgba(139,105,20,0.06)",
            hoverinfo="skip",
            showlegend=True,
        ))

    # ── Pipes ──
    for start_sta, end_sta, edata in profile_pipes:
        us_inv_orig = edata.get("us_invert")
        ds_inv_orig = edata.get("ds_invert")
        diameter_in = edata.get("diameter")
        pid = str(edata.get("pipe_id", ""))

        if us_inv_orig is None or ds_inv_orig is None:
            continue

        us_inv_orig = float(us_inv_orig)
        ds_inv_orig = float(ds_inv_orig)

        us_inv = get_current_value(ledger, pid, "us_invert", us_inv_orig)
        ds_inv = get_current_value(ledger, pid, "ds_invert", ds_inv_orig)
        was_edited = (us_inv != us_inv_orig or ds_inv != ds_inv_orig)
        dia_ft = float(diameter_in) / 12.0 if diameter_in else 0.5

        pipe_issues = issue_map.get(pid, [])
        has_adverse = any(i.issue_type == "ADVERSE_SLOPE" for i in pipe_issues)
        has_dia_dec = any(i.issue_type == "DIAMETER_DECREASE" for i in pipe_issues)

        if was_edited:
            color = "#00CC66"
        elif has_adverse:
            color = "#FF4444"
        elif has_dia_dec:
            color = "#FF8C00"
        else:
            color = "#4A90D9"

        # Ghost of original when edited
        if was_edited:
            ghost_xs = [start_sta, end_sta, end_sta, start_sta, start_sta]
            ghost_ys = [
                us_inv_orig, ds_inv_orig,
                ds_inv_orig + dia_ft, us_inv_orig + dia_ft,
                us_inv_orig,
            ]
            fig.add_trace(go.Scatter(
                x=ghost_xs,
                y=ghost_ys,
                mode="lines",
                fill="toself",
                fillcolor="rgba(136,136,136,0.10)",
                line=dict(color="#888888", width=1, dash="dot"),
                name="Original (before fix)",
                showlegend=False,
                hoverinfo="skip",
            ))

        # Filled pipe band (invert to crown) — polygon closed
        band_xs = [start_sta, end_sta, end_sta, start_sta, start_sta]
        band_ys = [us_inv, ds_inv, ds_inv + dia_ft, us_inv + dia_ft, us_inv]

        r, g, b = (
            int(color[1:3], 16),
            int(color[3:5], 16),
            int(color[5:7], 16),
        )
        fill_rgba = f"rgba({r},{g},{b},0.20)"

        slope = edata.get("slope")
        if slope is not None:
            calc_slope = float(slope)
        elif (end_sta - start_sta) > 0:
            calc_slope = (us_inv - ds_inv) / (end_sta - start_sta)
        else:
            calc_slope = 0.0

        hover_txt = (
            f"<b>Pipe {pid}</b><br>"
            f"Diameter: {diameter_in}\"<br>" if diameter_in else f"<b>Pipe {pid}</b><br>"
        )
        hover_txt += (
            f"US invert: {us_inv:.3f} ft<br>"
            f"DS invert: {ds_inv:.3f} ft<br>"
            f"Slope: {calc_slope:.4f}<br>"
            f"Length: {end_sta - start_sta:.1f} ft"
        )

        fig.add_trace(go.Scatter(
            x=band_xs,
            y=band_ys,
            mode="lines",
            fill="toself",
            fillcolor=fill_rgba,
            line=dict(color=color, width=2),
            name=f"Pipe {pid}",
            hovertemplate=hover_txt + "<extra></extra>",
            showlegend=False,
        ))

        # Crown dashed line
        fig.add_trace(go.Scatter(
            x=[start_sta, end_sta],
            y=[us_inv + dia_ft, ds_inv + dia_ft],
            mode="lines",
            line=dict(color=color, width=0.8, dash="dash"),
            hoverinfo="skip",
            showlegend=False,
        ))

        # Pipe label annotation at midpoint above crown
        mid_x = (start_sta + end_sta) / 2
        mid_crown = max(us_inv, ds_inv) + dia_ft
        label_parts = [pid]
        if diameter_in:
            label_parts.append(f'{diameter_in}"')
        label_parts.append(f"{calc_slope:.4f}")
        label_text = "<br>".join(label_parts)

        fig.add_annotation(
            x=mid_x,
            y=mid_crown + 0.25,
            text=label_text,
            showarrow=False,
            font=dict(size=9, color=TEXT_COLOR),
            align="center",
            bgcolor="rgba(14,17,23,0.6)",
            borderpad=2,
        )

    # ── Manholes ──
    # Build a lookup: station -> (node_id, node_data) for penetration marker logic
    node_station_map = {nid: sta for sta, nid, _ in profile_nodes}

    for sta, nid, ndata in profile_nodes:
        rim_orig = ndata.get("rim_elev")
        inv_orig = ndata.get("invert_elev")
        inv = get_current_value(ledger, nid, "invert_elev", inv_orig)

        node_issues = issue_map.get(nid, [])
        has_depth_issue = any(i.issue_type in ("SHALLOW_STRUCTURE", "DEEP_STRUCTURE")
                              for i in node_issues)
        has_mismatch = any(i.issue_type == "INVERT_MISMATCH" for i in node_issues)
        has_issue = has_depth_issue or has_mismatch

        mh_color = "#FF4444" if has_issue else "#5588aa"
        mh_fill = "rgba(255,68,68,0.15)" if has_issue else "rgba(85,136,170,0.15)"
        mh_border = "#FF4444" if has_issue else "#5588aa"

        if rim_orig is None or inv is None:
            # Fall back to a simple vertical line if missing elevation data
            if rim_orig is not None or inv is not None:
                y0 = float(inv) if inv is not None else float(rim_orig)
                y1 = float(rim_orig) if rim_orig is not None else float(inv)
                fig.add_trace(go.Scatter(
                    x=[sta, sta],
                    y=[y0, y1],
                    mode="lines",
                    line=dict(color=mh_color, width=1.5),
                    hoverinfo="skip",
                    showlegend=False,
                ))
            continue

        rim = float(rim_orig)
        inv_f = float(inv)

        # Manhole rectangle: x from sta-half_w to sta+half_w, y from inv to rim
        mh_xs = [
            sta - mh_half_w, sta + mh_half_w,
            sta + mh_half_w, sta - mh_half_w,
            sta - mh_half_w,
        ]
        mh_ys = [inv_f, inv_f, rim, rim, inv_f]

        hover_mh = (
            f"<b>MH {nid}</b><br>"
            f"Rim: {rim:.3f} ft<br>"
            f"Invert: {inv_f:.3f} ft<br>"
            f"Depth: {rim - inv_f:.2f} ft"
        )
        if has_issue:
            issue_labels = [i.issue_type for i in node_issues]
            hover_mh += "<br><b>Issues: " + ", ".join(issue_labels) + "</b>"

        fig.add_trace(go.Scatter(
            x=mh_xs,
            y=mh_ys,
            mode="lines",
            fill="toself",
            fillcolor=mh_fill,
            line=dict(color=mh_border, width=1.5),
            name=f"MH {nid}",
            hovertemplate=hover_mh + "<extra></extra>",
            showlegend=False,
        ))

        # Rim elevation label above rectangle
        fig.add_annotation(
            x=sta,
            y=rim + 0.15,
            text=f"{rim:.1f}",
            showarrow=False,
            font=dict(size=8, color=AXIS_COLOR),
            yanchor="bottom",
        )

        # Invert elevation label inside rectangle at bottom
        fig.add_annotation(
            x=sta,
            y=inv_f - 0.15,
            text=f"{inv_f:.1f}",
            showarrow=False,
            font=dict(size=8, color=AXIS_COLOR),
            yanchor="top",
        )

        # Node ID label above rim
        fig.add_annotation(
            x=sta,
            y=rim + 0.5,
            text=f"<b>{nid}</b>",
            showarrow=False,
            font=dict(size=9, color="#dddddd"),
            yanchor="bottom",
            textangle=-45,
        )

    # ── Pipe penetration markers at manhole walls ──
    # For each pipe, check the US and DS manhole inverts and flag mismatches
    for start_sta, end_sta, edata in profile_pipes:
        us_inv_orig = edata.get("us_invert")
        ds_inv_orig = edata.get("ds_invert")
        diameter_in = edata.get("diameter")
        pid = str(edata.get("pipe_id", ""))

        if us_inv_orig is None or ds_inv_orig is None:
            continue

        us_inv = get_current_value(ledger, pid, "us_invert", float(us_inv_orig))
        ds_inv = get_current_value(ledger, pid, "ds_invert", float(ds_inv_orig))
        dia_ft = float(diameter_in) / 12.0 if diameter_in else 0.5

        # Find the upstream and downstream nodes for this pipe segment
        us_node_data = None
        ds_node_data = None
        us_node_id = None
        ds_node_id = None
        for sta, nid, ndata in profile_nodes:
            if abs(sta - start_sta) < 0.01:
                us_node_data = ndata
                us_node_id = nid
            if abs(sta - end_sta) < 0.01:
                ds_node_data = ndata
                ds_node_id = nid

        for (pipe_sta, pipe_inv, mh_ndata, mh_nid, side_label) in [
            (start_sta + mh_half_w, us_inv, us_node_data, us_node_id, "US"),
            (end_sta - mh_half_w, ds_inv, ds_node_data, ds_node_id, "DS"),
        ]:
            if mh_ndata is None:
                continue
            mh_inv_orig = mh_ndata.get("invert_elev")
            if mh_inv_orig is None:
                continue
            mh_inv = get_current_value(ledger, mh_nid, "invert_elev", float(mh_inv_orig))

            # Pipe is below manhole invert = problem (red), above = normal (blue)
            pen_color = "#FF4444" if pipe_inv < mh_inv else "#4A90D9"

            # Horizontal bar at the pipe invert height at the manhole wall
            fig.add_trace(go.Scatter(
                x=[pipe_sta - dia_ft * 0.3, pipe_sta + dia_ft * 0.3],
                y=[pipe_inv, pipe_inv],
                mode="lines",
                line=dict(color=pen_color, width=3),
                hovertemplate=(
                    f"<b>{side_label} penetration — Pipe {pid}</b><br>"
                    f"Pipe invert: {pipe_inv:.3f} ft<br>"
                    f"MH invert: {mh_inv:.3f} ft<br>"
                    f"{'BELOW MH INVERT' if pipe_inv < mh_inv else 'Normal'}"
                    "<extra></extra>"
                ),
                showlegend=False,
            ))

            # Small diamond marker
            fig.add_trace(go.Scatter(
                x=[pipe_sta],
                y=[pipe_inv + dia_ft / 2],
                mode="markers",
                marker=dict(symbol="diamond", size=6, color=pen_color,
                            line=dict(color="#0e1117", width=1)),
                hoverinfo="skip",
                showlegend=False,
            ))

    # ── Legend traces (dummy scatter for legend entries) ──
    legend_entries = [
        ("Ground Surface", "#8B6914", "lines"),
        ("Pipe (normal)", "#4A90D9", "lines"),
        ("Adverse Slope", "#FF4444", "lines"),
        ("Diameter Decrease", "#FF8C00", "lines"),
        ("Edited (fix applied)", "#00CC66", "lines"),
        ("Original (before fix)", "#888888", "lines"),
        ("Penetration — normal", "#4A90D9", "markers"),
        ("Penetration — below invert", "#FF4444", "markers"),
    ]
    for leg_name, leg_color, leg_mode in legend_entries:
        marker_kw = dict(symbol="diamond", size=8, color=leg_color) if leg_mode == "markers" else {}
        line_kw = dict(color=leg_color, width=3) if leg_mode == "lines" else {}
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode=leg_mode,
            name=leg_name,
            marker=marker_kw if leg_mode == "markers" else dict(color=leg_color),
            line=line_kw if leg_mode == "lines" else dict(color=leg_color),
            showlegend=True,
        ))

    # ── Layout ──
    fig.update_layout(
        plot_bgcolor=BG,
        paper_bgcolor=BG,
        font=dict(color=TEXT_COLOR, size=11),
        xaxis=dict(
            title="Station (ft)",
            title_font=dict(color=AXIS_COLOR),
            tickfont=dict(color=AXIS_COLOR),
            gridcolor=GRID_COLOR,
            zerolinecolor=GRID_COLOR,
            showgrid=True,
        ),
        yaxis=dict(
            title="Elevation (ft)",
            title_font=dict(color=AXIS_COLOR),
            tickfont=dict(color=AXIS_COLOR),
            gridcolor=GRID_COLOR,
            zerolinecolor=GRID_COLOR,
            showgrid=True,
        ),
        legend=dict(
            bgcolor="rgba(26,26,46,0.85)",
            bordercolor="#333333",
            borderwidth=1,
            font=dict(color=TEXT_COLOR, size=10),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        margin=dict(l=60, r=20, t=60, b=50),
        dragmode="zoom",
        hovermode="closest",
        height=480,
    )

    return fig


# ════════════════════════════════════════════════════════════
# SIDEBAR — Upload, Field Mapping, Settings, Run
# ════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("Sewer Profile Analyzer")
    st.caption("Network QA/QC Tool")

    # ── Upload Section ──
    with st.expander("📁 Upload Data", expanded="analysis" not in st.session_state):
        pipes_files = upload_shapefile("Pipes *", "pipes_upload")
        junctions_files = upload_shapefile("Junctions *", "junctions_upload")
        pumps_files = upload_shapefile("Pumps", "pumps_upload")
        storage_files = upload_shapefile("Storage", "storage_upload")

        # ── Coordinate System ──
        st.markdown("**Coordinate System**")
        CRS_OPTIONS = {
            "Auto-detect from .prj": None,
            "NAD83 / State Plane (ft) — search below": "custom",
            "WGS 84 (EPSG:4326)": "EPSG:4326",
            "NAD83 (EPSG:4269)": "EPSG:4269",
            "NAD83 / UTM zone 10N (EPSG:26910)": "EPSG:26910",
            "NAD83 / UTM zone 11N (EPSG:26911)": "EPSG:26911",
            "NAD83 / UTM zone 12N (EPSG:26912)": "EPSG:26912",
            "NAD83 / UTM zone 13N (EPSG:26913)": "EPSG:26913",
            "NAD83 / UTM zone 14N (EPSG:26914)": "EPSG:26914",
            "NAD83 / UTM zone 15N (EPSG:26915)": "EPSG:26915",
            "NAD83 / UTM zone 16N (EPSG:26916)": "EPSG:26916",
            "NAD83 / UTM zone 17N (EPSG:26917)": "EPSG:26917",
            "NAD83 / UTM zone 18N (EPSG:26918)": "EPSG:26918",
            "NAD83 / UTM zone 19N (EPSG:26919)": "EPSG:26919",
            "NAD83 / California zone 5 ftUS (EPSG:2229)": "EPSG:2229",
            "NAD83 / California zone 6 ftUS (EPSG:2230)": "EPSG:2230",
            "NAD83 / Texas South Central ftUS (EPSG:2278)": "EPSG:2278",
            "NAD83 / Florida East ftUS (EPSG:2236)": "EPSG:2236",
            "NAD83 / Florida West ftUS (EPSG:2237)": "EPSG:2237",
            "NAD83 / New York Long Island ftUS (EPSG:2263)": "EPSG:2263",
            "NAD83 / Pennsylvania South ftUS (EPSG:2272)": "EPSG:2272",
            "NAD83 / Ohio South ftUS (EPSG:3735)": "EPSG:3735",
            "NAD83 / Georgia West ftUS (EPSG:2240)": "EPSG:2240",
            "NAD83 / Virginia North ftUS (EPSG:2283)": "EPSG:2283",
            "NAD83 / Virginia South ftUS (EPSG:2284)": "EPSG:2284",
            "NAD83 / North Carolina ftUS (EPSG:2264)": "EPSG:2264",
            "NAD83 / Colorado North ftUS (EPSG:2231)": "EPSG:2231",
            "NAD83 / Illinois East ftUS (EPSG:3435)": "EPSG:3435",
            "NAD83 / Illinois West ftUS (EPSG:3436)": "EPSG:3436",
            "NAD83 / Washington North ftUS (EPSG:2285)": "EPSG:2285",
            "NAD83 / Washington South ftUS (EPSG:2286)": "EPSG:2286",
            "NAD83 / Oregon North ftIntl (EPSG:2338)": "EPSG:2338",
            "NAD83 / Oregon South ftIntl (EPSG:2339)": "EPSG:2339",
        }
        crs_choice = st.selectbox(
            "Select CRS",
            options=list(CRS_OPTIONS.keys()),
            index=0,
            key="crs_select",
            help="Set the coordinate system of your shapefiles. Use Auto-detect if your upload includes a .prj file.",
        )
        selected_crs = CRS_OPTIONS[crs_choice]

        # Custom EPSG code entry for State Plane or other systems
        custom_epsg = None
        if selected_crs == "custom":
            custom_epsg = st.text_input(
                "Enter EPSG code",
                placeholder="e.g. 2229",
                key="custom_epsg_input",
                help="Look up your State Plane EPSG code at epsg.io",
            )
            if custom_epsg:
                custom_epsg = custom_epsg.strip()
                if not custom_epsg.startswith("EPSG:"):
                    custom_epsg = f"EPSG:{custom_epsg}"

    # Resolve the CRS to apply
    _user_crs = None
    if selected_crs == "custom":
        _user_crs = custom_epsg  # may be None if not entered yet
    elif selected_crs is not None:
        _user_crs = selected_crs

    # Process uploads
    gdfs = {}
    for ftype, files, required in [
        ("pipes", pipes_files, True),
        ("junctions", junctions_files, True),
        ("pumps", pumps_files, False),
        ("storage", storage_files, False),
    ]:
        if files:
            try:
                gdf = read_shapefile_from_upload(files)
                # Apply user-selected CRS: override if specified, otherwise keep auto-detected
                if _user_crs:
                    gdf = gdf.set_crs(_user_crs, allow_override=True)
                gdfs[ftype] = gdf
            except Exception as e:
                st.sidebar.error(f"{ftype}: {e}")

    # Show loaded counts and CRS info
    if gdfs:
        loaded = ", ".join(f"{k}: {len(v)}" for k, v in gdfs.items())
        # Show the active CRS from the first loaded GDF
        _first_gdf = next(iter(gdfs.values()))
        _crs_info = str(_first_gdf.crs) if _first_gdf.crs else "Unknown"
        st.caption(f"Loaded: {loaded}")
        st.caption(f"CRS: {_crs_info}")

        # Warn if coordinates look projected but no CRS is set
        if _first_gdf.crs is None:
            bounds = _first_gdf.total_bounds  # [minx, miny, maxx, maxy]
            if abs(bounds[0]) > 360 or abs(bounds[1]) > 360:
                st.sidebar.warning(
                    "⚠️ Coordinates appear to be in a projected system (not lat/lon) "
                    "but no CRS was detected. Please select the correct coordinate "
                    "system above (e.g. UTM zone or State Plane) or the map will not render correctly."
                )

    # ── Field Mapping ──
    mappings = {}
    if gdfs:
        with st.expander("🔗 Field Mapping", expanded="analysis" not in st.session_state):
            for ftype, gdf in gdfs.items():
                st.markdown(f"**{ftype.title()}**")

                # Quick data preview toggle
                if st.checkbox(f"Preview {ftype} data", key=f"preview_{ftype}"):
                    display_gdf = gdf.drop(columns=["geometry"], errors="ignore")
                    st.dataframe(display_gdf.head(10), height=200, width="stretch")

                mappings[ftype] = field_mapping_ui(gdf, ftype, ftype)
                st.markdown("---")

    # ── Analysis Settings ──
    with st.expander("⚙️ Settings"):
        snap_tolerance = st.slider("Snap Tolerance (m)", 0.1, 20.0, 1.0, 0.1)
        invert_tolerance = st.slider("Invert Mismatch Tolerance (m)", 0.001, 0.5, 0.01, 0.001, format="%.3f")
        min_depth = st.slider("Min Structure Depth (m)", 0.3, 2.0, 0.6, 0.1)
        max_depth = st.slider("Max Structure Depth (m)", 5.0, 20.0, 10.0, 0.5)

    # ── Run Button ──
    st.markdown("---")
    can_run = "pipes" in gdfs and "junctions" in gdfs
    if not can_run:
        st.info("Upload Pipes & Junctions to begin.")

    if can_run and st.button("▶ Run Analysis", type="primary", width="stretch"):
        with st.spinner("Analyzing..."):
            ingestion = {}
            for ftype, gdf in gdfs.items():
                overrides = mappings.get(ftype, {})
                result = ingest_gdf(gdf, ftype, overrides=overrides)
                ingestion[ftype] = result

            network = build_network(
                ingestion["pipes"]["records"],
                ingestion["junctions"]["records"],
                ingestion.get("pumps", {}).get("records"),
                ingestion.get("storage", {}).get("records"),
                snap_tolerance=snap_tolerance,
            )

            thresholds = {
                "invert_mismatch_tolerance_m": invert_tolerance,
                "invert_mismatch_tolerance_ft": invert_tolerance,
                "min_structure_depth_m": min_depth,
                "min_structure_depth_ft": min_depth,
                "max_structure_depth_m": max_depth,
                "max_structure_depth_ft": max_depth,
                "adverse_slope_severity_threshold": -0.01,
            }
            analysis = run_full_analysis(network, thresholds)

            st.session_state["ingestion"] = ingestion
            st.session_state["network"] = network
            st.session_state["analysis"] = analysis
            st.session_state["gdfs"] = gdfs
            st.session_state["mappings"] = mappings
            st.session_state["snap_tolerance"] = snap_tolerance
            st.session_state["thresholds"] = thresholds

            # Clear stale selection/inspection state from previous analysis
            st.session_state["map_selection"] = set()
            st.session_state["inspected_feature"] = None
            st.session_state["edit_ledger"] = []
            st.session_state["preview_entries"] = None

        st.rerun()

    # ── Layer Visibility (only after analysis) ──
    if "analysis" in st.session_state:
        st.markdown("---")
        with st.expander("🗺️ Layer Visibility", expanded=True):
            st.caption("Network Layers")
            vis_pipes = st.checkbox("Pipes", value=True, key="vis_pipes")
            vis_junctions = st.checkbox("Junctions", value=True, key="vis_junctions")
            vis_arrows = st.checkbox("Flow Arrows", value=True, key="vis_arrows")
            vis_pumps = st.checkbox("Pumps", value=True, key="vis_pumps")
            vis_storage = st.checkbox("Storage", value=True, key="vis_storage")

            # Issue layers — only show types that exist
            analysis_ref = st.session_state["analysis"]
            active_issue_types = sorted(set(i.issue_type for i in analysis_ref["issues"]))
            if active_issue_types:
                st.caption("Issue Layers")
                for itype in active_issue_types:
                    display = ISSUE_DISPLAY_NAMES.get(itype, itype.replace("_", " ").title())
                    color = ISSUE_COLORS.get(itype, "#999")
                    st.checkbox(
                        display,
                        value=True,
                        key=f"vis_issue_{itype}",
                    )
                # Resolved issues toggle (only show if there are fixes in the ledger)
                if st.session_state.get("edit_ledger"):
                    st.checkbox(
                        "✔ Resolved Issues",
                        value=True,
                        key="vis_resolved",
                    )

        # ── Selection Tools ──
        with st.expander("🔷 Selection", expanded=True):
            st.toggle("Multi-Select", value=False, key="multi_select_mode",
                      help="ON: clicks add/remove from selection. OFF: clicks inspect the feature.")

            map_sel = st.session_state.get("map_selection", set())
            if map_sel:
                st.markdown(f"**{len(map_sel)}** feature(s) selected")
                if st.button("✕ Clear Selection", key="clear_sel", width="stretch"):
                    st.session_state["map_selection"] = set()
                    st.rerun()

        # ── Filters ──
        st.markdown("---")
        st.markdown("**Filters**")
        all_types = list(set(i.issue_type for i in analysis_ref["issues"]))
        type_filter = st.multiselect(
            "Issue Type", all_types, default=all_types, key="filter_type"
        )

        # Export
        st.markdown("---")
        issues_data = [i.to_dict() for i in analysis_ref["issues"]]
        if issues_data:
            issues_df_export = pd.DataFrame(issues_data)
            issues_df_export = issues_df_export.drop(columns=["coordinates"], errors="ignore")
            if "details" in issues_df_export.columns:
                issues_df_export["details"] = issues_df_export["details"].apply(
                    lambda x: json.dumps(x) if isinstance(x, dict) else str(x)
                )
            csv_buffer = io.StringIO()
            issues_df_export.to_csv(csv_buffer, index=False)
            st.download_button(
                "📥 Export Issues CSV", csv_buffer.getvalue(),
                "sewer_issues.csv", "text/csv", width="stretch",
            )


# ════════════════════════════════════════════════════════════
# MAIN WORKSPACE — Map + Issues
# ════════════════════════════════════════════════════════════

if "analysis" not in st.session_state:
    # Landing state
    st.markdown("## Sewer Profile Analyzer")
    st.markdown(
        "Upload your sewer network shapefiles using the sidebar, "
        "map the fields, and click **Run Analysis** to get started."
    )
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("#### 1. Upload")
        st.markdown("Add your Pipes and Junctions shapefiles (+ optional Pumps/Storage).")
    with col2:
        st.markdown("#### 2. Map Fields")
        st.markdown("Verify auto-detected field mappings match your data schema.")
    with col3:
        st.markdown("#### 3. Analyze")
        st.markdown("Run the analysis to detect profile issues across your network.")

else:
    analysis = st.session_state["analysis"]
    network = st.session_state["network"]
    gdfs = st.session_state["gdfs"]
    stats = network["stats"]
    issues = analysis["issues"]

    # Apply sidebar filters (issue type only, severity removed)
    type_filter = st.session_state.get("filter_type", [])
    filtered_issues = [
        i for i in issues
        if i.issue_type in type_filter
    ]

    # ── Split into unfixed / fixed based on edit ledger ──
    ledger = st.session_state.get("edit_ledger", [])
    edited_feature_ids = set(e.feature_id for e in ledger) if ledger else set()
    unfixed_issues = [i for i in filtered_issues if str(i.feature_id) not in edited_feature_ids]
    fixed_issues = [i for i in filtered_issues if str(i.feature_id) in edited_feature_ids]

    # ── Build visible_layers dict from sidebar checkboxes ──
    visible_layers = {
        "Pipes": st.session_state.get("vis_pipes", True),
        "Junctions": st.session_state.get("vis_junctions", True),
        "Flow Arrows": st.session_state.get("vis_arrows", True),
        "Pumps": st.session_state.get("vis_pumps", True),
        "Storage": st.session_state.get("vis_storage", True),
    }
    # Add issue layer visibility (unfixed only)
    for itype in set(i.issue_type for i in unfixed_issues):
        display = ISSUE_DISPLAY_NAMES.get(itype, itype.replace("_", " ").title())
        visible_layers[display] = st.session_state.get(f"vis_issue_{itype}", True)

    # Add resolved issues visibility
    if st.session_state.get("edit_ledger"):
        visible_layers["Resolved Issues"] = st.session_state.get("vis_resolved", True)

    # ── Metric Bar ──
    render_metric_bar(filtered_issues, stats)

    # ── Main workspace: Map + Detail panel (fragment for fast clicks) ──
    @st.fragment
    def _map_fragment():
        # Process pending click inside the fragment so it runs on fragment reruns too
        _process_map_click()

        map_col, detail_col = st.columns([3, 1])

        with map_col:
            map_selection = st.session_state.get("map_selection", set())
            deck = build_pydeck_map(
                pipes_gdf=gdfs.get("pipes"),
                junctions_gdf=gdfs.get("junctions"),
                pumps_gdf=gdfs.get("pumps"),
                storage_gdf=gdfs.get("storage"),
                issues=unfixed_issues,
                network_result=network,
                selected_ids=map_selection,
                visible_layers=visible_layers,
                fixed_issues=fixed_issues,
            )

            # Render map
            st.pydeck_chart(
                deck,
                height=620,
                on_select="rerun",
                selection_mode="single-object",
                key="main_map",
            )

        with detail_col:
            inspected_fid = st.session_state.get("inspected_feature")
            map_sel = st.session_state.get("map_selection", set())

            if inspected_fid:
                # ── Look up feature ──
                feature_type = None
                feature_row = None

                if "pipes" in gdfs:
                    pid_col = gdfs["pipes"].columns[0]
                    match = gdfs["pipes"][gdfs["pipes"][pid_col].astype(str) == str(inspected_fid)]
                    if len(match) > 0:
                        feature_type = "pipes"
                        feature_row = match.iloc[0]

                if feature_row is None and "junctions" in gdfs:
                    jid_col = gdfs["junctions"].columns[0]
                    match = gdfs["junctions"][gdfs["junctions"][jid_col].astype(str) == str(inspected_fid)]
                    if len(match) > 0:
                        feature_type = "junctions"
                        feature_row = match.iloc[0]

                # Header with selection indicator
                type_label = "Pipe" if feature_type == "pipes" else "Junction" if feature_type == "junctions" else "Feature"
                type_icon = "🔵" if feature_type == "pipes" else "🟢" if feature_type == "junctions" else "📍"
                in_sel = str(inspected_fid) in map_sel
                sel_badge = ' <span style="background:#00FFFF;color:#000;padding:1px 6px;border-radius:3px;font-size:10px;font-weight:600;">SELECTED</span>' if in_sel else ""

                st.markdown(
                    f"{type_icon} **{type_label}**: {inspected_fid}{sel_badge}",
                    unsafe_allow_html=True,
                )

                # Action buttons at top
                btn_c1, btn_c2 = st.columns(2)
                with btn_c1:
                    if st.button("🔍 Zoom", key="zoom_inspected", width="stretch"):
                        bounds = get_feature_bounds(
                            [inspected_fid],
                            pipes_gdf=gdfs.get("pipes"),
                            junctions_gdf=gdfs.get("junctions"),
                            network_result=network,
                        )
                        if bounds:
                            st.session_state["zoom_bounds"] = bounds
                            st.rerun()
                with btn_c2:
                    sel_label = "Remove" if in_sel else "Select"
                    if st.button(f"{'✕' if in_sel else '＋'} {sel_label}", key="toggle_inspected_sel", width="stretch"):
                        sel = st.session_state.get("map_selection", set())
                        if in_sel:
                            sel.discard(str(inspected_fid))
                        else:
                            sel.add(str(inspected_fid))
                        st.session_state["map_selection"] = sel
                        st.rerun()

                # ── Collect data for tabs ──
                feature_issues = [
                    i for i in filtered_issues
                    if str(i.feature_id) == str(inspected_fid)
                ]
                fixable_issues = [i for i in feature_issues if get_strategies(i.issue_type)]

                # Check if this is a junction with a null invert (not covered by pipe issues)
                junction_needs_invert_fix = False
                if feature_type == "junctions" and feature_row is not None:
                    G = network["graph"]
                    node_data = G.nodes[str(inspected_fid)] if hasattr(G, 'nodes') else G._nodes.get(str(inspected_fid), {})
                    junc_inv = node_data.get("invert_elev")
                    if junc_inv is None:
                        junction_needs_invert_fix = True

                fix_count = len(fixable_issues) + (1 if junction_needs_invert_fix else 0)
                issue_count = len(feature_issues)

                # ── Tabs ──
                tab_labels = ["Info"]
                if issue_count:
                    tab_labels.append(f"Issues ({issue_count})")
                if fix_count:
                    tab_labels.append("Fix")
                detail_tabs = st.tabs(tab_labels)

                # ── Info Tab ──
                with detail_tabs[0]:
                    if feature_row is not None:
                        ftype_mappings = st.session_state.get("mappings", {}).get(feature_type, {})
                        mapped_items = []
                        for internal_name, source_col in ftype_mappings.items():
                            if source_col and source_col in feature_row.index:
                                val = feature_row[source_col]
                                if pd.notna(val):
                                    mapped_items.append((internal_name, val))

                        if mapped_items:
                            st.markdown("**Mapped Fields**")
                            for field_name, val in mapped_items:
                                st.markdown(
                                    f'<div style="display:flex;justify-content:space-between;padding:2px 0;font-size:13px;border-bottom:1px solid #eee;">'
                                    f'<span style="color:#666;">{field_name}</span>'
                                    f'<span style="font-weight:500;">{val}</span></div>',
                                    unsafe_allow_html=True,
                                )

                        with st.expander("All Attributes"):
                            for col in feature_row.index:
                                if col != "geometry":
                                    val = feature_row[col]
                                    if pd.notna(val):
                                        st.markdown(
                                            f'<span style="color:#888;font-size:12px;">{col}:</span> '
                                            f'<span style="font-size:12px;">{val}</span>',
                                            unsafe_allow_html=True,
                                        )
                    else:
                        st.caption("Feature not found in loaded data.")

                # ── Issues Tab ──
                if issue_count:
                    with detail_tabs[1]:
                        for issue in feature_issues:
                            color = ISSUE_COLORS.get(issue.issue_type, "#999")
                            display_name = ISSUE_DISPLAY_NAMES.get(issue.issue_type, issue.issue_type)
                            st.markdown(
                                f'<div style="margin:6px 0 2px 0;">'
                                f'<span style="color:{color};font-size:14px;">●</span> '
                                f'<b>{display_name}</b> '
                                f'<span style="font-size:11px;color:#888;">({issue.severity})</span>'
                                f'</div>'
                                f'<div style="font-size:12px;color:#555;margin-left:18px;margin-bottom:6px;">'
                                f'{issue.message}</div>',
                                unsafe_allow_html=True,
                            )

                # ── Fix Tab ──
                if fix_count:
                    fix_tab_idx = 2 if issue_count else 1
                    with detail_tabs[fix_tab_idx]:
                        for idx, issue in enumerate(fixable_issues):
                            strategies = get_strategies(issue.issue_type)
                            display_name = ISSUE_DISPLAY_NAMES.get(issue.issue_type, issue.issue_type)
                            strategy_names = [s[1] for s in strategies]

                            st.markdown(
                                f'<span style="font-size:13px;font-weight:600;">{display_name}</span>',
                                unsafe_allow_html=True,
                            )
                            selected_strategy = st.selectbox(
                                "Strategy",
                                strategy_names,
                                key=f"fix_strategy_{inspected_fid}_{issue.issue_type}_{idx}",
                                label_visibility="collapsed",
                            )

                            strategy_key = next(s[0] for s in strategies if s[1] == selected_strategy)

                            fix_col1, fix_col2 = st.columns(2)
                            with fix_col1:
                                if st.button("Preview", key=f"preview_{inspected_fid}_{idx}",
                                             width="stretch"):
                                    entries = compute_fix(
                                        strategy_key, issue,
                                        network["graph"],
                                        st.session_state["edit_ledger"],
                                    )
                                    st.session_state["preview_entries"] = entries
                                    st.session_state["preview_issue_key"] = f"{inspected_fid}_{idx}"
                                    st.rerun()

                            with fix_col2:
                                if st.button("Apply", key=f"apply_{inspected_fid}_{idx}",
                                             width="stretch", type="primary"):
                                    entries = compute_fix(
                                        strategy_key, issue,
                                        network["graph"],
                                        st.session_state["edit_ledger"],
                                    )
                                    if entries:
                                        apply_group(st.session_state["edit_ledger"], entries)
                                        st.session_state["preview_entries"] = None
                                        st.rerun()
                                    else:
                                        st.warning("No fix available — connected features may also have missing data.")

                            # Show preview if it matches this issue
                            preview = st.session_state.get("preview_entries")
                            preview_key = st.session_state.get("preview_issue_key")
                            if preview is not None and preview_key == f"{inspected_fid}_{idx}":
                                if preview:
                                    preview_data = []
                                    for e in preview:
                                        preview_data.append({
                                            "Feature": e.feature_id,
                                            "Field": e.field,
                                            "Old": f"{e.old_value:.3f}" if e.old_value is not None else "—",
                                            "New": f"{e.new_value:.3f}" if e.new_value is not None else "—",
                                            "Reason": e.reason,
                                        })
                                    st.dataframe(pd.DataFrame(preview_data), hide_index=True,
                                                 width="stretch", height=min(35 * len(preview_data) + 38, 200))
                                else:
                                    st.caption("No changes needed.")

                            if idx < len(fixable_issues) - 1:
                                st.markdown("---")

                        # ── Junction null invert fix ──
                        if junction_needs_invert_fix:
                            if fixable_issues:
                                st.markdown("---")
                            st.markdown(
                                '<span style="font-size:13px;font-weight:600;">Missing Junction Invert</span>',
                                unsafe_allow_html=True,
                            )
                            st.caption("Set invert to the lowest connected pipe invert")

                            jfix_col1, jfix_col2 = st.columns(2)
                            with jfix_col1:
                                if st.button("Preview", key="preview_junc_inv", width="stretch"):
                                    # Create a synthetic issue for the junction
                                    from src.profile_analyzer import ProfileIssue
                                    synth = ProfileIssue(
                                        "NULL_JUNCTION_INVERT", "HIGH", str(inspected_fid),
                                        f"Junction {inspected_fid}", "Missing invert elevation",
                                        {"us_node": str(inspected_fid), "ds_node": str(inspected_fid)},
                                    )
                                    entries = junction_invert_from_lowest_pipe(
                                        synth, network["graph"], st.session_state["edit_ledger"])
                                    st.session_state["preview_entries"] = entries
                                    st.session_state["preview_issue_key"] = "junc_inv"
                                    st.rerun()
                            with jfix_col2:
                                if st.button("Apply", key="apply_junc_inv",
                                             width="stretch", type="primary"):
                                    from src.profile_analyzer import ProfileIssue
                                    synth = ProfileIssue(
                                        "NULL_JUNCTION_INVERT", "HIGH", str(inspected_fid),
                                        f"Junction {inspected_fid}", "Missing invert elevation",
                                        {"us_node": str(inspected_fid), "ds_node": str(inspected_fid)},
                                    )
                                    entries = junction_invert_from_lowest_pipe(
                                        synth, network["graph"], st.session_state["edit_ledger"])
                                    if entries:
                                        apply_group(st.session_state["edit_ledger"], entries)
                                        st.session_state["preview_entries"] = None
                                        st.rerun()
                                    else:
                                        st.warning("No connected pipes with inverts found.")

                            preview = st.session_state.get("preview_entries")
                            preview_key = st.session_state.get("preview_issue_key")
                            if preview is not None and preview_key == "junc_inv":
                                if preview:
                                    preview_data = []
                                    for e in preview:
                                        preview_data.append({
                                            "Feature": e.feature_id,
                                            "Field": e.field,
                                            "Old": f"{e.old_value:.3f}" if e.old_value is not None else "—",
                                            "New": f"{e.new_value:.3f}" if e.new_value is not None else "—",
                                            "Reason": e.reason,
                                        })
                                    st.dataframe(pd.DataFrame(preview_data), hide_index=True,
                                                 width="stretch", height=min(35 * len(preview_data) + 38, 200))
                                else:
                                    st.warning("No connected pipes with inverts found.")

                        # Undo last fix
                        ledger = st.session_state.get("edit_ledger", [])
                        if ledger:
                            summary = ledger_summary(ledger)
                            st.markdown("---")
                            st.caption(f"{summary['total_edits']} pending edit(s) across {summary['total_fixes']} fix(es)")
                            if st.button("Undo Last Fix", key="undo_fix", width="stretch"):
                                undo_last_group(st.session_state["edit_ledger"])
                                st.session_state["preview_entries"] = None
                                st.rerun()

            else:
                # ── No feature selected: Issues Summary ──
                st.markdown("#### Issues Summary")
                st.markdown(render_issues_summary_html(filtered_issues), unsafe_allow_html=True)

                if filtered_issues:
                    st.markdown("---")
                    issue_options = ["(none)"] + [
                        f"{i.feature_id} — {i.issue_type.replace('_', ' ').title()}"
                        for i in filtered_issues
                    ]
                    zoom_pick = st.selectbox(
                        "Zoom to Issue", issue_options, index=0, key="zoom_issue_pick"
                    )
                    if zoom_pick != "(none)":
                        picked_fid = zoom_pick.split(" — ")[0]
                        bounds = get_feature_bounds(
                            [picked_fid],
                            pipes_gdf=gdfs.get("pipes"),
                            junctions_gdf=gdfs.get("junctions"),
                            network_result=network,
                        )
                        if bounds and bounds != st.session_state.get("zoom_bounds"):
                            st.session_state["zoom_bounds"] = bounds
                            st.rerun()
                    elif st.session_state.get("zoom_bounds") is not None:
                        st.session_state["zoom_bounds"] = None
                        st.rerun()

                st.markdown(
                    f"<span style='color:#888;font-size:12px;'>"
                    f"Click a feature on the map to inspect it.<br>"
                    f"{len(filtered_issues)} of {len(issues)} issues</span>",
                    unsafe_allow_html=True,
                )

    _map_fragment()

    # ── Below map: Tabs for Issues, Profile, Pipe Data, Junction Data, Network Info ──
    tab_issues, tab_profile, tab_pipes, tab_junctions, tab_network = st.tabs([
        "📋 Issue Details", "📐 Profile View", "🔵 Pipes", "🟢 Junctions", "🔍 Network Info"
    ])

    with tab_issues:
        issues_data = [i.to_dict() for i in filtered_issues]
        if issues_data:
            issues_df = pd.DataFrame(issues_data)
            issues_df = issues_df.drop(columns=["details", "coordinates"], errors="ignore")

            # Selectable dataframe for multi-row selection
            selection = st.dataframe(
                issues_df,
                width="stretch",
                height=350,
                selection_mode="multi-row",
                on_select="rerun",
                key="issues_selection",
                column_config={
                    "issue_type": st.column_config.TextColumn("Type", width="medium"),
                    "feature_id": st.column_config.TextColumn("Feature", width="small"),
                    "message": st.column_config.TextColumn("Description", width="large"),
                }
            )

            # Get selected row indices
            selected_rows = selection.selection.rows if selection and selection.selection else []

            # Build set of selected feature IDs for cross-tab filtering
            selected_feature_ids = set()
            if selected_rows:
                for row_idx in selected_rows:
                    if row_idx < len(issues_df):
                        selected_feature_ids.add(str(issues_df.iloc[row_idx]["feature_id"]))
            st.session_state["selected_feature_ids"] = selected_feature_ids

            # Selection action bar
            if selected_rows:
                sel_col1, sel_col2, sel_col3 = st.columns([1, 1, 2])
                with sel_col1:
                    if st.button("🔍 Zoom to Selection", width="stretch"):
                        bounds = get_feature_bounds(
                            list(selected_feature_ids),
                            pipes_gdf=gdfs.get("pipes"),
                            junctions_gdf=gdfs.get("junctions"),
                            network_result=network,
                        )
                        if bounds:
                            st.session_state["zoom_bounds"] = bounds
                            st.session_state["_map_render_key"] += 1
                            st.rerun()
                with sel_col2:
                    if st.button("🔄 Reset Zoom", width="stretch"):
                        st.session_state["zoom_bounds"] = None
                        st.rerun()
                with sel_col3:
                    st.caption(f"{len(selected_rows)} issue(s) selected")
        else:
            st.session_state["selected_feature_ids"] = set()
            st.success("No issues match the current filters.")

    with tab_profile:
        map_sel = st.session_state.get("map_selection", set())
        inspected = st.session_state.get("inspected_feature")
        _profile_target = list(map_sel) if map_sel else ([inspected] if inspected else [])
        if _profile_target:
            if st.button("Generate Profile", key="gen_profile", type="primary"):
                st.session_state["_show_profile"] = True
            if st.session_state.get("_show_profile"):
                fig = build_profile_plotly(set(_profile_target), network, gdfs, filtered_issues,
                                           ledger=st.session_state.get("edit_ledger", []))
                if fig:
                    st.plotly_chart(fig, use_container_width=True,
                                    config={"scrollZoom": True})
                else:
                    st.info("No pipe data found for the selected features.")
        else:
            st.info("Click a feature on the map to view its profile, "
                    "or use **Multi-Select** to select multiple features.")

    # ── Map selection drives table filtering (ArcGIS Pro behavior) ──
    map_selection = st.session_state.get("map_selection", set())

    with tab_pipes:
        if "pipes" in gdfs:
            pipes_display = gdfs["pipes"].drop(columns=["geometry"], errors="ignore")
            total_pipes = len(pipes_display)

            # Auto-filter when map selection is active
            if map_selection:
                id_col = pipes_display.columns[0]
                pipes_display = pipes_display[
                    pipes_display[id_col].astype(str).isin(map_selection)
                ]
                if len(pipes_display) > 0:
                    st.info(f"Filtered to {len(pipes_display)} of {total_pipes} pipes (map selection)")
                else:
                    st.caption(f"No pipes in current selection ({total_pipes} total)")

            st.dataframe(pipes_display, width="stretch", height=400)
            st.caption(f"{len(pipes_display)} pipes")
        else:
            st.info("No pipe data loaded.")

    with tab_junctions:
        if "junctions" in gdfs:
            juncs_display = gdfs["junctions"].drop(columns=["geometry"], errors="ignore")
            total_juncs = len(juncs_display)

            # Auto-filter when map selection is active
            if map_selection:
                id_col = juncs_display.columns[0]
                juncs_display = juncs_display[
                    juncs_display[id_col].astype(str).isin(map_selection)
                ]
                if len(juncs_display) > 0:
                    st.info(f"Filtered to {len(juncs_display)} of {total_juncs} junctions (map selection)")
                else:
                    st.caption(f"No junctions in current selection ({total_juncs} total)")

            st.dataframe(juncs_display, width="stretch", height=400)
            st.caption(f"{len(juncs_display)} junctions")
        else:
            st.info("No junction data loaded.")

    with tab_network:
        net_c1, net_c2 = st.columns(2)
        with net_c1:
            st.markdown("**Network Statistics**")
            st.markdown(f"- Pipes: **{stats['total_edges']}**")
            st.markdown(f"- Nodes: **{stats['total_nodes']}**")
            st.markdown(f"- Connected components: **{stats['connected_components']}**")
            st.markdown(f"- Largest component: **{stats['largest_component_size']}** nodes")
            st.markdown(f"- Virtual nodes created: **{stats['virtual_nodes_created']}**")

        with net_c2:
            st.markdown("**Connectivity**")
            if stats["source_nodes"]:
                st.markdown(f"- Source nodes: {', '.join(str(n) for n in stats['source_nodes'][:10])}"
                            f"{'...' if len(stats['source_nodes']) > 10 else ''}")
            if stats["dead_end_nodes"]:
                st.markdown(f"- Dead ends: {', '.join(str(n) for n in stats['dead_end_nodes'][:10])}"
                            f"{'...' if len(stats['dead_end_nodes']) > 10 else ''}")
            if stats["orphan_nodes"]:
                st.markdown(f"- Orphan nodes: {', '.join(str(n) for n in stats['orphan_nodes'][:10])}"
                            f"{'...' if len(stats['orphan_nodes']) > 10 else ''}")
            if not stats["dead_end_nodes"] and not stats["orphan_nodes"]:
                st.markdown("- All nodes properly connected")

            if network.get("snap_log"):
                with st.expander(f"Snap Log ({len(network['snap_log'])} actions)"):
                    for entry in network["snap_log"][:50]:
                        st.text(entry)
