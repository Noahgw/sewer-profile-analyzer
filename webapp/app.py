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
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
import numpy as np
from streamlit_folium import st_folium
from shapely.geometry import box as shapely_box

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from webapp.ingest_gpd import (
    auto_detect_fields, get_required_fields, load_field_config,
    ingest_gdf, read_shapefile_from_upload
)
from src.network_builder import build_network
from src.profile_analyzer import run_full_analysis, trace_profile
from webapp.map_builder import (
    build_base_map, add_issues_to_map, add_selection_layer,
    ISSUE_COLORS, ISSUE_DISPLAY_NAMES, get_feature_bounds
)
from webapp.fix_toolkit import (
    LedgerEntry, apply_group, undo_last_group, get_current_value,
    get_all_edits, ledger_summary, get_strategies, compute_fix,
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
if "_prev_map_click" not in st.session_state:
    st.session_state["_prev_map_click"] = None
if "_map_render_key" not in st.session_state:
    st.session_state["_map_render_key"] = 0
if "_prev_click_fid" not in st.session_state:
    st.session_state["_prev_click_fid"] = None
if "_prev_click_time" not in st.session_state:
    st.session_state["_prev_click_time"] = 0.0
if "inspected_feature" not in st.session_state:
    st.session_state["inspected_feature"] = None
if "_last_processed_drawing" not in st.session_state:
    st.session_state["_last_processed_drawing"] = None
if "box_select_mode" not in st.session_state:
    st.session_state["box_select_mode"] = False
if "box_select_corner1" not in st.session_state:
    st.session_state["box_select_corner1"] = None
if "edit_ledger" not in st.session_state:
    st.session_state["edit_ledger"] = []
if "preview_entries" not in st.session_state:
    st.session_state["preview_entries"] = None


# ════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════

def _save_map_view(map_result):
    """Save current map center/zoom to session state before a rerun."""
    if map_result and map_result.get("center"):
        c = map_result["center"]
        st.session_state["_map_center"] = [c["lat"], c["lng"]]
    if map_result and map_result.get("zoom") is not None:
        st.session_state["_map_zoom"] = map_result["zoom"]


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


def build_profile_figure(selected_ids, network, gdfs, issues, ledger=None):
    """Build a matplotlib profile view for selected pipes/junctions.

    Shows pipe inverts as sloped lines with diameter indicated,
    junction rim/invert elevations, and highlights issues.
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
    node_order = []
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
    stations = []  # cumulative distance
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

    # ── Plot ──
    fig, ax = plt.subplots(figsize=(14, 5))
    fig.patch.set_facecolor("#0e1117")
    ax.set_facecolor("#0e1117")

    if ledger is None:
        ledger = []

    # Draw pipes as sloped bands showing diameter
    for start_sta, end_sta, edata in profile_pipes:
        us_inv_orig = edata.get("us_invert")
        ds_inv_orig = edata.get("ds_invert")
        diameter_in = edata.get("diameter")
        pid = str(edata.get("pipe_id", ""))

        if us_inv_orig is None or ds_inv_orig is None:
            continue

        us_inv_orig = float(us_inv_orig)
        ds_inv_orig = float(ds_inv_orig)

        # Apply ledger edits
        us_inv = get_current_value(ledger, pid, "us_invert", us_inv_orig)
        ds_inv = get_current_value(ledger, pid, "ds_invert", ds_inv_orig)
        was_edited = (us_inv != us_inv_orig or ds_inv != ds_inv_orig)
        dia_ft = float(diameter_in) / 12.0 if diameter_in else 0.5

        # Pipe invert line (bottom of pipe)
        pipe_issues = issue_map.get(pid, [])
        has_adverse = any(i.issue_type == "ADVERSE_SLOPE" for i in pipe_issues)
        has_dia_decrease = any(i.issue_type == "DIAMETER_DECREASE" for i in pipe_issues)

        if has_adverse:
            color = "#FF4444"
            lw = 2.5
        elif has_dia_decrease:
            color = "#FF8C00"
            lw = 2.5
        else:
            color = "#4A90D9"
            lw = 1.5

        # If edited, draw original as faded ghost
        if was_edited:
            orig_inverts = [us_inv_orig, ds_inv_orig]
            orig_crowns = [us_inv_orig + dia_ft, ds_inv_orig + dia_ft]
            ax.plot([start_sta, end_sta], orig_inverts, color="#888888",
                    linewidth=1, linestyle=":", alpha=0.5, zorder=2)
            ax.plot([start_sta, end_sta], orig_crowns, color="#888888",
                    linewidth=0.5, linestyle=":", alpha=0.3, zorder=2)
            color = "#00CC66"  # Green for edited pipes
            lw = 2.5

        # Draw pipe as a band (invert to crown)
        xs = [start_sta, end_sta]
        inverts = [us_inv, ds_inv]
        crowns = [us_inv + dia_ft, ds_inv + dia_ft]

        ax.fill_between(xs, inverts, crowns, alpha=0.2, color=color)
        ax.plot(xs, inverts, color=color, linewidth=lw, solid_capstyle="round")
        ax.plot(xs, crowns, color=color, linewidth=0.8, linestyle="--", alpha=0.5)

        # Label pipe
        mid_x = (start_sta + end_sta) / 2
        mid_y = (us_inv + ds_inv) / 2 + dia_ft + 0.3
        label = f'{pid}'
        if diameter_in:
            label += f'\n{diameter_in}"'
        slope = edata.get("slope")
        if slope is not None:
            label += f"\n{float(slope):.4f}"
        elif us_inv and ds_inv and (end_sta - start_sta) > 0:
            calc_slope = (us_inv - ds_inv) / (end_sta - start_sta)
            label += f"\n{calc_slope:.4f}"

        ax.text(mid_x, mid_y, label, ha="center", va="bottom",
                fontsize=7, color="#cccccc", alpha=0.9)

    # Draw ground surface line (rim elevations)
    rim_stations = []
    rim_elevations = []
    for sta, nid, ndata in profile_nodes:
        rim = ndata.get("rim_elev")
        if rim is not None:
            rim_stations.append(sta)
            rim_elevations.append(float(rim))
    if len(rim_stations) >= 2:
        ax.plot(rim_stations, rim_elevations, color="#8B6914", linewidth=2,
                linestyle="-", label="Ground Surface", zorder=3)
        ax.fill_between(rim_stations, rim_elevations,
                        [max(rim_elevations) + 2] * len(rim_stations),
                        color="#8B6914", alpha=0.08)

    # Draw junction markers
    for sta, nid, ndata in profile_nodes:
        rim = ndata.get("rim_elev")
        inv_orig = ndata.get("invert_elev")
        inv = get_current_value(ledger, nid, "invert_elev", inv_orig)

        node_issues = issue_map.get(nid, [])
        has_depth_issue = any(i.issue_type in ("SHALLOW_STRUCTURE", "DEEP_STRUCTURE")
                             for i in node_issues)
        has_mismatch = any(i.issue_type == "INVERT_MISMATCH" for i in node_issues)

        marker_color = "#FF4444" if (has_depth_issue or has_mismatch) else "#00CC66"

        if rim is not None:
            rim = float(rim)
            ax.plot(sta, rim, "v", color=marker_color, markersize=8, zorder=5)
            ax.text(sta, rim + 0.3, f"{rim:.1f}", ha="center", va="bottom",
                    fontsize=6, color="#aaaaaa")

        if inv is not None:
            inv = float(inv)
            ax.plot(sta, inv, "^", color=marker_color, markersize=8, zorder=5)
            ax.text(sta, inv - 0.5, f"{inv:.1f}", ha="center", va="top",
                    fontsize=6, color="#aaaaaa")

        # Draw vertical structure line
        if rim is not None and inv is not None:
            line_color = "#FF4444" if (has_depth_issue or has_mismatch) else "#555555"
            ax.plot([sta, sta], [inv, rim], color=line_color,
                    linewidth=1.5, linestyle="-", alpha=0.6)

        # Node label
        label_y = (float(rim) if rim else float(inv)) if (rim or inv) else 0
        ax.text(sta, label_y + 0.8, nid, ha="center", va="bottom",
                fontsize=7, color="#dddddd", fontweight="bold", rotation=45)

    # Styling
    ax.set_xlabel("Station (ft)", color="#aaaaaa", fontsize=10)
    ax.set_ylabel("Elevation (ft)", color="#aaaaaa", fontsize=10)
    ax.tick_params(colors="#888888", labelsize=8)
    ax.grid(True, alpha=0.15, color="#555555")
    for spine in ax.spines.values():
        spine.set_color("#333333")

    # Legend
    legend_items = [
        mpatches.Patch(color="#8B6914", alpha=0.6, label="Ground surface (rim)"),
        mpatches.Patch(color="#4A90D9", alpha=0.4, label="Pipe (normal)"),
        mpatches.Patch(color="#FF4444", alpha=0.4, label="Adverse slope"),
        mpatches.Patch(color="#FF8C00", alpha=0.4, label="Diameter decrease"),
        mpatches.Patch(color="#00CC66", alpha=0.4, label="Edited (fix applied)"),
        mpatches.Patch(color="#888888", alpha=0.3, label="Original (before fix)"),
    ]
    ax.legend(handles=legend_items, loc="upper right", fontsize=7,
              facecolor="#1a1a2e", edgecolor="#333333", labelcolor="#cccccc")

    fig.tight_layout()
    return fig


def render_issues_summary(issues):
    """Render a clean issue summary grouped by type."""
    if not issues:
        st.success("No issues detected. Your network is clean.")
        return

    # Group by type
    by_type = {}
    for issue in issues:
        key = issue.issue_type
        if key not in by_type:
            by_type[key] = []
        by_type[key].append(issue)

    html = ""
    for itype, issue_list in sorted(by_type.items(), key=lambda x: -len(x[1])):
        count = len(issue_list)
        color = ISSUE_COLORS.get(itype, "#999")
        display_name = itype.replace("_", " ").title()

        html += f"""<div class="issue-row">
            <span style="color:{color};font-size:16px;margin-right:8px;">●</span>
            <span style="flex:1;font-weight:500;">{display_name}</span>
            <span style="font-weight:700;font-size:16px;">{count}</span>
        </div>"""

    st.markdown(html, unsafe_allow_html=True)


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
                gdfs[ftype] = gdf
            except Exception as e:
                st.sidebar.error(f"{ftype}: {e}")

    # Show loaded counts
    if gdfs:
        loaded = ", ".join(f"{k}: {len(v)}" for k, v in gdfs.items())
        st.caption(f"Loaded: {loaded}")

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
        snap_tolerance = st.slider("Snap Tolerance", 0.1, 20.0, 1.0, 0.1)
        invert_tolerance = st.slider("Invert Mismatch Tolerance (ft)", 0.01, 1.0, 0.02, 0.01)
        min_depth = st.slider("Min Structure Depth (ft)", 0.5, 5.0, 2.0, 0.5)
        max_depth = st.slider("Max Structure Depth (ft)", 15.0, 50.0, 30.0, 1.0)

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
                "invert_mismatch_tolerance_ft": invert_tolerance,
                "min_structure_depth_ft": min_depth,
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
            st.session_state["_prev_map_click"] = None
            st.session_state["zoom_bounds"] = None
            st.session_state["_last_processed_drawing"] = None
            st.session_state["edit_ledger"] = []
            st.session_state["preview_entries"] = None

            # Pre-convert GDFs to WGS84 so box-select doesn't re-convert every time
            gdfs_4326 = {}
            for ftype, gdf in gdfs.items():
                if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
                    gdfs_4326[ftype] = gdf.to_crs(epsg=4326)
                elif gdf.crs is None:
                    gdfs_4326[ftype] = gdf.set_crs(epsg=4326)
                else:
                    gdfs_4326[ftype] = gdf
            st.session_state["gdfs_4326"] = gdfs_4326

        st.rerun()

    # ── Layer Visibility (only after analysis) ──
    if "analysis" in st.session_state:
        st.markdown("---")
        with st.expander("🗺️ Layer Visibility", expanded=True):
            st.caption("Network Layers")
            vis_pipes = st.checkbox("Pipes", value=True, key="vis_pipes")
            vis_junctions = st.checkbox("Junctions", value=True, key="vis_junctions")
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

        # ── Selection Tools (ArcGIS Pro-style) ──
        with st.expander("🔷 Selection", expanded=True):
            st.toggle("Select on Map", key="select_on_map", value=False)
            st.radio(
                "Mode", ["Add to Selection", "Remove from Selection"],
                key="selection_action", horizontal=True,
                label_visibility="collapsed",
            )

            # ── Selection count & actions ──
            map_sel = st.session_state.get("map_selection", set())
            gdfs_ref = st.session_state.get("gdfs", {})
            if map_sel:
                pipe_sel_count = 0
                junc_sel_count = 0
                if "pipes" in gdfs_ref:
                    pipe_id_col = gdfs_ref["pipes"].columns[0]
                    pipe_sel_count = len(
                        map_sel & set(gdfs_ref["pipes"][pipe_id_col].astype(str))
                    )
                if "junctions" in gdfs_ref:
                    junc_id_col = gdfs_ref["junctions"].columns[0]
                    junc_sel_count = len(
                        map_sel & set(gdfs_ref["junctions"][junc_id_col].astype(str))
                    )
                st.markdown(f"**{len(map_sel)}** feature(s) selected")
                if pipe_sel_count:
                    st.caption(f"Pipes: {pipe_sel_count}")
                if junc_sel_count:
                    st.caption(f"Junctions: {junc_sel_count}")

                sel_c1, sel_c2 = st.columns(2)
                with sel_c1:
                    if st.button("✕ Clear", key="clear_sel", use_container_width=True):
                        st.session_state["map_selection"] = set()
                        st.session_state["_prev_map_click"] = None
                        st.session_state["_last_processed_drawing"] = None
                        st.rerun()
                with sel_c2:
                    if st.button("🔍 Zoom", key="zoom_sel", use_container_width=True):
                        bounds = get_feature_bounds(
                            list(map_sel),
                            pipes_gdf=gdfs_ref.get("pipes"),
                            junctions_gdf=gdfs_ref.get("junctions"),
                            network_result=st.session_state.get("network"),
                        )
                        if bounds:
                            st.session_state["zoom_bounds"] = bounds
                            st.session_state["_map_render_key"] += 1
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

    # ── Build visible_layers dict from sidebar checkboxes ──
    visible_layers = {
        "Pipes": st.session_state.get("vis_pipes", True),
        "Junctions": st.session_state.get("vis_junctions", True),
        "Pumps": st.session_state.get("vis_pumps", True),
        "Storage": st.session_state.get("vis_storage", True),
    }
    # Add issue layer visibility
    for itype in set(i.issue_type for i in filtered_issues):
        display = ISSUE_DISPLAY_NAMES.get(itype, itype.replace("_", " ").title())
        visible_layers[display] = st.session_state.get(f"vis_issue_{itype}", True)

    # ── Retrieve zoom bounds (set by Zoom to Selection / Zoom to Issue) ──
    zoom_bounds = st.session_state.get("zoom_bounds", None)

    # ── Metric Bar ──
    render_metric_bar(filtered_issues, stats)

    # ── Main workspace: Map + Issue Summary side by side ──
    map_col, detail_col = st.columns([3, 1])

    with map_col:
        # Selection mode banner
        if st.session_state.get("select_on_map", False):
            action = st.session_state.get("selection_action", "Add to Selection")
            sel_count = len(st.session_state.get("map_selection", set()))
            st.markdown(
                f'<div style="background:#00FFFF22;border:1px solid #00FFFF;border-radius:4px;'
                f'padding:6px 12px;margin-bottom:8px;font-size:13px;color:#00FFFF;">'
                f'<b>Selection Mode:</b> {action} &nbsp;·&nbsp; '
                f'{sel_count} feature(s) selected &nbsp;·&nbsp; '
                f'Click features or draw a box to select</div>',
                unsafe_allow_html=True,
            )

        # Build map with visibility state and zoom
        m = build_base_map(
            pipes_gdf=gdfs.get("pipes"),
            junctions_gdf=gdfs.get("junctions"),
            pumps_gdf=gdfs.get("pumps"),
            storage_gdf=gdfs.get("storage"),
            visible_layers=visible_layers,
            zoom_bounds=zoom_bounds,
            network_result=network,
        )
        m = add_issues_to_map(
            m, filtered_issues,
            pipes_gdf=gdfs.get("pipes"),
            junctions_gdf=gdfs.get("junctions"),
            network_result=network,
            visible_layers=visible_layers,
            add_layer_control=False,
        )

        # Add selection highlights (ArcGIS Pro-style cyan)
        map_selection = st.session_state.get("map_selection", set())
        m = add_selection_layer(m, map_selection, gdfs.get("pipes"), gdfs.get("junctions"))

        # ── Box Select: Draw plugin with rectangle tool always available ──
        from folium.plugins import Draw
        Draw(
            export=False,
            draw_options={
                "rectangle": {
                    "shapeOptions": {
                        "color": "#00FFFF",
                        "weight": 2,
                        "fillColor": "#00FFFF",
                        "fillOpacity": 0.15,
                    },
                },
                "polyline": False,
                "polygon": False,
                "circle": False,
                "marker": False,
                "circlemarker": False,
            },
            edit_options={"edit": False, "remove": False},
        ).add_to(m)

        # Render interactive map, preserving zoom/center across selection reruns
        render_key = st.session_state.get("_map_render_key", 0)
        saved_center = st.session_state.pop("_map_center", None)
        saved_zoom = st.session_state.pop("_map_zoom", None)
        folium_kwargs = dict(height=620, use_container_width=True,
                             key=f"main_map_{render_key}")
        if saved_center and saved_zoom is not None:
            folium_kwargs["center"] = saved_center
            folium_kwargs["zoom"] = saved_zoom
        map_result = st_folium(m, **folium_kwargs)

        # ── Process drawn rectangles (Box Select) ──
        if map_result and st.session_state.get("select_on_map", False):
            # Check all_drawings first (list), then last_active_drawing
            all_drawings = map_result.get("all_drawings") or []
            drawing = all_drawings[-1] if all_drawings else map_result.get("last_active_drawing")
            if drawing and drawing != st.session_state.get("_last_processed_drawing"):
                # Handle various drawing formats from streamlit-folium
                geom = drawing.get("geometry", drawing)
                geom_type = geom.get("type", "")
                if geom_type in ("Polygon", "Rectangle") or "coordinates" in geom:
                    st.session_state["_last_processed_drawing"] = drawing
                    coords = geom["coordinates"][0]  # GeoJSON [lon, lat]
                    lons = [c[0] for c in coords]
                    lats = [c[1] for c in coords]
                    bbox = shapely_box(min(lons), min(lats), max(lons), max(lats))

                    found_ids = set()
                    gdfs_4326 = st.session_state.get("gdfs_4326", {})
                    if "pipes" in gdfs_4326:
                        p_gdf = gdfs_4326["pipes"]
                        pid_col = p_gdf.columns[0]
                        for _, row in p_gdf.iterrows():
                            if row.geometry and not row.geometry.is_empty and bbox.intersects(row.geometry):
                                found_ids.add(str(row[pid_col]))
                    if "junctions" in gdfs_4326:
                        j_gdf = gdfs_4326["junctions"]
                        jid_col = j_gdf.columns[0]
                        for _, row in j_gdf.iterrows():
                            if row.geometry and not row.geometry.is_empty and bbox.intersects(row.geometry):
                                found_ids.add(str(row[jid_col]))

                    if found_ids:
                        sel = st.session_state.get("map_selection", set())
                        action = st.session_state.get("selection_action", "Add to Selection")
                        if action == "Add to Selection":
                            sel |= found_ids
                        else:
                            sel -= found_ids
                        st.session_state["map_selection"] = sel
                        _save_map_view(map_result)
                        st.rerun()

        # ── Handle clicks ──
        if map_result:
            click_data = map_result.get("last_object_clicked")
            prev_click = st.session_state.get("_prev_map_click")
            if click_data and click_data != prev_click:
                st.session_state["_prev_map_click"] = click_data
                tooltip = map_result.get("last_object_clicked_tooltip")
                if tooltip:
                    fid = tooltip.split(": ", 1)[1] if ": " in tooltip else tooltip

                    if st.session_state.get("select_on_map", False):
                        # ── Select mode: add/remove from selection ──
                        sel = st.session_state.get("map_selection", set())
                        action = st.session_state.get("selection_action", "Add to Selection")
                        if action == "Add to Selection":
                            sel.add(fid)
                        else:
                            sel.discard(fid)
                        st.session_state["map_selection"] = sel
                    else:
                        # ── Inspect mode: show feature details ──
                        st.session_state["inspected_feature"] = fid
                    _save_map_view(map_result)
                    st.rerun()

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
                if st.button("🔍 Zoom", key="zoom_inspected", use_container_width=True):
                    bounds = get_feature_bounds(
                        [inspected_fid],
                        pipes_gdf=gdfs.get("pipes"),
                        junctions_gdf=gdfs.get("junctions"),
                        network_result=network,
                    )
                    if bounds:
                        st.session_state["zoom_bounds"] = bounds
                        st.session_state["_map_render_key"] += 1
                        st.rerun()
            with btn_c2:
                sel_label = "Remove" if in_sel else "Select"
                if st.button(f"{'✕' if in_sel else '＋'} {sel_label}", key="toggle_inspected_sel", use_container_width=True):
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
            fix_count = len(fixable_issues)
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
                                         use_container_width=True):
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
                                         use_container_width=True, type="primary"):
                                entries = compute_fix(
                                    strategy_key, issue,
                                    network["graph"],
                                    st.session_state["edit_ledger"],
                                )
                                if entries:
                                    apply_group(st.session_state["edit_ledger"], entries)
                                    st.session_state["preview_entries"] = None
                                    st.rerun()

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
                                             use_container_width=True, height=min(35 * len(preview_data) + 38, 200))
                            else:
                                st.caption("No changes needed.")

                        if idx < fix_count - 1:
                            st.markdown("---")

                    # Undo last fix
                    ledger = st.session_state.get("edit_ledger", [])
                    if ledger:
                        summary = ledger_summary(ledger)
                        st.markdown("---")
                        st.caption(f"{summary['total_edits']} pending edit(s) across {summary['total_fixes']} fix(es)")
                        if st.button("Undo Last Fix", key="undo_fix", use_container_width=True):
                            undo_last_group(st.session_state["edit_ledger"])
                            st.session_state["preview_entries"] = None
                            st.rerun()

        else:
            # ── No feature selected: Issues Summary ──
            st.markdown("#### Issues Summary")
            render_issues_summary(filtered_issues)

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
                        st.session_state["_map_render_key"] += 1
                        st.rerun()
                elif st.session_state.get("zoom_bounds") is not None:
                    st.session_state["zoom_bounds"] = None
                    st.session_state["_map_render_key"] += 1
                    st.rerun()

            st.markdown(
                f"<span style='color:#888;font-size:12px;'>"
                f"Click a feature on the map to inspect it.<br>"
                f"{len(filtered_issues)} of {len(issues)} issues</span>",
                unsafe_allow_html=True,
            )

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
                        st.session_state["_map_render_key"] += 1
                        st.rerun()
                with sel_col3:
                    st.caption(f"{len(selected_rows)} issue(s) selected")
        else:
            st.session_state["selected_feature_ids"] = set()
            st.success("No issues match the current filters.")

    with tab_profile:
        map_sel = st.session_state.get("map_selection", set())
        if map_sel:
            fig = build_profile_figure(map_sel, network, gdfs, filtered_issues,
                                       ledger=st.session_state.get("edit_ledger", []))
            if fig:
                st.pyplot(fig, use_container_width=True)
                plt.close(fig)
            else:
                st.info("No pipe data found for the selected features.")
        else:
            st.info("Select features on the map to view their profile. "
                    "Enable **Select on Map** in the sidebar, then click pipes or use box-select.")

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
