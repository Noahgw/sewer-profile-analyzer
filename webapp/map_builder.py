"""
map_builder.py — Build interactive pydeck maps with color-coded issue overlays.

Uses deck.gl via pydeck for GPU-accelerated WebGL rendering.
Handles 3500+ pipes smoothly — no page rerun on pan/zoom.
"""

import pydeck as pdk
import geopandas as gpd
import json
import math


# Issue type -> [R, G, B] color mapping
ISSUE_COLORS_RGB = {
    "ADVERSE_SLOPE": [255, 0, 0],
    "INVERT_MISMATCH": [255, 140, 0],
    "DIAMETER_DECREASE": [255, 215, 0],
    "NULL_DIAMETER": [148, 0, 211],
    "NULL_INVERT": [148, 0, 211],
    "DEAD_END": [30, 144, 255],
    "ORPHAN_NODE": [128, 128, 128],
    "SHALLOW_STRUCTURE": [0, 206, 209],
    "DEEP_STRUCTURE": [139, 69, 19],
}

# Hex colors for UI labels
ISSUE_COLORS = {
    "ADVERSE_SLOPE": "#FF0000",
    "INVERT_MISMATCH": "#FF8C00",
    "DIAMETER_DECREASE": "#FFD700",
    "NULL_DIAMETER": "#9400D3",
    "NULL_INVERT": "#9400D3",
    "DEAD_END": "#1E90FF",
    "ORPHAN_NODE": "#808080",
    "SHALLOW_STRUCTURE": "#00CED1",
    "DEEP_STRUCTURE": "#8B4513",
}

ISSUE_DISPLAY_NAMES = {
    "ADVERSE_SLOPE": "Adverse Slope",
    "INVERT_MISMATCH": "Invert Mismatch",
    "DIAMETER_DECREASE": "Diameter Decrease",
    "NULL_DIAMETER": "Null Diameter",
    "NULL_INVERT": "Null Invert",
    "DEAD_END": "Dead End",
    "ORPHAN_NODE": "Orphan Node",
    "SHALLOW_STRUCTURE": "Shallow Structure",
    "DEEP_STRUCTURE": "Deep Structure",
}

PIPE_ISSUE_TYPES = {"ADVERSE_SLOPE", "NULL_DIAMETER", "NULL_INVERT", "DIAMETER_DECREASE"}


def _ensure_wgs84(gdf):
    """Reproject a GeoDataFrame to WGS84 (EPSG:4326) for mapping."""
    if gdf.crs is None:
        bounds = gdf.total_bounds
        if abs(bounds[0]) <= 360 and abs(bounds[1]) <= 180:
            gdf = gdf.set_crs(epsg=4326)
        else:
            return gdf  # can't convert without CRS
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def _extract_path_coords(geom):
    """Extract list of [lon, lat] for pydeck PathLayer from a line geometry."""
    if geom is None or geom.is_empty:
        return None
    gt = geom.geom_type
    if gt == "LineString":
        return [[c[0], c[1]] for c in geom.coords]
    if gt == "MultiLineString":
        coords = []
        for part in geom.geoms:
            coords.extend([c[0], c[1]] for c in part.coords)
        return coords
    return None


def _midpoint_arrow(path, arrow_len=0.00015):
    """
    Compute a short V-shaped arrow at the midpoint of a path,
    pointing in the flow direction.
    Returns list of [lon, lat] pairs for the arrow, or None.
    """
    if not path or len(path) < 2:
        return None

    # Find total length and midpoint segment
    dists = [0.0]
    for i in range(1, len(path)):
        dx = path[i][0] - path[i-1][0]
        dy = path[i][1] - path[i-1][1]
        dists.append(dists[-1] + math.sqrt(dx*dx + dy*dy))
    total = dists[-1]
    if total == 0:
        return None

    half = total / 2.0
    for i in range(1, len(dists)):
        if dists[i] >= half:
            seg_len = dists[i] - dists[i-1]
            frac = (half - dists[i-1]) / seg_len if seg_len > 0 else 0
            mx = path[i-1][0] + frac * (path[i][0] - path[i-1][0])
            my = path[i-1][1] + frac * (path[i][1] - path[i-1][1])
            # Direction vector
            dx = path[i][0] - path[i-1][0]
            dy = path[i][1] - path[i-1][1]
            d = math.sqrt(dx*dx + dy*dy)
            if d == 0:
                return None
            dx, dy = dx/d, dy/d
            # Arrow tip ahead of midpoint
            tip_x = mx + arrow_len * dx
            tip_y = my + arrow_len * dy
            # Two tail points (perpendicular, behind midpoint)
            tail_x = mx - arrow_len * 0.5 * dx
            tail_y = my - arrow_len * 0.5 * dy
            px, py = -dy, dx  # perpendicular
            hw = arrow_len * 0.5
            return [
                [tail_x + hw * px, tail_y + hw * py],
                [tip_x, tip_y],
                [tail_x - hw * px, tail_y - hw * py],
            ]
    return None


def _get_center_zoom(gdf_wgs):
    """Get center [lon, lat] and zoom from a WGS84 GeoDataFrame."""
    bounds = gdf_wgs.total_bounds  # [minx, miny, maxx, maxy]
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2

    # Estimate zoom from extent
    lon_range = bounds[2] - bounds[0]
    lat_range = bounds[3] - bounds[1]
    extent = max(lon_range, lat_range)
    if extent < 0.005:
        zoom = 17
    elif extent < 0.01:
        zoom = 16
    elif extent < 0.05:
        zoom = 14
    elif extent < 0.1:
        zoom = 13
    elif extent < 0.5:
        zoom = 11
    else:
        zoom = 9

    return center_lon, center_lat, zoom


def build_pydeck_map(pipes_gdf=None, junctions_gdf=None, pumps_gdf=None, storage_gdf=None,
                     issues=None, network_result=None, selected_ids=None,
                     visible_layers=None, fixed_issues=None):
    """
    Build a pydeck Deck with all network layers and issue overlays.

    Returns a pdk.Deck object for use with st.pydeck_chart.
    """
    layers = []
    center_lon, center_lat, zoom = -98.58, 39.83, 4  # US center fallback

    vis = visible_layers or {}
    selected_ids = selected_ids or set()
    issues = issues or []
    fixed_issues = fixed_issues or []

    # ── Pipes layer ──
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        center_lon, center_lat, zoom = _get_center_zoom(pipes_wgs)

        if vis.get("Pipes", True):
            # Simplify geometry for performance
            pipes_simple = pipes_wgs.copy()
            pipes_simple["geometry"] = pipes_simple.geometry.simplify(0.00001, preserve_topology=True)

            id_col = pipes_simple.columns[0]
            pipe_paths = []
            for idx, row in pipes_simple.iterrows():
                path = _extract_path_coords(row.geometry)
                if path and len(path) >= 2:
                    pid = str(row.get(id_col, idx))
                    is_selected = pid in selected_ids
                    pipe_paths.append({
                        "path": path,
                        "name": pid,
                        "color": [0, 255, 255, 230] if is_selected else [74, 144, 217, 180],
                        "width": 8 if is_selected else 3,
                    })

            if pipe_paths:
                layers.append(pdk.Layer(
                    "PathLayer",
                    data=pipe_paths,
                    get_path="path",
                    get_color="color",
                    get_width="width",
                    width_min_pixels=2,
                    width_max_pixels=12,
                    pickable=True,
                    auto_highlight=True,
                    highlight_color=[0, 255, 255, 100],
                ))

            # ── Flow direction arrows at pipe midpoints ──
            if vis.get("Flow Arrows", True):
                arrow_data = []
                for p in pipe_paths:
                    arrow = _midpoint_arrow(p["path"])
                    if arrow:
                        arrow_data.append({
                            "path": arrow,
                            "name": p["name"],
                            "color": [58, 120, 181, 220],
                        })
                if arrow_data:
                    layers.append(pdk.Layer(
                        "PathLayer",
                        data=arrow_data,
                        get_path="path",
                        get_color="color",
                        get_width=2,
                        width_min_pixels=2,
                        width_max_pixels=6,
                        pickable=False,
                    ))

    # ── Junctions layer ──
    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        if pipes_gdf is None or len(pipes_gdf) == 0:
            center_lon, center_lat, zoom = _get_center_zoom(juncs_wgs)

        if vis.get("Junctions", True):
            id_col = juncs_wgs.columns[0]
            junc_points = []
            for idx, row in juncs_wgs.iterrows():
                geom = row.geometry
                if geom is None or geom.is_empty:
                    continue
                jid = str(row.get(id_col, idx))
                is_selected = jid in selected_ids
                junc_points.append({
                    "position": [float(geom.x), float(geom.y)],
                    "name": jid,
                    "color": [0, 255, 255, 230] if is_selected else [46, 139, 87, 200],
                    "radius": 12 if is_selected else 6,
                })

            if junc_points:
                layers.append(pdk.Layer(
                    "ScatterplotLayer",
                    data=junc_points,
                    get_position="position",
                    get_fill_color="color",
                    get_radius="radius",
                    radius_min_pixels=3,
                    radius_max_pixels=14,
                    pickable=True,
                    auto_highlight=True,
                    highlight_color=[0, 255, 255, 100],
                ))

    # ── Pumps layer ──
    if pumps_gdf is not None and len(pumps_gdf) > 0 and vis.get("Pumps", True):
        pumps_wgs = _ensure_wgs84(pumps_gdf)
        id_col = pumps_wgs.columns[0]
        pump_points = []
        for idx, row in pumps_wgs.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            pump_points.append({
                "position": [float(geom.x), float(geom.y)],
                "name": str(row.get(id_col, idx)),
                "color": [220, 50, 50, 220],
                "radius": 10,
            })
        if pump_points:
            layers.append(pdk.Layer(
                "ScatterplotLayer",
                data=pump_points,
                get_position="position",
                get_fill_color="color",
                get_radius="radius",
                radius_min_pixels=5,
                radius_max_pixels=16,
                pickable=True,
            ))

    # ── Storage layer ──
    if storage_gdf is not None and len(storage_gdf) > 0 and vis.get("Storage", True):
        stor_wgs = _ensure_wgs84(storage_gdf)
        id_col = stor_wgs.columns[0]
        stor_points = []
        for idx, row in stor_wgs.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            stor_points.append({
                "position": [float(geom.x), float(geom.y)],
                "name": str(row.get(id_col, idx)),
                "color": [30, 100, 200, 220],
                "radius": 10,
            })
        if stor_points:
            layers.append(pdk.Layer(
                "ScatterplotLayer",
                data=stor_points,
                get_position="position",
                get_fill_color="color",
                get_radius="radius",
                radius_min_pixels=5,
                radius_max_pixels=16,
                pickable=True,
            ))

    # ── Issue pipe highlights ──
    _add_issue_layers(layers, issues, pipes_gdf, junctions_gdf, network_result, vis)

    # ── Resolved issue highlights (green) ──
    if fixed_issues:
        _add_resolved_layers(layers, fixed_issues, pipes_gdf, junctions_gdf, network_result, vis)

    # ── Build deck ──
    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=zoom,
        pitch=0,
        bearing=0,
    )

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_style="light",
        tooltip={"text": "{name}"},
    )

    return deck


def _add_issue_layers(layers, issues, pipes_gdf, junctions_gdf, network_result, vis):
    """Add issue overlay layers (pipe highlights + node markers)."""
    if not issues:
        return

    # Build lookups
    pipe_geoms = {}
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        id_col = pipes_wgs.columns[0]
        for idx, row in pipes_wgs.iterrows():
            pid = str(row.get(id_col, idx))
            if row.geometry and not row.geometry.is_empty:
                pipe_geoms[pid] = row.geometry

    node_coords = {}
    if network_result and "graph" in network_result:
        G = network_result["graph"]
        for nid in G.nodes:
            attrs = G.nodes[nid] if hasattr(G, 'degree') else G._nodes[nid]
            coords = attrs.get("coords")
            if coords:
                node_coords[str(nid)] = coords

    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        id_col = juncs_wgs.columns[0]
        for idx, row in juncs_wgs.iterrows():
            if row.geometry and not row.geometry.is_empty:
                fid = str(row.get(id_col, idx))
                node_coords[fid] = (float(row.geometry.x), float(row.geometry.y))

    # Group issues by type
    pipe_issue_paths = []
    node_issue_points = []

    for issue in issues:
        itype = issue.issue_type
        display = ISSUE_DISPLAY_NAMES.get(itype, itype)
        if not vis.get(display, True):
            continue

        color = ISSUE_COLORS_RGB.get(itype, [255, 0, 0])

        if itype in PIPE_ISSUE_TYPES:
            pid = str(issue.feature_id)
            if pid in pipe_geoms:
                path = _extract_path_coords(pipe_geoms[pid])
                if path and len(path) >= 2:
                    pipe_issue_paths.append({
                        "path": path,
                        "name": f"{display}: {pid}",
                        "color": color + [220],
                    })
        else:
            node_id = str(issue.feature_id)
            details = issue.details or {}
            placed = False
            for nid in [node_id] + [str(details.get(k, "")) for k in ("us_node", "ds_node", "junction_id")]:
                if nid in node_coords:
                    c = node_coords[nid]
                    node_issue_points.append({
                        "position": [c[0], c[1]],
                        "name": f"{display}: {nid}",
                        "color": color + [200],
                        "radius": 14,
                    })
                    placed = True
                    break
            if not placed and issue.coordinates:
                node_issue_points.append({
                    "position": [issue.coordinates[0], issue.coordinates[1]],
                    "name": f"{display}: {node_id}",
                    "color": color + [200],
                    "radius": 14,
                })

    if pipe_issue_paths:
        layers.append(pdk.Layer(
            "PathLayer",
            data=pipe_issue_paths,
            get_path="path",
            get_color="color",
            get_width=7,
            width_min_pixels=4,
            width_max_pixels=14,
            pickable=True,
        ))

    if node_issue_points:
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=node_issue_points,
            get_position="position",
            get_fill_color="color",
            get_radius="radius",
            radius_min_pixels=6,
            radius_max_pixels=18,
            pickable=True,
            stroked=True,
            get_line_color=[0, 0, 0, 150],
            line_width_min_pixels=2,
        ))


def _add_resolved_layers(layers, issues, pipes_gdf, junctions_gdf, network_result, vis):
    """Add resolved issue overlay (green muted)."""
    if not vis.get("Resolved Issues", True):
        return

    pipe_geoms = {}
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        id_col = pipes_wgs.columns[0]
        for idx, row in pipes_wgs.iterrows():
            pid = str(row.get(id_col, idx))
            if row.geometry and not row.geometry.is_empty:
                pipe_geoms[pid] = row.geometry

    node_coords = {}
    if network_result and "graph" in network_result:
        G = network_result["graph"]
        for nid in G.nodes:
            attrs = G.nodes[nid] if hasattr(G, 'degree') else G._nodes[nid]
            coords = attrs.get("coords")
            if coords:
                node_coords[str(nid)] = coords

    resolved_paths = []
    resolved_points = []

    for issue in issues:
        itype = issue.issue_type
        if itype in PIPE_ISSUE_TYPES:
            pid = str(issue.feature_id)
            if pid in pipe_geoms:
                path = _extract_path_coords(pipe_geoms[pid])
                if path:
                    resolved_paths.append({
                        "path": path,
                        "name": f"Resolved: {pid}",
                        "color": [76, 175, 80, 150],
                    })
        else:
            node_id = str(issue.feature_id)
            if node_id in node_coords:
                c = node_coords[node_id]
                resolved_points.append({
                    "position": [c[0], c[1]],
                    "name": f"Resolved: {node_id}",
                    "color": [76, 175, 80, 120],
                    "radius": 12,
                })

    if resolved_paths:
        layers.append(pdk.Layer(
            "PathLayer",
            data=resolved_paths,
            get_path="path",
            get_color="color",
            get_width=5,
            width_min_pixels=3,
            pickable=True,
        ))
    if resolved_points:
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=resolved_points,
            get_position="position",
            get_fill_color="color",
            get_radius="radius",
            radius_min_pixels=5,
            pickable=True,
        ))


def get_feature_bounds(feature_ids, pipes_gdf=None, junctions_gdf=None, network_result=None):
    """
    Given a list of feature IDs, return [[lat_min, lon_min], [lat_max, lon_max]]
    bounding box in WGS84.
    """
    lats, lons = [], []

    node_coords = {}
    if network_result and "graph" in network_result:
        G = network_result["graph"]
        for nid in G.nodes:
            attrs = G.nodes[nid] if hasattr(G, 'degree') else G._nodes[nid]
            coords = attrs.get("coords")
            if coords:
                node_coords[str(nid)] = coords

    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        id_col = juncs_wgs.columns[0]
        for idx, row in juncs_wgs.iterrows():
            if row.geometry and not row.geometry.is_empty:
                fid = str(row.get(id_col, idx))
                node_coords[fid] = (float(row.geometry.x), float(row.geometry.y))

    pipe_geoms = {}
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        id_col = pipes_wgs.columns[0]
        for idx, row in pipes_wgs.iterrows():
            pid = str(row.get(id_col, idx))
            if row.geometry and not row.geometry.is_empty:
                pipe_geoms[pid] = row.geometry

    for fid in feature_ids:
        fid_str = str(fid)
        if fid_str in pipe_geoms:
            path = _extract_path_coords(pipe_geoms[fid_str])
            if path:
                for lon, lat in path:
                    lats.append(lat)
                    lons.append(lon)
                continue
        if fid_str in node_coords:
            c = node_coords[fid_str]
            lons.append(c[0])
            lats.append(c[1])

    if not lats or not lons:
        return None

    lat_pad = max((max(lats) - min(lats)) * 0.15, 0.001)
    lon_pad = max((max(lons) - min(lons)) * 0.15, 0.001)

    return [
        [min(lats) - lat_pad, min(lons) - lon_pad],
        [max(lats) + lat_pad, max(lons) + lon_pad],
    ]


def render_issues_summary_html(issues):
    """Build HTML for a compact issues-by-type summary."""
    if not issues:
        return "<p style='color:#888;'>No issues found.</p>"

    counts = {}
    for i in issues:
        counts[i.issue_type] = counts.get(i.issue_type, 0) + 1

    rows = ""
    for itype, count in sorted(counts.items(), key=lambda x: -x[1]):
        color = ISSUE_COLORS.get(itype, "#999")
        label = ISSUE_DISPLAY_NAMES.get(itype, itype.replace("_", " ").title())
        rows += (
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:3px 0;border-bottom:1px solid #eee;font-size:13px;">'
            f'<span><span style="color:{color};font-size:16px;">●</span> {label}</span>'
            f'<span style="font-weight:600;">{count}</span></div>'
        )

    return f'<div style="margin:4px 0;">{rows}</div>'
