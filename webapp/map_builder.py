"""
map_builder.py — Build interactive Folium maps with color-coded issue overlays.

Creates a Leaflet-based map showing:
- Base network (pipes as lines, junctions as circles)
- Color-coded issue markers with popup details
- Profile trace highlighting
"""

import folium
from folium.plugins import MarkerCluster
import geopandas as gpd
import json
import math


def _line_midpoint_and_bearing(coords):
    """
    Find the midpoint along a polyline and the bearing at that point.

    Parameters
    ----------
    coords : list of (lat, lon) tuples

    Returns
    -------
    (mid_lat, mid_lon, bearing_deg) or None
    """
    if not coords or len(coords) < 2:
        return None

    # Calculate cumulative distances along the line
    dists = [0.0]
    for i in range(1, len(coords)):
        dlat = coords[i][0] - coords[i - 1][0]
        dlon = coords[i][1] - coords[i - 1][1]
        d = math.sqrt(dlat ** 2 + dlon ** 2)
        dists.append(dists[-1] + d)

    total = dists[-1]
    if total == 0:
        return None

    half = total / 2.0

    # Find the segment that contains the midpoint
    for i in range(1, len(dists)):
        if dists[i] >= half:
            # Interpolate within this segment
            seg_len = dists[i] - dists[i - 1]
            if seg_len == 0:
                frac = 0
            else:
                frac = (half - dists[i - 1]) / seg_len
            mid_lat = coords[i - 1][0] + frac * (coords[i][0] - coords[i - 1][0])
            mid_lon = coords[i - 1][1] + frac * (coords[i][1] - coords[i - 1][1])
            # Bearing from segment start to end
            dy = coords[i][0] - coords[i - 1][0]
            dx = coords[i][1] - coords[i - 1][1]
            bearing = math.degrees(math.atan2(dx, dy))  # 0=north, 90=east
            return (mid_lat, mid_lon, bearing)

    return None


def _arrow_triangle(lat, lon, bearing_deg, size):
    """
    Create triangle polygon vertices for a flow-direction arrow.

    Parameters
    ----------
    lat, lon : float
        Center of the arrow (midpoint of pipe).
    bearing_deg : float
        Direction the arrow points (0=north, 90=east).
    size : float
        Half-length of the arrow in degrees.

    Returns
    -------
    list of [lat, lon] for a triangle polygon.
    """
    rad = math.radians(bearing_deg)
    # Tip of arrow (pointing in flow direction)
    tip_lat = lat + size * math.cos(rad)
    tip_lon = lon + size * math.sin(rad)
    # Two base points (perpendicular to bearing, behind center)
    back_rad = math.radians(bearing_deg + 180)
    perp_rad = math.radians(bearing_deg + 90)
    base_lat = lat + size * 0.6 * math.cos(back_rad)
    base_lon = lon + size * 0.6 * math.sin(back_rad)
    half_w = size * 0.5
    left_lat = base_lat + half_w * math.cos(perp_rad)
    left_lon = base_lon + half_w * math.sin(perp_rad)
    right_lat = base_lat - half_w * math.cos(perp_rad)
    right_lon = base_lon - half_w * math.sin(perp_rad)
    return [[tip_lat, tip_lon], [left_lat, left_lon], [right_lat, right_lon]]


def _extract_line_coords(geom):
    """Extract a list of (lat, lon) tuples from any line geometry for Folium."""
    if geom is None or geom.is_empty:
        return None
    gt = geom.geom_type
    if gt == "LineString":
        return [(c[1], c[0]) for c in geom.coords]
    if gt == "MultiLineString":
        coords = []
        for part in geom.geoms:
            coords.extend((c[1], c[0]) for c in part.coords)
        return coords
    # fallback — try centroid
    return None


def _extract_point_latlon(geom):
    """Extract (lat, lon) from any point-like geometry for Folium."""
    if geom is None or geom.is_empty:
        return None
    gt = geom.geom_type
    if gt == "Point":
        return (geom.y, geom.x)
    if gt == "MultiPoint":
        pts = list(geom.geoms)
        if pts:
            return (pts[0].y, pts[0].x)
        return None
    # fallback — centroid
    c = geom.centroid
    if c and not c.is_empty:
        return (c.y, c.x)
    return None


# Issue type -> color mapping
ISSUE_COLORS = {
    "ADVERSE_SLOPE": "#FF0000",        # Red
    "INVERT_MISMATCH": "#FF8C00",      # Dark Orange
    "DIAMETER_DECREASE": "#FFD700",     # Gold
    "NULL_DIAMETER": "#9400D3",         # Purple
    "NULL_INVERT": "#9400D3",           # Purple
    "DEAD_END": "#1E90FF",             # Dodger Blue
    "ORPHAN_NODE": "#808080",           # Gray
    "SHALLOW_STRUCTURE": "#00CED1",     # Dark Turquoise
    "DEEP_STRUCTURE": "#8B4513",        # Saddle Brown
}

SEVERITY_ICONS = {
    "HIGH": "exclamation-circle",
    "MEDIUM": "exclamation-triangle",
    "LOW": "info-circle",
}


def _ensure_wgs84(gdf):
    """Reproject a GeoDataFrame to WGS84 (EPSG:4326) for Folium."""
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def _get_centroid(gdf):
    """Get the centroid of all features for initial map center."""
    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2
    return [center_lat, center_lon]


def build_base_map(pipes_gdf=None, junctions_gdf=None, pumps_gdf=None, storage_gdf=None,
                   visible_layers=None, zoom_bounds=None, network_result=None):
    """
    Build a base Folium map showing the sewer network.

    Parameters
    ----------
    visible_layers : dict, optional
        Maps layer name to bool. Controls initial show state.
    zoom_bounds : list, optional
        [[lat_min, lon_min], [lat_max, lon_max]] to fit the map view.
    network_result : dict, optional
        Network build result containing 'graph'. Used to orient pipe
        arrows from upstream to downstream.

    Returns a folium.Map object.
    """
    # Find center from available data
    center = [39.8283, -98.5795]  # US center fallback
    zoom = 4

    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        center = _get_centroid(pipes_wgs)
        zoom = 15
    elif junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        center = _get_centroid(juncs_wgs)
        zoom = 15

    m = folium.Map(
        location=center,
        zoom_start=zoom,
        tiles="CartoDB positron",
        control_scale=True,
    )

    # Add tile layer options
    folium.TileLayer("OpenStreetMap", name="Street Map").add_to(m)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Satellite",
    ).add_to(m)

    # Add pipes layer as a single GeoJSON for fast rendering
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        _show = visible_layers.get("Pipes", True) if visible_layers else True

        # Simplify geometry to reduce vertex count (tolerance in degrees ≈ 1m)
        pipes_simple = pipes_wgs.copy()
        pipes_simple["geometry"] = pipes_simple.geometry.simplify(0.00001, preserve_topology=True)

        # Build GeoJSON with tooltip field
        id_col = pipes_simple.columns[0]
        pipes_simple["_tooltip"] = pipes_simple[id_col].astype(str)
        geojson_data = json.loads(pipes_simple[["_tooltip", "geometry"]].to_json())

        pipes_layer = folium.FeatureGroup(name="Pipes", show=_show)
        folium.GeoJson(
            geojson_data,
            style_function=lambda f: {
                "color": "#4A90D9",
                "weight": 3,
                "opacity": 0.7,
            },
            tooltip=folium.GeoJsonTooltip(fields=["_tooltip"], aliases=[""], labels=False),
        ).add_to(pipes_layer)
        pipes_layer.add_to(m)

    # Add junctions layer as a single GeoJSON for fast rendering
    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        _show = visible_layers.get("Junctions", True) if visible_layers else True

        id_col = juncs_wgs.columns[0]
        juncs_wgs = juncs_wgs.copy()
        juncs_wgs["_tooltip"] = juncs_wgs[id_col].astype(str)
        geojson_data = json.loads(juncs_wgs[["_tooltip", "geometry"]].to_json())

        juncs_layer = folium.FeatureGroup(name="Junctions", show=_show)
        folium.GeoJson(
            geojson_data,
            style_function=lambda f: {
                "color": "#2E8B57",
                "fillColor": "#2E8B57",
                "fillOpacity": 0.8,
                "weight": 1,
                "radius": 5,
            },
            marker=folium.CircleMarker(radius=5, fill=True, fill_color="#2E8B57",
                                        fill_opacity=0.8, color="#2E8B57", weight=1),
            tooltip=folium.GeoJsonTooltip(fields=["_tooltip"], aliases=[""], labels=False),
        ).add_to(juncs_layer)
        juncs_layer.add_to(m)

    # Add pumps layer
    if pumps_gdf is not None and len(pumps_gdf) > 0:
        pumps_wgs = _ensure_wgs84(pumps_gdf)
        _show = visible_layers.get("Pumps", True) if visible_layers else True
        pumps_layer = folium.FeatureGroup(name="Pumps", show=_show)
        for idx, row in pumps_wgs.iterrows():
            latlon = _extract_point_latlon(row.geometry)
            if latlon:
                folium.Marker(
                    location=list(latlon),
                    icon=folium.Icon(color="red", icon="bolt", prefix="fa"),
                    tooltip=f"Pump: {row.get(pumps_wgs.columns[0], idx)}",
                ).add_to(pumps_layer)
        pumps_layer.add_to(m)

    # Add storage layer
    if storage_gdf is not None and len(storage_gdf) > 0:
        stor_wgs = _ensure_wgs84(storage_gdf)
        _show = visible_layers.get("Storage", True) if visible_layers else True
        stor_layer = folium.FeatureGroup(name="Storage", show=_show)
        for idx, row in stor_wgs.iterrows():
            latlon = _extract_point_latlon(row.geometry)
            if latlon:
                folium.Marker(
                    location=list(latlon),
                    icon=folium.Icon(color="blue", icon="database", prefix="fa"),
                    tooltip=f"Storage: {row.get(stor_wgs.columns[0], idx)}",
                ).add_to(stor_layer)
        stor_layer.add_to(m)

    # Apply zoom bounds if specified (from "Zoom to Selection")
    # Bounds already include ~15% buffer — no extra padding needed
    if zoom_bounds:
        m.fit_bounds(zoom_bounds, max_zoom=19)

    # Don't add LayerControl here — add it after issues are overlaid
    return m


# Display names for issue types in layer control
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


def add_issues_to_map(m, issues, pipes_gdf=None, junctions_gdf=None, network_result=None,
                      visible_layers=None, add_layer_control=True):
    """
    Add color-coded issue markers/highlights to an existing map.

    Each issue type gets its own toggleable layer in the layer control.
    Pipe issues: bold highlighted lines with dark border outline.
    Node issues: large colored circle markers.

    Parameters
    ----------
    visible_layers : dict, optional
        Maps layer name to bool visibility. If provided, layers not in
        the dict or set to False are hidden (show=False in FeatureGroup).
    add_layer_control : bool
        Whether to add LayerControl at the end (default True).
        Set False when adding more layers (e.g. selection) before the control.
    """
    if not issues:
        if add_layer_control:
            folium.LayerControl(collapsed=False).add_to(m)
        return m

    # ── Build coordinate lookups ──
    node_coords = {}
    if network_result and "graph" in network_result:
        G = network_result["graph"]
        for nid in G.nodes:
            attrs = G.nodes[nid] if hasattr(G, 'degree') else G._nodes[nid]
            coords = attrs.get("coords")
            if coords:
                node_coords[nid] = coords

    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        for idx, row in juncs_wgs.iterrows():
            latlon = _extract_point_latlon(row.geometry)
            if latlon:
                fid = str(row.get(juncs_wgs.columns[0], idx))
                node_coords[fid] = (latlon[1], latlon[0])

    pipe_geoms = {}
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        for idx, row in pipes_wgs.iterrows():
            pid = str(row.get(pipes_wgs.columns[0], idx))
            if row.geometry and not row.geometry.is_empty:
                pipe_geoms[pid] = row.geometry

    # ── Group issues by type ──
    issues_by_type = {}
    for issue in issues:
        itype = issue.issue_type
        if itype not in issues_by_type:
            issues_by_type[itype] = []
        issues_by_type[itype].append(issue)

    # ── Create a separate FeatureGroup per issue type ──
    for itype, type_issues in issues_by_type.items():
        color = ISSUE_COLORS.get(itype, "#FF0000")
        display_name = ISSUE_DISPLAY_NAMES.get(itype, itype)
        count = len(type_issues)
        # Determine visibility from the sidebar controls
        is_visible = True
        if visible_layers is not None:
            is_visible = visible_layers.get(display_name, True)

        layer = folium.FeatureGroup(
            name=f'<span style="color:{color};font-weight:bold;">&#9632;</span> {display_name} ({count})',
            show=is_visible,
        )

        for issue in type_issues:
            # ── Pipe issues: bold line with dark outline ──
            if itype in PIPE_ISSUE_TYPES:
                pid = str(issue.feature_id)
                if pid in pipe_geoms:
                    coords = _extract_line_coords(pipe_geoms[pid])
                    if not coords:
                        continue
                    # Dark border line (non-interactive, visual only)
                    border = folium.PolyLine(
                        coords, color="#000000", weight=10, opacity=0.6,
                    )
                    border.options["interactive"] = False
                    border.add_to(layer)
                    # Bright colored line on top
                    highlight = folium.PolyLine(
                        coords, color=color, weight=7, opacity=1.0,
                    )
                    highlight.options["interactive"] = False
                    highlight.add_to(layer)
                    continue

            # ── Node issues: large markers ──
            node_id = str(issue.feature_id)
            details = issue.details or {}
            check_nodes = [node_id]
            for key in ("us_node", "ds_node", "junction_id"):
                if key in details:
                    check_nodes.append(str(details[key]))

            placed = False
            for nid in check_nodes:
                if nid in node_coords:
                    c = node_coords[nid]
                    lat, lon = c[1], c[0]
                    # Outer glow ring (non-interactive)
                    glow = folium.CircleMarker(
                        location=[lat, lon], radius=16,
                        color=color, fill=True, fill_color=color,
                        fill_opacity=0.25, weight=0,
                    )
                    glow.options["interactive"] = False
                    glow.add_to(layer)
                    # Inner solid marker (non-interactive)
                    inner = folium.CircleMarker(
                        location=[lat, lon], radius=9,
                        color="#000000", weight=2, fill=True,
                        fill_color=color, fill_opacity=0.9,
                    )
                    inner.options["interactive"] = False
                    inner.add_to(layer)
                    placed = True
                    break

            if not placed and issue.coordinates:
                lat, lon = issue.coordinates[1], issue.coordinates[0]
                glow = folium.CircleMarker(
                    location=[lat, lon], radius=16,
                    color=color, fill=True, fill_color=color,
                    fill_opacity=0.25, weight=0,
                )
                glow.options["interactive"] = False
                glow.add_to(layer)
                inner = folium.CircleMarker(
                    location=[lat, lon], radius=9,
                    color="#000000", weight=2, fill=True,
                    fill_color=color, fill_opacity=0.9,
                )
                inner.options["interactive"] = False
                inner.add_to(layer)

        layer.add_to(m)

    if add_layer_control:
        folium.LayerControl(collapsed=False).add_to(m)

    return m


def add_resolved_issues_to_map(m, issues, pipes_gdf=None, junctions_gdf=None,
                                network_result=None, visible=True):
    """
    Add a 'Resolved Issues' layer with muted green styling for issues
    that have been addressed via the edit ledger.

    All elements are non-interactive (visual only).
    """
    if not issues:
        return m

    RESOLVED_COLOR = "#4CAF50"  # muted green

    # ── Build coordinate lookups (same as add_issues_to_map) ──
    node_coords = {}
    if network_result and "graph" in network_result:
        G = network_result["graph"]
        for nid in G.nodes:
            attrs = G.nodes[nid] if hasattr(G, 'degree') else G._nodes[nid]
            coords = attrs.get("coords")
            if coords:
                node_coords[nid] = coords

    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        for idx, row in juncs_wgs.iterrows():
            latlon = _extract_point_latlon(row.geometry)
            if latlon:
                fid = str(row.get(juncs_wgs.columns[0], idx))
                node_coords[fid] = (latlon[1], latlon[0])

    pipe_geoms = {}
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        for idx, row in pipes_wgs.iterrows():
            pid = str(row.get(pipes_wgs.columns[0], idx))
            if row.geometry and not row.geometry.is_empty:
                pipe_geoms[pid] = row.geometry

    count = len(issues)
    layer = folium.FeatureGroup(
        name=f'<span style="color:{RESOLVED_COLOR};font-weight:bold;">&#10004;</span> Resolved Issues ({count})',
        show=visible,
    )

    for issue in issues:
        itype = issue.issue_type
        # ── Pipe issues: dashed green line ──
        if itype in PIPE_ISSUE_TYPES:
            pid = str(issue.feature_id)
            if pid in pipe_geoms:
                coords = _extract_line_coords(pipe_geoms[pid])
                if not coords:
                    continue
                line = folium.PolyLine(
                    coords, color=RESOLVED_COLOR, weight=5, opacity=0.6,
                    dash_array="8 6",
                )
                line.options["interactive"] = False
                line.add_to(layer)
                continue

        # ── Node issues: green ring marker ──
        node_id = str(issue.feature_id)
        details = issue.details or {}
        check_nodes = [node_id]
        for key in ("us_node", "ds_node", "junction_id"):
            if key in details:
                check_nodes.append(str(details[key]))

        placed = False
        for nid in check_nodes:
            if nid in node_coords:
                c = node_coords[nid]
                lat, lon = c[1], c[0]
                marker = folium.CircleMarker(
                    location=[lat, lon], radius=12,
                    color=RESOLVED_COLOR, weight=2, fill=True,
                    fill_color=RESOLVED_COLOR, fill_opacity=0.2,
                )
                marker.options["interactive"] = False
                marker.add_to(layer)
                placed = True
                break

        if not placed and issue.coordinates:
            lat, lon = issue.coordinates[1], issue.coordinates[0]
            marker = folium.CircleMarker(
                location=[lat, lon], radius=12,
                color=RESOLVED_COLOR, weight=2, fill=True,
                fill_color=RESOLVED_COLOR, fill_opacity=0.2,
            )
            marker.options["interactive"] = False
            marker.add_to(layer)

    layer.add_to(m)
    return m


def get_feature_bounds(feature_ids, pipes_gdf=None, junctions_gdf=None, network_result=None):
    """
    Given a list of feature IDs, return [[lat_min, lon_min], [lat_max, lon_max]]
    bounding box in WGS84 for use with Folium fit_bounds.
    Returns None if no coordinates can be resolved.
    """
    lats, lons = [], []

    # Build node coord lookup from graph
    node_coords = {}
    if network_result and "graph" in network_result:
        G = network_result["graph"]
        for nid in G.nodes:
            attrs = G.nodes[nid] if hasattr(G, 'degree') else G._nodes[nid]
            coords = attrs.get("coords")
            if coords:
                node_coords[str(nid)] = coords  # (x, y) = (lon, lat)

    # Junction GeoDataFrame lookup
    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        for idx, row in juncs_wgs.iterrows():
            latlon = _extract_point_latlon(row.geometry)
            if latlon:
                fid = str(row.get(juncs_wgs.columns[0], idx))
                node_coords[fid] = (latlon[1], latlon[0])  # store as (lon, lat)

    # Pipe GeoDataFrame lookup
    pipe_geoms = {}
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        for idx, row in pipes_wgs.iterrows():
            pid = str(row.get(pipes_wgs.columns[0], idx))
            if row.geometry and not row.geometry.is_empty:
                pipe_geoms[pid] = row.geometry

    for fid in feature_ids:
        fid_str = str(fid)
        # Try pipe geometry (use centroid)
        if fid_str in pipe_geoms:
            coords = _extract_line_coords(pipe_geoms[fid_str])
            if coords:
                for lat, lon in coords:
                    lats.append(lat)
                    lons.append(lon)
                continue
        # Try node coords
        if fid_str in node_coords:
            c = node_coords[fid_str]
            lats.append(c[1])  # lat
            lons.append(c[0])  # lon

    if not lats or not lons:
        return None

    # Add padding so single points aren't at max zoom
    lat_pad = max((max(lats) - min(lats)) * 0.15, 0.001)
    lon_pad = max((max(lons) - min(lons)) * 0.15, 0.001)

    return [
        [min(lats) - lat_pad, min(lons) - lon_pad],
        [max(lats) + lat_pad, max(lons) + lon_pad],
    ]


def add_selection_layer(m, selected_ids, pipes_gdf=None, junctions_gdf=None):
    """
    Add cyan highlight layer for selected features (ArcGIS Pro-style selection).

    Parameters
    ----------
    m : folium.Map
    selected_ids : set of str
        Feature IDs currently in the selection set.
    pipes_gdf, junctions_gdf : GeoDataFrame, optional
    """
    if not selected_ids:
        return m

    sel_layer = folium.FeatureGroup(name="Selection", show=True)
    has_features = False

    # Highlight selected pipes with cyan
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf)
        id_col = pipes_wgs.columns[0]
        for idx, row in pipes_wgs.iterrows():
            fid = str(row.get(id_col, idx))
            if fid in selected_ids:
                coords = _extract_line_coords(row.geometry)
                if coords:
                    # Dark outline underneath (non-interactive so clicks pass through)
                    outline = folium.PolyLine(
                        coords, color="#000000", weight=12, opacity=0.5,
                    )
                    outline.options["interactive"] = False
                    outline.add_to(sel_layer)
                    # Cyan highlight on top
                    line = folium.PolyLine(
                        coords, color="#00FFFF", weight=8, opacity=0.9,
                    )
                    line.options["interactive"] = False
                    line.add_to(sel_layer)
                    has_features = True

    # Highlight selected junctions with cyan halo
    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf)
        id_col = juncs_wgs.columns[0]
        for idx, row in juncs_wgs.iterrows():
            fid = str(row.get(id_col, idx))
            if fid in selected_ids:
                latlon = _extract_point_latlon(row.geometry)
                if latlon:
                    # Outer glow (non-interactive so clicks pass through)
                    glow = folium.CircleMarker(
                        location=list(latlon), radius=16,
                        color="#00FFFF", weight=0,
                        fill=True, fill_color="#00FFFF", fill_opacity=0.3,
                    )
                    glow.options["interactive"] = False
                    glow.add_to(sel_layer)
                    # Inner ring
                    ring = folium.CircleMarker(
                        location=list(latlon), radius=9,
                        color="#00FFFF", weight=3,
                        fill=True, fill_color="#00FFFF", fill_opacity=0.6,
                    )
                    ring.options["interactive"] = False
                    ring.add_to(sel_layer)
                    has_features = True

    if has_features:
        sel_layer.add_to(m)

    return m


def build_legend_html():
    """Build an HTML legend for issue colors."""
    rows = ""
    for issue_type, color in ISSUE_COLORS.items():
        label = issue_type.replace("_", " ").title()
        rows += f"""
        <tr>
            <td><span style="display:inline-block;width:14px;height:14px;
                background:{color};border-radius:50%;margin-right:6px"></span></td>
            <td style="padding:2px 8px;font-size:13px">{label}</td>
        </tr>"""

    return f"""
    <div style="background:white;padding:12px 16px;border-radius:8px;
         box-shadow:0 2px 6px rgba(0,0,0,0.15);max-width:220px">
        <h4 style="margin:0 0 8px 0;font-size:14px">Issue Types</h4>
        <table>{rows}</table>
    </div>
    """
