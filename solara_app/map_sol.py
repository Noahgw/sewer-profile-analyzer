"""
map_sol.py — Interactive ipyleaflet map builder for Solara.

Uses ipyleaflet (Leaflet.js via ipywidgets) for map rendering.
Map view (center/zoom) is preserved across re-renders.
Supports box-select for multi-feature selection.
"""

import solara
import ipyleaflet
import geopandas as gpd
import json
import math
from pyproj import Transformer
from shapely.geometry import mapping as shapely_mapping, box as shapely_box

# ── Monkey-patch pyproj enums for Python 3.9 ──
# In Python 3.9, str(EnumMember) returns "ClassName.VALUE" instead of "VALUE",
# which breaks pyproj's internal enum lookups.
import pyproj.enums as _pyproj_enums
if not hasattr(_pyproj_enums.BaseEnum, '_patched'):
    _pyproj_enums.BaseEnum.__str__ = lambda self: self.value
    _pyproj_enums.BaseEnum._patched = True

# Issue type -> hex color mapping
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


def _guess_crs_from_bounds(bounds):
    """Guess EPSG code from coordinate bounds when CRS is missing."""
    minx, miny, maxx, maxy = bounds
    if abs(minx) <= 360 and abs(miny) <= 180:
        return "EPSG:4326"
    if 100_000 < minx < 900_000 and 0 < miny < 10_000_000:
        if 5_300_000 < miny < 5_500_000:
            return "EPSG:26910" if minx < 500_000 else "EPSG:26911"
        elif 4_800_000 < miny < 5_300_000:
            return "EPSG:26910" if minx < 500_000 else "EPSG:26911"
        elif 3_500_000 < miny < 4_800_000:
            return "EPSG:26914"
    if minx > 1_000_000 and miny > 100_000:
        return None
    return None


def _reproject_with_transformer(gdf, src_crs):
    """Reproject using pyproj Transformer + shapely.ops.transform."""
    from shapely.ops import transform as shapely_transform
    src_str = str(src_crs) if not isinstance(src_crs, str) else src_crs
    transformer = Transformer.from_crs(src_str, "EPSG:4326", always_xy=True)
    new_geoms = []
    for geom in gdf.geometry.tolist():
        if geom is None or geom.is_empty:
            new_geoms.append(geom)
        else:
            new_geoms.append(shapely_transform(transformer.transform, geom))
    data = {col: gdf[col].tolist() for col in gdf.columns if col != "geometry"}
    result = gpd.GeoDataFrame(data, geometry=new_geoms)
    result = result.set_crs("EPSG:4326")
    return result


def _ensure_wgs84(gdf):
    """Reproject a GeoDataFrame to WGS84 (EPSG:4326) for mapping."""
    if gdf.crs is None:
        bounds = gdf.total_bounds
        guessed = _guess_crs_from_bounds(bounds)
        if guessed:
            gdf = gdf.set_crs(guessed, allow_override=True)
            if guessed != "EPSG:4326":
                gdf = gdf.to_crs(epsg=4326)
        else:
            return gdf
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def _get_center_zoom(gdf_wgs):
    """Calculate map center and zoom level from GeoDataFrame bounds."""
    bounds = gdf_wgs.total_bounds
    center_lat = float((bounds[1] + bounds[3]) / 2)
    center_lon = float((bounds[0] + bounds[2]) / 2)
    lat_range = bounds[3] - bounds[1]
    lon_range = bounds[2] - bounds[0]
    max_range = max(lat_range, lon_range)
    if max_range < 0.001:
        zoom = 18
    elif max_range < 0.01:
        zoom = 16
    elif max_range < 0.05:
        zoom = 14
    elif max_range < 0.2:
        zoom = 12
    elif max_range < 1:
        zoom = 10
    else:
        zoom = 8
    return (center_lat, center_lon), zoom


def _gdf_to_geojson(gdf, id_col=None, simplify_tolerance=0.00001):
    """Convert GeoDataFrame to GeoJSON dict for ipyleaflet."""
    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].simplify(simplify_tolerance)
    features = []
    for idx, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        props = {}
        if id_col and id_col in row.index:
            props["id"] = str(row[id_col])
            props["name"] = str(row[id_col])
        features.append({
            "type": "Feature",
            "geometry": shapely_mapping(geom),
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": features}


def _build_coord_transformer(gdf):
    """Build a pyproj transformer from the GDF's CRS to WGS84."""
    src_crs = gdf.crs
    if src_crs is None:
        guessed = _guess_crs_from_bounds(gdf.total_bounds)
        if guessed and guessed != "EPSG:4326":
            src_crs = guessed
        elif guessed == "EPSG:4326":
            return None, False
        else:
            return None, True
    if hasattr(src_crs, 'to_epsg') and src_crs.to_epsg() == 4326:
        return None, False
    transformer = Transformer.from_crs(src_crs, "EPSG:4326", always_xy=True)
    return transformer, True


def _find_features_in_bbox(bbox, pipes_geojson, juncs_geojson):
    """Find feature IDs whose geometry intersects a bounding box.
    bbox = [south, west, north, east] in WGS84."""
    south, west, north, east = bbox
    sel_box = shapely_box(west, south, east, north)
    found = set()

    for dataset in [pipes_geojson, juncs_geojson]:
        if not dataset:
            continue
        for feat in dataset.get("features", []):
            fid = feat.get("properties", {}).get("id", "")
            if not fid:
                continue
            from shapely.geometry import shape
            try:
                geom = shape(feat["geometry"])
                if geom.intersects(sel_box):
                    found.add(fid)
            except Exception:
                continue
    return found


# ── Module-level cache for expensive data prep ──
# Avoids re-reprojecting on every selection change
_cached_data = {}

# ── Module-level reference to the last map widget ──
# Used to read the user's current center/zoom on re-render,
# avoiding fragile observe/skip logic.
_last_map_widget = [None]


def _prepare_map_data(pipes_gdf, junctions_gdf, pumps_gdf, storage_gdf, issues, network_result):
    """Prepare all data needed for map rendering. Returns a cache key and data dict.
    This is expensive (reprojection, GeoJSON conversion) so we cache results."""
    # Build a simple cache key from data identity
    key_parts = []
    for name, gdf in [("p", pipes_gdf), ("j", junctions_gdf), ("pu", pumps_gdf), ("s", storage_gdf)]:
        if gdf is not None:
            key_parts.append(f"{name}:{id(gdf)}:{len(gdf)}")
        else:
            key_parts.append(f"{name}:None")
    key_parts.append(f"i:{len(issues) if issues else 0}")
    cache_key = "|".join(key_parts)

    global _cached_data
    if cache_key in _cached_data:
        return _cached_data[cache_key]

    result = {}

    # Determine center/zoom and coord_transformer
    center = (49.3, -123.1)
    zoom = 12
    coord_transformer = None

    for gdf in [pipes_gdf, junctions_gdf, pumps_gdf, storage_gdf]:
        if gdf is not None and len(gdf) > 0:
            coord_transformer, _ = _build_coord_transformer(gdf)
            gdf_wgs = _ensure_wgs84(gdf.copy())
            center, zoom = _get_center_zoom(gdf_wgs)
            break

    result["center"] = center
    result["zoom"] = zoom
    result["coord_transformer"] = coord_transformer

    # Prepare pipes GeoJSON
    if pipes_gdf is not None and len(pipes_gdf) > 0:
        pipes_wgs = _ensure_wgs84(pipes_gdf.copy())
        id_col = pipes_wgs.columns[0]
        result["pipes_geojson"] = _gdf_to_geojson(pipes_wgs, id_col=id_col)
        result["pipes_id_col"] = id_col
    else:
        result["pipes_geojson"] = None

    # Prepare junctions GeoJSON
    if junctions_gdf is not None and len(junctions_gdf) > 0:
        juncs_wgs = _ensure_wgs84(junctions_gdf.copy())
        id_col = juncs_wgs.columns[0]
        result["juncs_geojson"] = _gdf_to_geojson(juncs_wgs, id_col=id_col)
    else:
        result["juncs_geojson"] = None

    # Prepare issue marker locations
    markers = []
    if issues and network_result:
        G = network_result["graph"]
        for issue in issues:
            fid = str(issue.feature_id)
            display_name = ISSUE_DISPLAY_NAMES.get(issue.issue_type, issue.issue_type)
            coords = None
            if hasattr(issue, 'coordinates') and issue.coordinates:
                coords = issue.coordinates
            elif fid in G.nodes:
                c = G.nodes[fid].get("coords")
                if c:
                    coords = c
            if coords is None:
                for u, v, data in G.edges(data=True):
                    if str(data.get("pipe_id", "")) == fid:
                        uc = G.nodes.get(u, {}).get("coords")
                        vc = G.nodes.get(v, {}).get("coords")
                        if uc and vc:
                            coords = ((uc[0]+vc[0])/2, (uc[1]+vc[1])/2)
                        break
            if coords is None:
                continue
            lon, lat = float(coords[0]), float(coords[1])
            if coord_transformer:
                lon, lat = coord_transformer.transform(lon, lat)
            elif abs(lon) > 360 or abs(lat) > 180:
                continue
            markers.append({
                "fid": fid,
                "lat": lat,
                "lon": lon,
                "severity": issue.severity,
                "display": display_name,
            })
    result["markers"] = markers

    # Keep only last cache entry to avoid memory growth
    _cached_data.clear()
    _cached_data[cache_key] = result
    return result


@solara.component
def build_leaflet_map(
    pipes_gdf=None,
    junctions_gdf=None,
    pumps_gdf=None,
    storage_gdf=None,
    issues=None,
    network_result=None,
    selected_ids=None,
    on_feature_click=None,
    on_feature_select=None,
    on_box_select=None,
    multi_select=False,
    visible_layers=None,
):
    """Build and render an ipyleaflet map with all layers.
    Preserves map center/zoom across re-renders (selection changes etc.)."""

    if visible_layers is None:
        visible_layers = {}
    if selected_ids is None:
        selected_ids = set()
    if issues is None:
        issues = []

    # Prepare data (cached — only recomputes when GDFs/issues change)
    data = _prepare_map_data(
        pipes_gdf, junctions_gdf, pumps_gdf, storage_gdf,
        issues, network_result,
    )

    # ── Persistent map view ──
    # Read center/zoom from the PREVIOUS map widget (if it exists).
    # This captures any user pan/zoom without fragile observe/skip logic.
    prev = _last_map_widget[0]
    if prev is not None:
        try:
            center = tuple(prev.center)
            zoom = prev.zoom
        except Exception:
            center = data["center"]
            zoom = data["zoom"]
    else:
        center = data["center"]
        zoom = data["zoom"]

    # Build the map
    m = ipyleaflet.Map(
        center=center,
        zoom=zoom,
        basemap=ipyleaflet.basemaps.CartoDB.DarkMatter,
        layout={"height": "620px", "width": "100%"},
        scroll_wheel_zoom=True,
    )
    _last_map_widget[0] = m

    # ── Pipes Layer ──
    if data["pipes_geojson"] and visible_layers.get("Pipes", True):
        def pipe_style(feature):
            fid = feature.get("properties", {}).get("id", "")
            if fid in selected_ids:
                return {"color": "#00FFFF", "weight": 6, "opacity": 0.9}
            return {"color": "#4A90D9", "weight": 3, "opacity": 0.7}

        def on_pipe_click(feature=None, **kwargs):
            if feature:
                fid = feature.get("properties", {}).get("id", "")
                if fid:
                    if on_feature_click:
                        on_feature_click(fid)

        pipes_layer = ipyleaflet.GeoJSON(
            data=data["pipes_geojson"],
            style_callback=pipe_style,
            hover_style={"color": "#FFFFFF", "weight": 5},
            name="Pipes",
        )
        pipes_layer.on_click(on_pipe_click)
        m.add(pipes_layer)

    # ── Junctions Layer ──
    if data["juncs_geojson"] and visible_layers.get("Junctions", True):
        def junc_style(feature):
            fid = feature.get("properties", {}).get("id", "")
            if fid in selected_ids:
                return {
                    "color": "#00FFFF", "fillColor": "#00FFFF",
                    "weight": 2, "fillOpacity": 0.8, "radius": 6,
                }
            return {
                "color": "#4A90D9", "fillColor": "#4A90D9",
                "weight": 1, "fillOpacity": 0.7, "radius": 4,
            }

        def on_junc_click(feature=None, **kwargs):
            if feature:
                fid = feature.get("properties", {}).get("id", "")
                if fid:
                    if on_feature_click:
                        on_feature_click(fid)

        juncs_layer = ipyleaflet.GeoJSON(
            data=data["juncs_geojson"],
            point_style={"radius": 4, "fillOpacity": 0.7},
            style_callback=junc_style,
            hover_style={"color": "#FFFFFF", "weight": 3, "fillOpacity": 1.0},
            name="Junctions",
        )
        juncs_layer.on_click(on_junc_click)
        m.add(juncs_layer)

    # ── Issue Markers ──
    for mkr in data["markers"]:
        icon = ipyleaflet.AwesomeIcon(
            name="exclamation-triangle",
            marker_color="red" if mkr["severity"] == "HIGH" else "orange" if mkr["severity"] == "MEDIUM" else "blue",
            icon_color="white",
        )
        marker = ipyleaflet.Marker(
            location=(mkr["lat"], mkr["lon"]),
            icon=icon,
            title=f"{mkr['display']}: {mkr['fid']}",
        )

        def make_issue_click(feature_id):
            def handler(**kwargs):
                if on_feature_click:
                    on_feature_click(feature_id)
            return handler

        marker.on_click(make_issue_click(mkr["fid"]))
        m.add(marker)

    # ── Draw Control for box selection ──
    if on_box_select:
        draw_control = ipyleaflet.DrawControl(
            rectangle={"shapeOptions": {
                "color": "#00FFFF",
                "fillColor": "#00FFFF",
                "fillOpacity": 0.1,
                "weight": 2,
                "dashArray": "5,5",
            }},
            polyline={},
            polygon={},
            circle={},
            circlemarker={},
            marker={},
            edit=False,
            remove=False,
        )

        def handle_draw(target, action, geo_json):
            if action == "created" and geo_json:
                geom_type = geo_json.get("geometry", {}).get("type", "")
                coords = geo_json.get("geometry", {}).get("coordinates", [])
                if geom_type == "Polygon" and coords:
                    ring = coords[0]
                    lons = [c[0] for c in ring]
                    lats = [c[1] for c in ring]
                    bbox = [min(lats), min(lons), max(lats), max(lons)]
                    found = _find_features_in_bbox(
                        bbox, data["pipes_geojson"], data["juncs_geojson"]
                    )
                    if found:
                        on_box_select(found)
                # Clear the drawn rectangle
                draw_control.clear()

        draw_control.on_draw(handle_draw)
        m.add(draw_control)

    # Layer control + scale
    m.add(ipyleaflet.LayersControl(position="topright"))
    m.add(ipyleaflet.ScaleControl(position="bottomleft"))

    solara.display(m)
