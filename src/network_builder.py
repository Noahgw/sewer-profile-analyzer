"""
network_builder.py — Build a directed graph from ingested sewer network data.

Creates a NetworkX-compatible directed graph from pipes, junctions, pumps,
and storage features. Handles spatial snapping, flow direction inference,
and connectivity validation.

NOTE: This module uses networkx when available (production, inside ArcGIS Pro).
Falls back to a lightweight pure-Python directed graph for testing.
"""

import math

try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False


# ============================================================
# Lightweight fallback graph (no external dependencies)
# ============================================================

class SimpleDirectedGraph:
    """
    Minimal directed graph implementation for testing without networkx.
    Mirrors the subset of networkx.DiGraph API that we use.
    """

    def __init__(self):
        self._nodes = {}   # {node_id: {attr_dict}}
        self._adj = {}     # {node_id: {neighbor_id: {edge_attrs}}}  (outgoing)
        self._pred = {}    # {node_id: {predecessor_id: {edge_attrs}}}  (incoming)

    def add_node(self, node_id, **attrs):
        if node_id not in self._nodes:
            self._nodes[node_id] = {}
            self._adj[node_id] = {}
            self._pred[node_id] = {}
        self._nodes[node_id].update(attrs)

    def add_edge(self, u, v, **attrs):
        self.add_node(u)
        self.add_node(v)
        self._adj[u][v] = attrs
        self._pred[v][u] = attrs

    @property
    def nodes(self):
        return self._nodes

    def edges(self, data=False):
        result = []
        for u, neighbors in self._adj.items():
            for v, attrs in neighbors.items():
                if data:
                    result.append((u, v, attrs))
                else:
                    result.append((u, v))
        return result

    def successors(self, node_id):
        return list(self._adj.get(node_id, {}).keys())

    def predecessors(self, node_id):
        return list(self._pred.get(node_id, {}).keys())

    def in_degree(self, node_id):
        return len(self._pred.get(node_id, {}))

    def out_degree(self, node_id):
        return len(self._adj.get(node_id, {}))

    def has_node(self, node_id):
        return node_id in self._nodes

    def has_edge(self, u, v):
        return v in self._adj.get(u, {})

    def number_of_nodes(self):
        return len(self._nodes)

    def number_of_edges(self):
        return sum(len(nbrs) for nbrs in self._adj.values())

    def get_edge_data(self, u, v):
        return self._adj.get(u, {}).get(v, None)

    def subgraph_nodes(self):
        """Find weakly connected components (treating edges as undirected)."""
        visited = set()
        components = []
        undirected = {n: set() for n in self._nodes}
        for u, neighbors in self._adj.items():
            for v in neighbors:
                undirected[u].add(v)
                undirected[v].add(u)

        for node in self._nodes:
            if node not in visited:
                component = set()
                stack = [node]
                while stack:
                    n = stack.pop()
                    if n not in visited:
                        visited.add(n)
                        component.add(n)
                        stack.extend(undirected[n] - visited)
                components.append(component)
        return components


def _create_graph():
    """Create a directed graph using networkx if available, else fallback."""
    if HAS_NETWORKX:
        return nx.DiGraph()
    return SimpleDirectedGraph()


# ============================================================
# Spatial helpers
# ============================================================

def _distance(p1, p2):
    """Euclidean distance between two points."""
    if isinstance(p1, dict):
        x1, y1 = p1["x"], p1["y"]
    else:
        x1, y1 = p1
    if isinstance(p2, dict):
        x2, y2 = p2["x"], p2["y"]
    else:
        x2, y2 = p2
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


def _get_point_coords(geom):
    """
    Extract (x, y) from any geometry type.
    Returns None for null, empty, or unreadable geometries.
    """
    try:
        if geom is None:
            return None

        # Stub dict from synthetic test data
        if isinstance(geom, dict):
            if geom.get("type") == "point":
                return (geom["x"], geom["y"])
            return None

        # Check for empty geometry first (Shapely)
        if hasattr(geom, "is_empty") and geom.is_empty:
            return None

        # Use geom_type string if available (most reliable for Shapely)
        geom_type = getattr(geom, "geom_type", None)

        if geom_type == "Point":
            return (float(geom.x), float(geom.y))

        if geom_type == "MultiPoint":
            pts = list(geom.geoms)
            if pts:
                return (float(pts[0].x), float(pts[0].y))
            return None

        if geom_type in ("LineString", "MultiLineString", "Polygon",
                         "MultiPolygon", "GeometryCollection"):
            c = geom.centroid
            if c is not None and not c.is_empty:
                return (float(c.x), float(c.y))
            return None

        # Fallback — try lowercase .x/.y first (Shapely)
        try:
            if hasattr(geom, "x") and hasattr(geom, "y"):
                return (float(geom.x), float(geom.y))
        except (AttributeError, TypeError):
            pass

        # Fallback — any geometry with centroid
        if hasattr(geom, "centroid"):
            try:
                c = geom.centroid
                if c is not None:
                    return (float(c.x), float(c.y))
            except (AttributeError, TypeError):
                pass

        # arcpy Point geometry (uppercase .X/.Y — only works in arcpy)
        try:
            if hasattr(geom, "X") and hasattr(geom, "Y"):
                return (float(geom.X), float(geom.Y))
        except (AttributeError, TypeError):
            pass

    except Exception:
        pass

    return None


def _get_line_endpoints(geom):
    """
    Extract start (x, y) and end (x, y) from any line geometry.
    Returns (None, None) for null, empty, or unreadable geometries.
    """
    try:
        if geom is None:
            return None, None

        # Stub dict from synthetic test data
        if isinstance(geom, dict):
            if geom.get("type") == "line":
                return geom["start"], geom["end"]
            return None, None

        # Check for empty geometry first (Shapely)
        if hasattr(geom, "is_empty") and geom.is_empty:
            return None, None

        geom_type = getattr(geom, "geom_type", None)

        if geom_type == "LineString":
            coords = list(geom.coords)
            if len(coords) >= 2:
                return coords[0], coords[-1]
            return None, None

        if geom_type == "MultiLineString":
            sub_lines = list(geom.geoms)
            if sub_lines:
                first_coords = list(sub_lines[0].coords)
                last_coords = list(sub_lines[-1].coords)
                if first_coords and last_coords:
                    return first_coords[0], last_coords[-1]
            return None, None

        # Generic fallback — any geometry with coords (Shapely)
        if hasattr(geom, "coords"):
            try:
                coords = list(geom.coords)
                if len(coords) >= 2:
                    return coords[0], coords[-1]
            except (AttributeError, TypeError, NotImplementedError):
                pass

        # arcpy polyline (uppercase .X/.Y — only works in arcpy)
        try:
            if hasattr(geom, "firstPoint") and hasattr(geom, "lastPoint"):
                fp = geom.firstPoint
                lp = geom.lastPoint
                return (float(fp.X), float(fp.Y)), (float(lp.X), float(lp.Y))
        except (AttributeError, TypeError):
            pass

    except Exception:
        pass

    return None, None


# ============================================================
# Network construction
# ============================================================

def build_node_index(junctions, pumps=None, storage=None):
    """
    Build a spatial index of all node features.

    Returns
    -------
    dict: {node_id: {'coords': (x,y), 'type': str, 'attrs': dict}}
    """
    index = {}

    for junc in junctions:
        jid = junc.get("junction_id", f"J-{len(index)}")
        coords = _get_point_coords(junc.get("geometry"))
        if coords:
            index[jid] = {
                "coords": coords,
                "type": "junction",
                "attrs": {k: v for k, v in junc.items() if k != "geometry"},
            }

    if pumps:
        for pump in pumps:
            pid = pump.get("station_id", f"P-{len(index)}")
            coords = _get_point_coords(pump.get("geometry"))
            if coords:
                index[pid] = {
                    "coords": coords,
                    "type": "pump",
                    "attrs": {k: v for k, v in pump.items() if k != "geometry"},
                }

    if storage:
        for store in storage:
            sid = store.get("tank_id", f"S-{len(index)}")
            coords = _get_point_coords(store.get("geometry"))
            if coords:
                index[sid] = {
                    "coords": coords,
                    "type": "storage",
                    "attrs": {k: v for k, v in store.items() if k != "geometry"},
                }

    return index


def _find_nearest_node(coords, node_index, snap_tolerance):
    """Find the nearest node within snap tolerance. Returns (node_id, distance) or (None, None)."""
    best_id = None
    best_dist = float("inf")
    for nid, ndata in node_index.items():
        d = _distance(coords, ndata["coords"])
        if d < best_dist:
            best_dist = d
            best_id = nid
    if best_dist <= snap_tolerance:
        return best_id, best_dist
    return None, None


def build_network(pipes, junctions, pumps=None, storage=None, snap_tolerance=5.0):
    """
    Build a directed graph from network features.

    Strategy:
    1. If pipes have us_node/ds_node attributes, use those directly.
    2. Otherwise, snap pipe endpoints to nearest node within tolerance.
    3. Add all nodes with attributes, add pipes as directed edges.
    4. Identify connectivity issues.

    Parameters
    ----------
    pipes : list[dict]
        Pipe records from ingestion.
    junctions : list[dict]
        Junction records from ingestion.
    pumps : list[dict], optional
    storage : list[dict], optional
    snap_tolerance : float
        Max distance (in CRS units) to snap pipe endpoints to nodes.

    Returns
    -------
    dict:
        'graph': directed graph (networkx.DiGraph or SimpleDirectedGraph)
        'snap_log': list of snapping actions taken
        'warnings': list of warning messages
        'stats': dict with network statistics
    """
    G = _create_graph()
    snap_log = []
    warnings = []

    # Build node spatial index
    node_index = build_node_index(junctions, pumps, storage)

    # Add all nodes to graph
    for nid, ndata in node_index.items():
        G.add_node(nid, node_type=ndata["type"], coords=ndata["coords"], **ndata["attrs"])

    # Process pipes
    for pipe in pipes:
        pid = pipe.get("pipe_id", "UNKNOWN")
        us_node = pipe.get("us_node")
        ds_node = pipe.get("ds_node")

        # If node IDs are provided, use them
        if us_node and ds_node:
            # Verify nodes exist in our index
            if not G.has_node(us_node):
                # Node referenced but not in junctions — create virtual node
                warnings.append(f"Pipe {pid}: upstream node '{us_node}' not found in junctions. Creating virtual node.")
                line_start, _ = _get_line_endpoints(pipe.get("geometry"))
                G.add_node(us_node, node_type="virtual", coords=line_start)

            if not G.has_node(ds_node):
                warnings.append(f"Pipe {pid}: downstream node '{ds_node}' not found in junctions. Creating virtual node.")
                _, line_end = _get_line_endpoints(pipe.get("geometry"))
                G.add_node(ds_node, node_type="virtual", coords=line_end)

        else:
            # Snap pipe endpoints to nearest nodes
            line_start, line_end = _get_line_endpoints(pipe.get("geometry"))

            if line_start:
                us_node, us_dist = _find_nearest_node(line_start, node_index, snap_tolerance)
                if us_node:
                    snap_log.append(f"Pipe {pid} start snapped to {us_node} (dist={us_dist:.2f})")
                else:
                    # Create virtual node at pipe start
                    us_node = f"VIRT-{pid}-US"
                    G.add_node(us_node, node_type="virtual", coords=line_start)
                    warnings.append(f"Pipe {pid}: no junction within {snap_tolerance} of start. Virtual node created.")

            if line_end:
                ds_node, ds_dist = _find_nearest_node(line_end, node_index, snap_tolerance)
                if ds_node:
                    snap_log.append(f"Pipe {pid} end snapped to {ds_node} (dist={ds_dist:.2f})")
                else:
                    ds_node = f"VIRT-{pid}-DS"
                    G.add_node(ds_node, node_type="virtual", coords=line_end)
                    warnings.append(f"Pipe {pid}: no junction within {snap_tolerance} of end. Virtual node created.")

        if us_node and ds_node:
            # Add pipe as directed edge
            edge_attrs = {k: v for k, v in pipe.items() if k not in ("geometry", "us_node", "ds_node")}
            G.add_edge(us_node, ds_node, **edge_attrs)

    # Connectivity analysis
    if HAS_NETWORKX:
        components = list(nx.weakly_connected_components(G))
    else:
        components = G.subgraph_nodes()

    # Find orphan and dead-end nodes
    orphan_nodes = []
    dead_end_nodes = []
    for nid in G.nodes:
        in_deg = G.in_degree(nid)
        out_deg = G.out_degree(nid)
        if in_deg == 0 and out_deg == 0:
            orphan_nodes.append(nid)
        elif in_deg > 0 and out_deg == 0:
            node_type = G.nodes[nid].get("node_type", "unknown") if HAS_NETWORKX else G._nodes[nid].get("node_type", "unknown")
            # Dead ends are OK for pumps/storage (they're terminal)
            if node_type not in ("pump", "storage"):
                dead_end_nodes.append(nid)

    # Source nodes (no incoming pipes — likely system headwaters)
    source_nodes = []
    for nid in G.nodes:
        if G.in_degree(nid) == 0 and G.out_degree(nid) > 0:
            source_nodes.append(nid)

    stats = {
        "total_nodes": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "connected_components": len(components),
        "largest_component_size": max(len(c) for c in components) if components else 0,
        "orphan_nodes": orphan_nodes,
        "dead_end_nodes": dead_end_nodes,
        "source_nodes": source_nodes,
        "virtual_nodes_created": sum(1 for nid in G.nodes
                                     if (G.nodes[nid] if HAS_NETWORKX else G._nodes[nid]).get("node_type") == "virtual"),
    }

    if len(components) > 1:
        warnings.append(
            f"Network has {len(components)} disconnected components. "
            f"Largest has {stats['largest_component_size']} nodes."
        )
    if orphan_nodes:
        warnings.append(f"Orphan nodes (no connections): {orphan_nodes}")
    if dead_end_nodes:
        warnings.append(f"Dead-end nodes (incoming but no outgoing pipes): {dead_end_nodes}")

    return {
        "graph": G,
        "snap_log": snap_log,
        "warnings": warnings,
        "stats": stats,
    }
