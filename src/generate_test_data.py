"""
generate_test_data.py — Generate synthetic sewer network test data with known issues.

Creates pipe, junction, pump, and storage records as plain Python dicts
(mimicking the output of ingest.py) for testing the network builder and
profile analyzer WITHOUT needing arcpy or real shapefiles.

The synthetic network layout:

    MH-001 (rim=110, inv=100)
      |  P-001 (8", 200ft, slope OK)
    MH-002 (rim=108, inv=98)
      |  P-002 (8", 200ft, ADVERSE SLOPE — inv goes UP)
    MH-003 (rim=106, inv=99)  <-- invert higher than upstream = bad
      |  P-003 (8" -> 6" DIAMETER DECREASE, 200ft)
    MH-004 (rim=104, inv=95)
      |  P-004 (6", 200ft, slope OK)
    MH-005 (rim=102, inv=93)
      |  P-005 (6", 200ft, to pump station)
    PS-001 (inlet_inv=91)
      |  P-006 (FORCE MAIN, 4", 500ft, uphill to storage)
    ST-001 (base=95, max=110)

    Branch from MH-002:
    MH-002
      |  P-007 (8", 150ft)
    MH-006 (rim=107, inv=97)
      |  P-008 (8", 150ft)
    MH-007 (rim=105, inv=95.5)  <-- DISCONNECTED (no pipe out)

    Orphan junction (no connections at all):
    MH-008 (rim=100, inv=90)

Known issues embedded:
1. ADVERSE SLOPE: P-002 (ds_invert > us_invert)
2. DIAMETER DECREASE: P-003 (8" -> 6" without pump)
3. INVERT MISMATCH at MH-003: incoming pipe ds_inv=99, but junction inv varies
4. DISCONNECTED NODE: MH-007 (dead end, no outgoing pipe)
5. ORPHAN NODE: MH-008 (no connections at all)
6. SHALLOW STRUCTURE: MH-005 (rim - inv = 9ft, actually fine, but we include one shallow)
7. NULL DIAMETER: P-006 has diameter set to None to test null handling
"""

import math


def _make_point(x, y):
    """Stub geometry — tuple representing a point."""
    return {"type": "point", "x": x, "y": y}


def _make_line(x1, y1, x2, y2):
    """Stub geometry — dict representing a line from (x1,y1) to (x2,y2)."""
    return {"type": "line", "start": (x1, y1), "end": (x2, y2)}


def generate_junctions():
    """Generate synthetic junction/manhole records."""
    junctions = [
        {"junction_id": "MH-001", "rim_elev": 110.0, "invert_elev": 100.0,
         "depth": 10.0, "structure_type": "MANHOLE", "geometry": _make_point(1000, 5000)},

        {"junction_id": "MH-002", "rim_elev": 108.0, "invert_elev": 98.0,
         "depth": 10.0, "structure_type": "MANHOLE", "geometry": _make_point(1000, 4800)},

        {"junction_id": "MH-003", "rim_elev": 106.0, "invert_elev": 98.5,  # ISSUE: doesn't match incoming pipe ds_inv=99.0
         "depth": 7.5, "structure_type": "MANHOLE", "geometry": _make_point(1000, 4600)},

        {"junction_id": "MH-004", "rim_elev": 104.0, "invert_elev": 95.0,
         "depth": 9.0, "structure_type": "MANHOLE", "geometry": _make_point(1000, 4400)},

        {"junction_id": "MH-005", "rim_elev": 102.0, "invert_elev": 93.0,
         "depth": 9.0, "structure_type": "MANHOLE", "geometry": _make_point(1000, 4200)},

        # Branch from MH-002
        {"junction_id": "MH-006", "rim_elev": 107.0, "invert_elev": 97.0,
         "depth": 10.0, "structure_type": "MANHOLE", "geometry": _make_point(1200, 4700)},

        {"junction_id": "MH-007", "rim_elev": 105.0, "invert_elev": 95.5,  # DEAD END
         "depth": 9.5, "structure_type": "MANHOLE", "geometry": _make_point(1400, 4600)},

        # ORPHAN — no pipes connect to this
        {"junction_id": "MH-008", "rim_elev": 100.0, "invert_elev": 90.0,
         "depth": 10.0, "structure_type": "MANHOLE", "geometry": _make_point(2000, 5000)},
    ]
    return junctions


def generate_pipes():
    """Generate synthetic pipe records with known issues."""
    pipes = [
        # Main trunk — normal slope
        {"pipe_id": "P-001", "us_invert": 100.0, "ds_invert": 98.0,
         "diameter": 8, "material": "PVC", "length": 200.0,
         "us_node": "MH-001", "ds_node": "MH-002",
         "geometry": _make_line(1000, 5000, 1000, 4800)},

        # ADVERSE SLOPE — downstream invert is HIGHER than upstream
        {"pipe_id": "P-002", "us_invert": 98.0, "ds_invert": 99.0,
         "diameter": 8, "material": "PVC", "length": 200.0,
         "us_node": "MH-002", "ds_node": "MH-003",
         "geometry": _make_line(1000, 4800, 1000, 4600)},

        # DIAMETER DECREASE without pump — 8" to 6"
        {"pipe_id": "P-003", "us_invert": 99.0, "ds_invert": 95.0,
         "diameter": 6, "material": "PVC", "length": 200.0,
         "us_node": "MH-003", "ds_node": "MH-004",
         "geometry": _make_line(1000, 4600, 1000, 4400)},

        # Normal pipe
        {"pipe_id": "P-004", "us_invert": 95.0, "ds_invert": 93.0,
         "diameter": 6, "material": "PVC", "length": 200.0,
         "us_node": "MH-004", "ds_node": "MH-005",
         "geometry": _make_line(1000, 4400, 1000, 4200)},

        # To pump station
        {"pipe_id": "P-005", "us_invert": 93.0, "ds_invert": 91.0,
         "diameter": 6, "material": "PVC", "length": 200.0,
         "us_node": "MH-005", "ds_node": "PS-001",
         "geometry": _make_line(1000, 4200, 1000, 4000)},

        # FORCE MAIN from pump — NULL DIAMETER (data error)
        {"pipe_id": "P-006", "us_invert": 91.0, "ds_invert": 95.0,
         "diameter": None, "material": "DIP", "length": 500.0,
         "us_node": "PS-001", "ds_node": "ST-001",
         "geometry": _make_line(1000, 4000, 1000, 3500)},

        # Branch pipes
        {"pipe_id": "P-007", "us_invert": 98.0, "ds_invert": 97.0,
         "diameter": 8, "material": "PVC", "length": 150.0,
         "us_node": "MH-002", "ds_node": "MH-006",
         "geometry": _make_line(1000, 4800, 1200, 4700)},

        {"pipe_id": "P-008", "us_invert": 97.0, "ds_invert": 95.5,
         "diameter": 8, "material": "PVC", "length": 150.0,
         "us_node": "MH-006", "ds_node": "MH-007",
         "geometry": _make_line(1200, 4700, 1400, 4600)},
    ]
    return pipes


def generate_pumps():
    """Generate synthetic pump station records."""
    pumps = [
        {"station_id": "PS-001", "capacity": 500.0,
         "pump_on_elev": 89.0, "pump_off_elev": 87.0,
         "inlet_invert": 91.0, "force_main_id": "P-006",
         "geometry": _make_point(1000, 4000)},
    ]
    return pumps


def generate_storage():
    """Generate synthetic storage facility records."""
    storage = [
        {"tank_id": "ST-001", "volume": 50000.0,
         "base_elev": 95.0, "max_elev": 110.0, "min_elev": 96.0,
         "geometry": _make_point(1000, 3500)},
    ]
    return storage


def generate_all():
    """
    Generate complete synthetic test dataset.

    Returns dict mimicking ingest.ingest_all() output structure.
    """
    junctions = generate_junctions()
    pipes = generate_pipes()
    pumps = generate_pumps()
    storage = generate_storage()

    return {
        "pipes": {"records": pipes, "count": len(pipes)},
        "junctions": {"records": junctions, "count": len(junctions)},
        "pumps": {"records": pumps, "count": len(pumps)},
        "storage": {"records": storage, "count": len(storage)},
    }


# Known issues for test verification
EXPECTED_ISSUES = {
    "adverse_slopes": ["P-002"],
    "diameter_decreases": [("P-002", "P-003")],  # 8" -> 6" at MH-003
    "null_diameters": ["P-006"],
    "dead_ends": ["MH-007"],
    "orphan_nodes": ["MH-008"],
    "invert_mismatches": ["MH-003"],  # incoming ds_inv doesn't match junction inv
}


if __name__ == "__main__":
    data = generate_all()
    print(f"Generated test data:")
    print(f"  Junctions: {data['junctions']['count']}")
    print(f"  Pipes:     {data['pipes']['count']}")
    print(f"  Pumps:     {data['pumps']['count']}")
    print(f"  Storage:   {data['storage']['count']}")
    print(f"\nExpected issues to detect:")
    for issue_type, items in EXPECTED_ISSUES.items():
        print(f"  {issue_type}: {items}")
