"""
run_local_test.py — Verify the webapp pipeline works end-to-end.

Tests the full flow: synthetic data -> network build -> analysis -> fixes
using the same modules the Streamlit app uses (no arcpy, no streamlit needed).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generate_test_data import generate_all
from src.network_builder import build_network
from src.profile_analyzer import run_full_analysis, trace_profile
from webapp.auto_fix import generate_all_fixes


def run():
    print("=" * 60)
    print("WEBAPP PIPELINE TEST")
    print("=" * 60)

    # Generate data
    data = generate_all()
    print(f"\nData: {data['pipes']['count']} pipes, "
          f"{data['junctions']['count']} junctions, "
          f"{data['pumps']['count']} pumps, "
          f"{data['storage']['count']} storage")

    # Build network
    network = build_network(
        data["pipes"]["records"],
        data["junctions"]["records"],
        data["pumps"]["records"],
        data["storage"]["records"],
        snap_tolerance=5.0,
    )
    stats = network["stats"]
    print(f"\nNetwork: {stats['total_nodes']} nodes, {stats['total_edges']} edges, "
          f"{stats['connected_components']} components")

    # Analyze
    analysis = run_full_analysis(network)
    print(f"\nIssues found: {analysis['summary']['total_issues']}")
    for issue in analysis["issues"]:
        print(f"  {issue}")

    # Generate fixes
    fixes = generate_all_fixes(
        analysis,
        data["pipes"]["records"],
        data["junctions"]["records"],
    )
    print(f"\nFixes proposed: {len(fixes)}")
    for fix in fixes:
        print(f"  [{fix.confidence}] {fix.issue_type} {fix.feature_id}: {fix.description[:80]}...")

    # Trace profile
    profile = trace_profile(network["graph"], "MH-001")
    print(f"\nProfile trace from MH-001: {len(profile)} nodes")
    for entry in profile:
        pipe = entry.get("pipe_to_next")
        inv = entry.get("invert_elev") or "N/A"
        if pipe:
            print(f"  {entry['node_id']:8s} (inv={inv}) "
                  f"--[{pipe['pipe_id']}, {pipe['diameter']}\", "
                  f"US={pipe['us_invert']} DS={pipe['ds_invert']}]--> {pipe['to_node']}")
        else:
            print(f"  {entry['node_id']:8s} (inv={inv}) -- END")

    # Verify counts
    print(f"\n{'=' * 60}")
    all_pass = True
    checks = [
        ("Issues detected", analysis['summary']['total_issues'] >= 6),
        ("Fixes proposed", len(fixes) >= 5),
        ("Profile traced", len(profile) >= 5),
        ("Multiple components", stats['connected_components'] >= 2),
        ("Orphans found", len(stats['orphan_nodes']) >= 1),
        ("Dead ends found", len(stats['dead_end_nodes']) >= 1),
    ]
    for name, passed in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  {status}: {name}")

    print(f"\nRESULT: {'ALL PASSED' if all_pass else 'SOME FAILED'}")
    print("=" * 60)
    return all_pass


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
