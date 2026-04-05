# Sewer Profile Analyzer — User Guide

**Version 1.0 | April 2026**

A web-based sewer network QA/QC tool that detects profile issues in your GIS pipe and junction data, visualizes them on an interactive map, and provides automated fix tools to correct common problems.

**Live App:** Deployed on Railway at your project URL.

---

## Table of Contents

1. [Getting Started](#1-getting-started)
2. [Uploading Data](#2-uploading-data)
3. [Field Mapping](#3-field-mapping)
4. [Settings & Thresholds](#4-settings--thresholds)
5. [Running the Analysis](#5-running-the-analysis)
6. [The Map](#6-the-map)
7. [Viewing Issues](#7-viewing-issues)
8. [Feature Inspector & Fix Tools](#8-feature-inspector--fix-tools)
9. [Profile View](#9-profile-view)
10. [Data Tables](#10-data-tables)
11. [Network Info](#11-network-info)
12. [Issue Type Reference](#12-issue-type-reference)
13. [Fix Strategy Reference](#13-fix-strategy-reference)

---

## 1. Getting Started

The app follows a three-step workflow:

1. **Upload** — Add your Pipes and Junctions shapefiles (plus optional Pumps and Storage).
2. **Map Fields** — Verify that auto-detected field mappings match your data schema.
3. **Analyze** — Click **Run Analysis** to detect profile issues across your network.

The sidebar on the left contains all configuration. The main area shows the map, issue tables, profile view, and data tables.

---

## 2. Uploading Data

In the sidebar **Upload** tab, upload ZIP archives containing your shapefiles:

| Dataset | Required? | Description |
|---|---|---|
| **Pipes (.zip)** | Yes | Sanitary main / gravity pipe features with invert elevations |
| **Junctions (.zip)** | Yes | Manholes / junction structures with rim and invert elevations |
| **Pumps (.zip)** | No | Pump station locations (used to skip diameter decrease checks at pump stations) |
| **Storage (.zip)** | No | Storage tanks / wet wells |

Each ZIP should contain the standard shapefile components (`.shp`, `.shx`, `.dbf`, `.prj`, etc.).

After uploading, you'll see a confirmation with the feature count (e.g., "Loaded 1,247 pipes").

---

## 3. Field Mapping

Switch to the **Fields** tab in the sidebar. The app auto-detects which columns in your shapefile correspond to the required internal fields.

### Required Fields

**Pipes:**
- `pipe_id` — Unique identifier for each pipe
- `us_invert` — Upstream invert elevation
- `ds_invert` — Downstream invert elevation
- `diameter` — Pipe diameter

**Junctions:**
- `junction_id` — Unique identifier for each junction/manhole
- `invert_elev` — Junction invert (bottom) elevation

### Reviewing Mappings

- Each field shows the auto-detected source column in a dropdown.
- If a mapping is incorrect, select the correct source column from the dropdown.
- Fields marked with an asterisk (*) are required — analysis will fail without them.
- You can also set the **unit** for each field (e.g., feet vs. metres) if your data isn't in metric.

---

## 4. Settings & Thresholds

Switch to the **Settings** tab in the sidebar to configure analysis thresholds:

| Setting | Default | Range | Description |
|---|---|---|---|
| **Snap Tolerance** | 1.0 m | 0.1–20.0 m | Maximum distance to snap junctions to pipe endpoints when building the network graph |
| **Invert Mismatch Tolerance** | 0.01 m | 0.001–0.5 m | Allowable difference between pipe inverts and junction inverts at a shared node |
| **Min Structure Depth** | 0.6 m | 0.3–2.0 m | Minimum rim-to-invert depth before flagging as a shallow structure |
| **Max Structure Depth** | 10.0 m | 5.0–20.0 m | Maximum rim-to-invert depth before flagging as a deep structure |
| **Min Slope (m/m)** | 0.005 | User input | Minimum allowable pipe slope used by all fix strategies (0.005 = 0.5%) |

---

## 5. Running the Analysis

Click the **Run Analysis** button at the bottom of the sidebar. The analysis:

1. Builds a directed network graph from your pipes and junctions (snapping endpoints within the snap tolerance).
2. Runs six checks across the network:
   - Adverse/flat slopes
   - Invert mismatches at junctions
   - Diameter continuity
   - Structure depths (rim-to-invert)
   - Missing (null) inverts and diameters
   - Connectivity (dead ends, orphan nodes)
3. Returns all detected issues, displayed on the map and in the Issue Details table.

---

## 6. The Map

After analysis, the main area shows an interactive map (dark basemap) with your network.

### Layers

Toggle layer visibility in the **Layers** section of the sidebar:
- **Pipes** — Shown as blue lines; issue pipes are colored by issue type
- **Junctions** — Shown as blue circles
- **Flow Arrows** — Directional indicators showing pipe flow direction
- **Pumps / Storage** — Optional layers if uploaded

### Issue Coloring on Pipes

Pipes with issues are colored directly on the map:
- **Red** — Adverse / Flat Slope
- **Orange** — Invert Mismatch
- **Gold** — Diameter Decrease
- **Purple** — Null Invert / Null Diameter
- Other issue types shown with their respective colors (see [Issue Type Reference](#12-issue-type-reference))

### Issue Markers

Exclamation triangle markers appear at issue locations, colored by severity:
- **Red** — HIGH severity
- **Orange** — MEDIUM severity
- **Blue** — LOW severity

### Selecting Features

There are two ways to select features:

1. **Box Select** — Use the rectangle draw tool (in the map toolbar) to drag a selection box. All pipes and junctions within the box are added to your selection.
2. **Click** — Click any pipe, junction, or issue marker to inspect it in the detail panel.

### Selection Management

In the **Selection** section of the sidebar:
- View the count and list of selected features
- Remove individual features with the **✕** button
- **Zoom to Selection** — Centers and zooms the map to your selected features
- **Clear All** — Removes all features from the selection

---

## 7. Viewing Issues

### Issue Filters

In the **Issue Filters** section of the sidebar, toggle issue types on/off:
- Each toggle button shows the issue type name and count (e.g., "Adverse Slope (23)")
- Click a button to show/hide that issue type on the map and in tables
- Use **All** / **None** buttons to quickly enable or disable all types

### Issue Details Table

The **Issue Details** tab (below the map) shows a paginated table of all issues matching your current filters:

| Column | Description |
|---|---|
| **Severity** | Colored badge — HIGH (red), MEDIUM (orange), LOW (blue) |
| **Type** | Issue type with colored indicator dot |
| **Feature / Message** | Feature ID in bold, detailed message below |
| **Zoom** | Button to zoom the map to that feature and open its inspector |

The table shows 25 issues per page with pagination controls.

---

## 8. Feature Inspector & Fix Tools

Click any feature on the map to open the **Feature Inspector** in the right panel.

### Feature Details

- Shows all attributes (field name and value pairs)
- Indicates whether the feature is in your current selection
- **Add/Remove from Selection** button

### Related Issues

Lists all issues affecting the selected feature, each showing:
- Issue type (with color)
- Severity badge
- Detailed message

### Fix Tools

For each issue on the selected feature, the inspector shows available automated fix strategies as buttons. Click a strategy button to apply it.

**Connectivity Confirmation:** When a fix changes inverts at a shared junction (e.g., Min Slope from Upstream), a dialog appears asking if you want to also adjust connecting pipes to maintain connectivity:

- **"Yes, adjust pipes"** — Apply the fix and update adjacent pipe inverts
- **"No, fix only"** — Apply only the base fix to the target pipe
- **"Cancel"** — Discard the fix entirely

### Undo

After applying fixes, an **Undo Last Fix** button appears at the bottom of the inspector. Each click undoes the most recent fix group (including any connectivity adjustments that were part of it).

The edit count (e.g., "3 edit(s) applied") shows how many ledger entries are active.

---

## 9. Profile View

The **Profile View** tab (below the map) shows an interactive elevation profile of your selected pipes.

### What's Displayed

- **Ground Surface Line** — Brown line connecting rim elevations across manholes, with light earth-fill shading
- **Pipe Barrels** — Rectangular shapes from invert to crown (invert + diameter), colored blue (normal) or red (adverse slope)
- **Manholes** — Vertical rectangles from invert to rim elevation, colored orange if they have issues
- **Issue Markers** — Triangle markers at problem locations with hover details
- **Labels** — Pipe IDs, diameters, and slope percentages at pipe centers; node IDs above manholes

### Reactive Updates

The profile view updates automatically when you:
- Change your selection on the map
- Apply a fix using the Fix Tools
- Undo a fix

### How Selection Works

- Select one or more pipes (via box select or clicking) to see their connected profile
- The profile includes manholes at both ends of all selected pipes, even if the manholes aren't explicitly selected
- Pipes are ordered into a chain following the flow path from upstream to downstream

### Interactivity

- **Hover** over any element to see detailed info (invert elevations, diameter, slope, issue details)
- **Zoom/pan** using Plotly's built-in controls
- **Legend** at top shows Ground Surface trace

---

## 10. Data Tables

The **Pipes** and **Junctions** tabs show full attribute tables for your datasets:

- If features are selected on the map, only those features are shown
- If no selection, all features are displayed
- Tables are paginated (25 rows per page)
- All non-geometry columns from your original shapefile are included

---

## 11. Network Info

The **Network Info** tab shows summary statistics in three columns:

| Network | Issues | Connectivity |
|---|---|---|
| Total pipes | Total issues found | Source nodes (no incoming pipes) |
| Total nodes | Unique issue types | Dead ends (no outgoing pipes) |
| Connected components | HIGH/MEDIUM/LOW counts | Orphan nodes (completely unconnected) |
| Largest component size | | |
| Virtual nodes (auto-created) | | |

---

## 12. Issue Type Reference

| Issue Type | Severity | Description |
|---|---|---|
| **Adverse Slope** | HIGH | Pipe flows uphill — downstream invert is higher than upstream invert. Includes flat slopes (zero gradient). |
| **Invert Mismatch** | MEDIUM | Pipe invert at a junction differs from the junction's invert elevation beyond the configured tolerance. |
| **Diameter Decrease** | MEDIUM | Pipe diameter decreases downstream without a pump station in between. |
| **Null Invert** | HIGH | Pipe is missing its upstream and/or downstream invert elevation. |
| **Null Diameter** | HIGH | Pipe is missing its diameter value. |
| **Shallow Structure** | LOW | Junction depth (rim minus invert) is less than the minimum depth threshold. |
| **Deep Structure** | LOW | Junction depth (rim minus invert) exceeds the maximum depth threshold. |
| **Dead End** | LOW | Junction has incoming pipes but no outgoing pipes. |
| **Orphan Node** | MEDIUM | Junction has no pipe connections at all. |

### Issue Colors

| Issue Type | Color |
|---|---|
| Adverse Slope | Red (#FF0000) |
| Invert Mismatch | Orange (#FF8C00) |
| Diameter Decrease | Gold (#FFD700) |
| Null Invert / Null Diameter | Purple (#9400D3) |
| Dead End | Blue (#1E90FF) |
| Orphan Node | Grey (#808080) |
| Shallow Structure | Turquoise (#00CED1) |
| Deep Structure | Brown (#8B4513) |

---

## 13. Fix Strategy Reference

### Adverse Slope Fixes

| Strategy | What It Does |
|---|---|
| **Flip Inverts** | Swaps the upstream and downstream invert values. Use when inverts were entered backwards. |
| **Linear Interpolate** | Sets inverts by interpolating between the upstream pipe's DS invert and the downstream pipe's US invert. |
| **Min Slope from Upstream** | Keeps the upstream invert and lowers the downstream invert to achieve the minimum slope. |
| **Min Slope to Downstream** | Keeps the downstream invert and raises the upstream invert to achieve the minimum slope. |
| **Use Ground Slope** | Sets pipe inverts to follow the ground surface slope (rim-to-rim). If ground slope is flatter than minimum, uses minimum slope instead. |

### Invert Mismatch Fixes

| Strategy | What It Does |
|---|---|
| **Adjust Pipe to Match Junction** | Changes the pipe's invert at the junction to match the junction's invert elevation. |
| **Adjust Junction to Match Pipe** | Changes the junction's invert elevation to match the pipe's invert at that junction. |

### Null Invert Fixes

| Strategy | What It Does |
|---|---|
| **From Connected Junction** | Fills missing pipe invert from the adjacent junction's invert elevation. |
| **From Neighbor Pipe** | Fills missing invert from the connected pipe's invert at the shared junction. |
| **Interpolate (Min Slope)** | Calculates missing invert using the known invert plus minimum slope over the pipe length. |
| **Use Ground Slope** | Same as the adverse slope strategy — uses rim elevations to determine pipe inverts. |

### Connectivity Adjustments

When a fix changes inverts at a shared junction, you'll be prompted to adjust connecting pipes. Strategies that trigger this dialog:
- Min Slope from Upstream
- Min Slope to Downstream
- Use Ground Slope
- Linear Interpolate

---

## Tips & Best Practices

1. **Start with the worst issues** — Filter to show only HIGH severity issues first (Adverse Slopes, Null Inverts).
2. **Use box select** to inspect a section of your network, then switch to the Profile View to see the elevation profile.
3. **Check connectivity** — When using slope-based fixes, always review the connectivity dialog to decide whether adjacent pipes should be adjusted.
4. **Undo freely** — Every fix can be undone. The edit ledger tracks all changes.
5. **Adjust min slope** in Settings before applying fixes if your jurisdiction uses a different minimum (e.g., 0.003 m/m for large-diameter pipes).
6. **Review the Profile View** after each fix to visually confirm the pipe inverts look correct.
7. **Unit conversion** — If your data is in imperial units (feet), set the units on the Fields tab before running analysis. All thresholds are converted automatically.

---

*Built with [Solara](https://solara.dev) | Deployed on [Railway](https://railway.app)*
