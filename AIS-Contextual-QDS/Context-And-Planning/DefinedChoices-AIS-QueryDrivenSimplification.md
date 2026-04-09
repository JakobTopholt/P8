# Defined Choices

## Project
**Geofence-aware query-driven simplification for Danish AIS**

**Purpose**
Lock the implementation choices for the MVP so the project can start without reopening scope questions.

**Status rule**
Anything in this document is treated as fixed unless a later experiment clearly shows it must change.

---

## 1. Data source and storage

### 1.1 Source data
**Locked choice**
- Main source: historical AIS data provided by the Danish Maritime Authority.
- Data is already cleaned in preprocessing for duplicate removal and outlier removal.
- Data is imported into SQL tables in a PostGIS database.

**Consequence**
- No additional external AIS source will be used in the MVP.
- The raw point tables in PostGIS are treated as the ground-truth source for all query labels and trajectory construction.

### 1.2 Working data tables
**Locked choice**
The MVP will use these logical table groups:
- `ais_points_cleaned`: cleaned AIS point records
- `trajectories_raw`: trajectories built from cleaned AIS points
- `context_zones`: maritime geofence polygons
- `context_corridors`: corridor polygons
- `trajectories_simplified_*`: one output table per simplification method and budget

**Consequence**
- The project will be built around reproducible SQL + Python scripts that read/write these tables.

---

## 2. Study scope

### 2.1 Study region
**Locked choice**
- Region: **Great Belt / Storebælt study area**
- Region representation: one fixed study polygon stored in PostGIS

**Why this choice**
- Constrained waterway
- High relevance for corridor-style traffic
- Easier to define meaningful corridor and zone queries than open water
- Simpler than trying to model several different Danish traffic environments at once

**Consequence**
- The MVP will not start with all Danish waters.
- All trajectories are clipped/selected to the Great Belt study polygon.

### 2.2 Vessel subset
**Locked choice**
- Vessel class: **cargo vessels only** for the MVP

**Why this choice**
- Simpler than mixing vessel behaviors
- Less route repetition noise than ferries
- More realistic transit behavior for corridor and zone-entry queries

**Consequence**
- Other vessel classes are excluded from the first phase.
- Expansion to ferry/tanker/etc. is postponed until after the MVP works.

### 2.3 Time span
**Locked choice**
- Initial working window: **4 consecutive weeks**

**Why this choice**
- Large enough to contain repeated traffic patterns
- Small enough to debug and inspect manually

**Consequence**
- The first implementation and all debugging runs use only this 4-week slice.
- Larger time ranges are postponed.

---

## 3. Trajectory construction

### 3.1 Ordering
**Locked choice**
- Points are ordered by `mmsi, timestamp`.

### 3.2 Trajectory splitting rules
**Locked choice**
A new trajectory starts when any of the following is true:
- Time gap between consecutive points is greater than **30 minutes**
- Implied speed between consecutive points exceeds a conservative sanity threshold after cleaning
- Vessel leaves the study region and does not reappear within the same continuous track

### 3.3 Minimum trajectory size
**Locked choice**
- Minimum valid trajectory length: **20 points**

### 3.4 Stationary filtering
**Locked choice**
- No separate stationary-period removal in the MVP
- If stationary behavior exists inside the chosen region, it remains part of the trajectory

**Why**
- Removing stationary behavior adds another source of ambiguity
- Zone-entry queries can still be meaningful for slow or temporarily stationary vessels

### 3.5 Endpoint policy
**Locked choice**
- Every simplification method must always keep the **first and last point** of each trajectory

---

## 4. Context layers

### 4.1 Coastline / land mask
**Locked choice**
- Use one coastline/land polygon layer for sanity checks and optional feature computation

**Usage in MVP**
- Validate spatial overlays
- Count obviously invalid path artifacts
- Optional distance-to-land feature

### 4.2 Geofence zones
**Locked choice**
Use a **small set of fixed geofence polygons** inside the study region:
- `zone_port_approach`
- `zone_anchor_or_waiting_area`
- `zone_narrow_passage_control`

**Rule**
- Total number of geofence zones in MVP: **3**

**Why**
- Small enough to inspect manually
- Enough variety to test context-aware behavior

### 4.3 Corridor layer
**Locked choice**
- Use **one corridor polygon** representing the main transit lane through the study region
- If the source geometry is a line, convert it once to a polygon buffer and freeze it

**Rule**
- Only one corridor is used in the MVP

---

## 5. Query workload

The project remains query-driven. The target of simplification is preserving query answers under storage compression.

### 5.1 Primary query 1: zone-entry query
**Locked definition**
- Query form: “Which trajectories entered zone Z during time interval T?”
- Output unit: **trajectory-level yes/no**

**Semantics**
A trajectory counts as entering zone Z if:
- It is outside Z before the entry event, and
- It later has at least one point inside Z, or one segment that crosses into Z

**Additional rules**
- Starting inside the zone does **not** count as an entry
- Boundary-only touching does **not** count as entry unless the segment crosses from outside to inside
- Multiple entries are collapsed to one trajectory-level positive result

### 5.2 Primary query 2: corridor-membership query
**Locked definition**
- Query form: “Which trajectories passed through corridor C during time interval T?”
- Output unit: **trajectory-level yes/no**

**Semantics**
A trajectory counts as corridor-positive if:
- At least one point lies inside the corridor polygon, or
- A trajectory segment intersects the corridor polygon

**Additional rules**
- Boundary touch alone counts as corridor-positive only if the segment intersects the corridor polygon beyond a single numerical artifact
- No minimum dwell time is required in the MVP

### 5.3 Query scope
**Locked choice**
- The MVP optimizes only for these two query families
- Route similarity queries are postponed until after the MVP is stable

---

## 6. Ground truth labeling

### 6.1 Label source
**Locked choice**
- Ground truth query labels are generated from the **raw trajectories** in `trajectories_raw`

### 6.2 Spatial logic
**Locked choice**
- Labeling uses both:
  - point-in-polygon checks
  - segment-polygon intersection checks

**Why**
- Point-only logic misses crossings between sparse AIS samples
- Segment-aware labeling is still simple enough to implement and trust

### 6.3 Time logic
**Locked choice**
- The MVP uses trajectory membership within the selected 4-week time window
- Event timestamp precision is **not** a primary evaluation target in the MVP

**Consequence**
- The query task is classification-oriented, not event-timing-oriented

---

## 7. Compression policy

### 7.1 Budget type
**Locked choice**
- Compression is applied **per trajectory**
- Each trajectory is simplified to a target retained-point ratio

**Why**
- Easier to compare methods fairly in the MVP
- Simpler than global budget allocation across all trajectories

### 7.2 Retained-point budgets
**Locked choice**
Evaluate these retained-point ratios:
- **10%**
- **20%**
- **30%**
- **40%**
- **50%**

### 7.3 Output rule
**Locked choice**
- Simplified trajectories must preserve point order
- No synthetic points are generated in the MVP
- All retained points must come from the original trajectory

---

## 8. Methods included in the MVP

### 8.1 Baselines
**Locked choice**
The MVP includes exactly these baseline families:
1. **Uniform subsampling**
2. **Geometry-based simplification** (Douglas-Peucker or equivalent)
3. **Query-driven simplification without AIS context**
4. **Query-driven simplification with AIS context**

### 8.2 Model complexity
**Locked choice**
- No GNN, no diffusion, and no reproduction of full MLSimp in the MVP

**Why**
- The first goal is to prove the query-driven and context-aware effect with a controllable method
- Heavy models are deferred until the simpler approach is benchmarked and trusted

---

## 9. Point scoring design for the MVP

### 9.1 Base score
**Locked choice**
The initial non-context query-driven score combines:
- simple geometric importance (turning / local deviation proxy)
- query relevance for preserving zone-entry and corridor-membership results

### 9.2 Context-aware additions
**Locked choice**
The context-aware method adds only these AIS-relevant terms:
- **boundary proximity term**: higher score near zone or corridor boundaries
- **transition term**: higher score near entry/exit transitions for zones and corridor

### 9.3 Excluded features
**Locked choice**
The MVP will not include:
- weather
- bathymetry
- vessel-to-vessel interaction
- anomaly labels
- learned embeddings
- dynamic context layers

---

## 10. Features to compute per point

**Locked feature set**
For each point, compute:
- `inside_zone_id` (nullable)
- `inside_corridor` (boolean)
- `distance_to_nearest_zone_boundary`
- `distance_to_corridor_boundary_or_centerline`
- `distance_to_land` (optional but allowed)
- `zone_transition_flag`
- `corridor_transition_flag`
- simple local turn/deviation proxy

**Rule**
- No additional feature families are added before the first benchmark is complete

---

## 11. Coordinate system and distance handling

### 11.1 CRS
**Locked choice**
- All distance-based operations must be done in a **projected CRS appropriate for Denmark**
- Lat/lon storage is allowed, but metric distance calculations must not be performed directly in geographic coordinates

### 11.2 Corridor geometry
**Locked choice**
- Corridor logic must operate on a polygon layer
- If the corridor starts as a line, buffer it once and store the buffered result

---

## 12. Evaluation setup

### 12.1 Development subset
**Locked choice**
- Create one small dev subset of **200–500 trajectories** for rapid debugging and manual inspection

### 12.2 Evaluation subset
**Locked choice**
- Create one held-out evaluation subset separated from the dev subset by time or trajectory IDs
- Tuning happens only on the dev subset

### 12.3 Primary metrics
**Locked choice**
Measure for each method and budget:
- zone-entry precision
- zone-entry recall
- zone-entry F1
- corridor-membership precision
- corridor-membership recall
- corridor-membership F1
- retained-point ratio
- simplification runtime

### 12.4 Sanity metrics
**Locked choice**
Also track:
- false zone crossing count created by simplification
- missed zone crossing count caused by simplification
- obviously invalid spatial artifacts relative to land mask

### 12.5 Inspection rule
**Locked choice**
- Every major benchmark run must include manual inspection of representative trajectories
- Aggregate metrics alone are not considered sufficient evidence

---

## 13. Implementation stack

### 13.1 Core stack
**Locked choice**
- PostGIS for spatial storage and spatial query support
- Python for preprocessing scripts, simplification, experiment running, and plotting
- SQL for data extraction, labeling, and feature generation where sensible

### 13.2 Reproducibility
**Locked choice**
- Every experiment run must be driven by a config containing:
  - time window
  - vessel class
  - region polygon
  - budgets
  - method name
  - feature toggles

---

## 14. Success criteria for the MVP

**Locked success target**
The MVP is considered successful if:
1. The raw query engine is trusted on the chosen region
2. All 4 methods run at all 5 budgets
3. The context-aware query-driven method improves at least one primary query metric over the context-unaware query-driven baseline at the same budget
4. The improvement survives manual inspection
5. The method remains simple and reproducible

---

## 15. Deferred choices

These are explicitly postponed and are **not** allowed to expand the MVP scope yet:
- second vessel class
- second study region
- route similarity query
- global cross-trajectory budget allocation
- GNN or diffusion models
- streaming/online simplification
- weather and other dynamic context
- vessel interaction modeling
- paper/publishing framing

---

## 16. Final rule

If a new idea does not directly improve or clarify:
- zone-entry query fidelity,
- corridor-membership query fidelity, or
- simplification under fixed retained-point budgets,

then it is out of scope for the MVP.

