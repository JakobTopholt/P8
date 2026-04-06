"""Investigate ships with large MaxGap that the outlier detector doesn't reduce."""
import csv
import math
from collections import defaultdict

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# Ships with big gap and 0% reduction
targets = [
    "255915614",  # 228 km, 0%
    "226000000",  # 210 km, 0%
    "538004060",  # 198 km, 0%
    "636018825",  # 194 km, 0%
    "240633000",  # 176 km, 0%, but 62 deletions
    "354962000",  # 174 km, 0%
    "314617000",  # 174 km, 0%
    "667001645",  # 171 km, 0%
    "414706000",  # 141 km, 0%
    "235094449",  # 140 km, 0%
]

csvfile = "AISDATA/aisdk-2026-02-05.cleaned.csv/part-00000-17ad57f7-c7cc-4a0b-b920-f3bb1f8ee7b6-c000.csv"

# Find actual cleaned file
import os
cleaned_dir = "AISDATA/aisdk-2026-02-05.cleaned.csv"
for f in os.listdir(cleaned_dir):
    if f.startswith("part-") and f.endswith(".csv"):
        csvfile = os.path.join(cleaned_dir, f)
        break

print(f"Reading: {csvfile}\n")

ship_rows = defaultdict(list)
with open(csvfile, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["MMSI"] in targets:
            ship_rows[row["MMSI"]].append(row)

for mmsi in targets:
    rows = ship_rows.get(mmsi, [])
    if not rows:
        print(f"MMSI {mmsi}: NOT FOUND in cleaned data\n")
        continue

    print(f"{'='*100}")
    print(f"MMSI {mmsi}: {len(rows)} points in cleaned data")

    # Find the max gap
    max_gap = 0
    max_gap_idx = 0
    gaps = []
    for i in range(1, len(rows)):
        lat1 = float(rows[i-1]["Latitude"])
        lon1 = float(rows[i-1]["Longitude"])
        lat2 = float(rows[i]["Latitude"])
        lon2 = float(rows[i]["Longitude"])
        d = haversine(lat1, lon1, lat2, lon2)
        gaps.append(d)
        if d > max_gap:
            max_gap = d
            max_gap_idx = i

    def fmt_d(km):
        return f"{km*1000:.1f} m" if km < 1 else f"{km:.3f} km"

    print(f"Max gap: {fmt_d(max_gap)} between point [{max_gap_idx-1}] and [{max_gap_idx}]")

    # Time gap at max distance
    ts_before = rows[max_gap_idx-1]["# Timestamp"]
    ts_after = rows[max_gap_idx]["# Timestamp"]

    # Show context: 3 points before and after the max gap
    start = max(0, max_gap_idx - 3)
    end = min(len(rows), max_gap_idx + 4)

    print(f"\n  Points around the max gap:")
    for i in range(start, end):
        r = rows[i]
        lat = float(r["Latitude"])
        lon = float(r["Longitude"])
        sog = float(r["SOG"])
        marker = " <<< MAX GAP HERE" if i == max_gap_idx else ""
        dist_prev = ""
        time_diff = ""
        if i > 0:
            plat = float(rows[i-1]["Latitude"])
            plon = float(rows[i-1]["Longitude"])
            d = haversine(plat, plon, lat, lon)
            dist_prev = f"  dist_prev={fmt_d(d):>12}"

            # Parse timestamps for time diff
            ts_cur = r["# Timestamp"]
            ts_prv = rows[i-1]["# Timestamp"]
            # simple time diff from ISO timestamps
            from datetime import datetime
            try:
                t1 = datetime.fromisoformat(ts_prv)
                t2 = datetime.fromisoformat(ts_cur)
                dt = (t2 - t1).total_seconds()
                hours = dt / 3600
                time_diff = f"  dt={dt:.0f}s ({hours:.2f}h)"
            except:
                time_diff = ""

        print(f"  [{i:4d}] ts={r['# Timestamp']}  lat={lat:9.4f}  lon={lon:10.4f}  SOG={sog:5.1f}{dist_prev}{time_diff}{marker}")

    # Count gaps > 10km
    big_gaps = [(i+1, g) for i, g in enumerate(gaps) if g > 10]
    print(f"\n  Gaps > 10 km: {len(big_gaps)}")
    for idx, g in big_gaps[:5]:
        r_before = rows[idx-1]
        r_after = rows[idx]
        from datetime import datetime
        try:
            t1 = datetime.fromisoformat(r_before["# Timestamp"])
            t2 = datetime.fromisoformat(r_after["# Timestamp"])
            dt_h = (t2 - t1).total_seconds() / 3600
        except:
            dt_h = -1
        sog_b = float(r_before["SOG"])
        sog_a = float(r_after["SOG"])
        max_sog = max(sog_b, sog_a)
        # What speed would be needed?
        needed_kn = g / (dt_h * 1.852) if dt_h > 0 else float('inf')
        print(f"    Gap [{idx-1}→{idx}]: {fmt_d(g):>12}  time={dt_h:.2f}h  SOG_before={sog_b:.1f}  SOG_after={sog_a:.1f}  needed={needed_kn:.1f}kn vs max_sog={max_sog:.1f}kn")

    print()
