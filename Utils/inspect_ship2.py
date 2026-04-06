import csv
import math

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

csvfile = "AISDATA/aisdk-2026-02-05.cleaned.csv/part-00000-17ad57f7-c7cc-4a0b-b920-f3bb1f8ee7b6-c000.csv"
target_mmsi = "232003652"
rows = []

with open(csvfile, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["MMSI"] == target_mmsi:
            rows.append(row)

print(f"MMSI {target_mmsi}: {len(rows)} total points in cleaned data")
print()

# Show rows 40-60 with distance to prev
for i in range(max(0, 40), min(len(rows), 60)):
    r = rows[i]
    lat = float(r["Latitude"])
    lon = float(r["Longitude"])
    sog = float(r["SOG"])
    
    dist_prev = ""
    time_diff = ""
    if i > 0:
        prev = rows[i-1]
        plat = float(prev["Latitude"])
        plon = float(prev["Longitude"])
        d = haversine(plat, plon, lat, lon)
        dist_prev = f"  dist_prev={d:10.1f}km"
    
    flag = ""
    if lat < 40 or lat > 70 or lon < -10 or lon > 25:
        flag = "  *** FAR AWAY ***"
    
    print(f"  [{i:3d}] ts={r['# Timestamp']}  lat={lat:9.4f}  lon={lon:10.4f}  SOG={sog:6.1f}{dist_prev}{flag}")

print()
print("--- Key insight: why are the far-away points consecutive? ---")
print("The outlier detector keeps a point if reachable from EITHER prev or next.")
print("Two consecutive far-away points are 'reachable from each other' (same location),")
print("so each one is kept because it's reachable from its partner.")
