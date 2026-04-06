import csv

csvfile = "AISDATA/aisdk-2026-02-05.cleaned.csv/part-00000-17ad57f7-c7cc-4a0b-b920-f3bb1f8ee7b6-c000.csv"
rows_out = []
total = 0

with open(csvfile, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        total += 1
        lat = float(row["Latitude"])
        lon = float(row["Longitude"])
        # Far from Denmark/Scandinavia
        if lat < 40 or lat > 70 or lon < -10 or lon > 25:
            rows_out.append(row)

print(f"Total rows: {total}")
print(f"Points outside Denmark bbox: {len(rows_out)}")
print()

# Group by MMSI to see which ships
from collections import Counter, defaultdict
mmsi_counts = Counter()
mmsi_rows = defaultdict(list)
for r in rows_out:
    mmsi_counts[r["MMSI"]] += 1
    mmsi_rows[r["MMSI"]].append(r)

print(f"Unique MMSIs with far-away points: {len(mmsi_counts)}")
print()

for mmsi, cnt in mmsi_counts.most_common(20):
    print(f"MMSI {mmsi}: {cnt} points outside bbox")
    for r in mmsi_rows[mmsi][:5]:
        lat = float(r["Latitude"])
        lon = float(r["Longitude"])
        print(f"   lat={lat:9.4f}  lon={lon:10.4f}  SOG={r['SOG']:>6}  ts={r['# Timestamp']}  ship_type={r.get('Ship type','?')}")
    print()
