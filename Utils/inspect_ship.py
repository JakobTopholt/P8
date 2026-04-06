import csv

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
for i, r in enumerate(rows):
    lat = float(r["Latitude"])
    lon = float(r["Longitude"])
    sog = float(r["SOG"])
    print(f"  [{i:3d}] ts={r['# Timestamp']}  lat={lat:9.4f}  lon={lon:10.4f}  SOG={sog:6.1f}  ship_type={r.get('Ship type','?')}")

# Also check the raw data before cleaning
print("\n\n--- Raw data (original CSV) ---")
rawfile = "AISDATA/aisdk-2026-02-05.csv"
raw_rows = []
with open(rawfile, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["MMSI"] == target_mmsi:
            raw_rows.append(row)

print(f"MMSI {target_mmsi}: {len(raw_rows)} total points in raw data")
for i, r in enumerate(raw_rows):
    lat = float(r["Latitude"])
    lon = float(r["Longitude"])
    sog = float(r["SOG"])
    print(f"  [{i:3d}] ts={r['# Timestamp']}  lat={lat:9.4f}  lon={lon:10.4f}  SOG={sog:6.1f}  type_mobile={r.get('Type of mobile','?')}")
