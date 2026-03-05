import csv
import argparse


def filter_csv(input_file="AISDATA/aisdk-2026-02-05.csv", output_file= "output2.csv", id_column="MMSI", ids_to_keep={"210071000"}):
    """Filter a CSV file based on a list of IDs."""
    with open(input_file, "r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames

        if id_column not in fieldnames:
            print(f"Error: Column '{id_column}' not found in CSV. Available columns: {fieldnames}")
            return

        with open(output_file, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            kept = 0
            total = 0
            for row in reader:
                total += 1
                if row[id_column] in ids_to_keep:
                    writer.writerow(row)
                    kept += 1

    print(f"Filtered {kept}/{total} rows. Output written to '{output_file}'.")


def load_ids_from_file(filepath):
    """Load IDs from a text file (one ID per line)."""
    with open(filepath, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


if __name__ == "__main__":
    filter_csv()