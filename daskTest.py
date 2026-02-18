import dask.dataframe as dd
import time

start_time = time.time()

input_file = "aisdk-2026-02-05.csv"
output_file = "aisdk-2026-02-05.cleaned.csv"

print("Loading CSV file with Dask...")
df = dd.read_csv(input_file, dtype={'Cargo type': 'str', 'ETA': 'str', 'Name': 'str'})

print("Removing duplicates...")
df_cleaned = df.drop_duplicates()

print(f"Saving cleaned data to {output_file}...")
df_cleaned.to_csv(output_file, single_file=True, index=False)

elapsed_time = time.time() - start_time

print("=" * 50)
print("RESULTS:")
print(f"Time elapsed: {elapsed_time:.2f} seconds")
print("=" * 50)