# AIS Cleaning Pipeline (`ais_pipeline/`)

This package contains the root Spark-based AIS cleaning pipeline code.

## Entry Points

- Root wrapper: `main.py`
- Module entrypoint: `python -m ais_pipeline`
- Direct module path: `python -m ais_pipeline.pipeline`

Both run the same `run()` function.

## Package Layout

- `pipeline.py`: orchestrates Spark session setup, environment bootstrapping, and step execution.
- `environment/`: Java/Hadoop/Spark runtime bootstrap helpers used by the pipeline.
- `steps/`: individual transformation modules used by the pipeline.
  - `remove_duplicates.py`
  - `trim_stationary.py`
  - `ship_type.py`
  - `remove_shiptypes.py`
  - `remove_outliers.py`
  - `trim_moving.py` (currently not invoked by the default `run()` pipeline)
- `tools/`: utility and exploratory scripts related to the AIS workflow.

## Related Docs

- [`environment/README.md`](environment/README.md) for Java/Hadoop/Spark setup helpers.
- [`../AISDATA/README.md`](../AISDATA/README.md) for input/output dataset conventions.
