# Environment Setup (`ais_pipeline/environment/`)

Helpers for configuring Java, Hadoop, and Spark/PySpark runtime variables before
running the root AIS cleaning pipeline (`main.py` -> `ais_pipeline/pipeline.py`).

## Files

- `java_environment.py`: discovers/activates usable Java runtime (`JAVA_HOME`, `PATH`).
- `hadoop_environment.py`: discovers/activates Hadoop home and native library paths.
- `spark_environment.py`: configures Spark temp/conf and PySpark Python executables.
- `spark_conf/log4j2.properties`: local Spark logging configuration.

## How It Is Used

`ais_pipeline/pipeline.py` (invoked by `main.py`) calls these setup functions before starting Spark:

- `configure_java_environment(project_dir, verbose=True)`
- `configure_hadoop_environment(project_dir, verbose=True)`
- `configure_pyspark_python()`
- `configure_spark_environment(project_dir)`

## Pipeline Runtime Environment Variables

Used by the AIS cleaning pipeline:

- `AIS_INPUT_FILE` (default `AISDATA/aisdk-2026-02-05.csv`)
- `AIS_OUTPUT_PATH` (default `AISDATA/aisdk-2026-02-05.cleaned.csv`)
- `SPARK_LOCAL_CORES` (default `4`)
- `SPARK_SHUFFLE_PARTITIONS` (default `64`)
- `SPARK_INPUT_PARTITION_MB` (default `64`)
- `SPARK_OUTPUT_PARTITIONS` (default `1`)
- `PRINT_ROW_COUNT` (`1` enables final row count print)

Example:

```bash
SPARK_LOCAL_CORES=2 \
SPARK_SHUFFLE_PARTITIONS=96 \
SPARK_INPUT_PARTITION_MB=32 \
SPARK_OUTPUT_PARTITIONS=4 \
python main.py
```
