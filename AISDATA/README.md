# AIS Data (`AISDATA/`)

Dataset folder for raw and cleaned AIS CSV files used by the root pipeline.

## Expected Conventions

- Raw input file default: `aisdk-2026-02-05.csv`
- Cleaned output default: `aisdk-2026-02-05.cleaned.csv`

When `AIS_OUTPUT_PATH` is not set, the pipeline derives the output filename from
`AIS_INPUT_FILE`. For example:

```text
AISDATA/aisdk-2026-01-01.csv -> AISDATA/aisdk-2026-01-01.cleaned.csv
```

## Notes

- This folder can contain very large files.
- Root pipeline (`main.py`, backed by `ais_pipeline/pipeline.py`) reads and writes here by default unless overridden with:
  - `AIS_INPUT_FILE`
  - `AIS_OUTPUT_PATH`
- Normal pipeline output is a single CSV file. Set `AIS_OUTPUT_AS_DIRECTORY=1`
  only if you intentionally want Spark's multi-part output directory.

## Related Docs

- [`../README.md`](../README.md) for root pipeline quick start.
- [`../db/README.md`](../db/README.md) for CSV import/query and database scripts.
