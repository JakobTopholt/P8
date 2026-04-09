# Pipeline Tools (`ais_pipeline/tools/`)

Utility and exploratory scripts related to AIS processing workflows.

## Files

- `filter_csv.py`: simple MMSI-based CSV filter helper.
- `validate_cleaning.py`: validates rows removed by the outlier step and reports per-MMSI impact.
- `check_gaps.py`: inspects largest consecutive trajectory gaps in cleaned output and flags suspicious ones.

## Notes

- These scripts are optional and not part of the primary Spark cleaning pipeline.
- Core DB operational scripts live in [`../../db/README.md`](../../db/README.md).
