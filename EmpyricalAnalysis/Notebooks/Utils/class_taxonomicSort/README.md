# IFCB taxonomic runtime pipeline starter

This starter project turns one or more IFCB classifier-output CSVs into a reusable particle-level analysis table.

## Design

- `data/raw/`: original CSV files, unchanged.
- `config/class_taxonomy_map.csv`: editable lookup table from model class name to collapsed class and broad taxonomic group.
- `data/processed/master_particles.parquet`: derived master table for analysis.
- `scripts/01_build_master_dataset.py`: combines raw files and appends metadata/class assignments.
- `scripts/02_runtime_summary.py`: produces first-pass runtime summaries.

## Why this architecture

The raw files remain the source of truth. The master table is rebuilt whenever you edit taxonomy assignments, add files, or change processing rules. This makes the workflow reproducible and easier to debug than manually appending files.

## Run

From the project folder:

```bash
python scripts/01_build_master_dataset.py
python scripts/02_runtime_summary.py
```


For real though use this (I am not chat this is me mike staiger):
```bash
 python scripts/01_build_master_dataset.py   --raw-dir data/raw   --map config/class_taxonomy_map.csv   --output data processed/master_particles.parquet
```
this creates a parquret file very similar to csv just faster for long column files like these will be


The master table keeps the original metadata columns, including `RunTime`, `ADCtime`, `VolumeAnalyzed`, ROI dimensions/position, and `pid`. It adds:

- `source_file`
- `sample_datetime`
- `best_class`
- `best_score`
- `collapsed_class`
- `taxonomic_group`
- `score_sum_<taxonomic_group>` columns

## Recommended next additions

1. Add sample-level metadata, such as station, depth, cruise, treatment, bottle, or deployment ID.
2. Add filters for low-confidence classifications, artifacts, detritus, or zero-ROI rows.
3. Add plotting/statistical scripts for comparing runtime distributions among groups.
4. Version-control `config/class_taxonomy_map.csv` because changes there affect all downstream analyses.
