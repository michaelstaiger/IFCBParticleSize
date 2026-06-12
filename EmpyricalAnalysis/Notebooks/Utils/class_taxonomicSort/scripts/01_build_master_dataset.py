"""
Build an analysis-ready IFCB particle table from one or more classifier CSVs.

Input assumption:
- Each raw CSV has metadata columns followed by class probability columns.
- In the current files, metadata ends at `pid`; all columns after `pid` are class scores.

Output:
- particle-level table with original metadata, best class assignment, collapsed class,
  taxonomic group, and optional summed group probability scores.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import re
import pandas as pd


def parse_sample_datetime_from_filename(path: Path) -> str | None:
    """Extract datetime like D20230727T030526 from IFCB filenames."""
    match = re.search(r"D(\d{8}T\d{6})", path.name)
    if not match:
        return None
    return pd.to_datetime(match.group(1), format="%Y%m%dT%H%M%S").isoformat()


def get_metadata_and_class_columns(df: pd.DataFrame, class_map: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Prefer known class names from the taxonomy map; fall back to columns after pid."""
    mapped_classes = [c for c in class_map["class_name"].tolist() if c in df.columns]
    if mapped_classes:
        class_cols = mapped_classes
    elif "pid" in df.columns:
        pid_idx = df.columns.get_loc("pid")
        class_cols = list(df.columns[pid_idx + 1 :])
    else:
        raise ValueError("Could not infer class score columns. Add class names to config/class_taxonomy_map.csv.")
    metadata_cols = [c for c in df.columns if c not in class_cols]
    return metadata_cols, class_cols


def process_one_file(path: Path, class_map: pd.DataFrame, include_group_scores: bool = False) -> pd.DataFrame:
    df = pd.read_csv(path)
    metadata_cols, class_cols = get_metadata_and_class_columns(df, class_map)

    scores = df[class_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    best_class = scores.idxmax(axis=1)
    best_score = scores.max(axis=1)

    lookup = class_map.set_index("class_name")
    out = df[metadata_cols].copy()
    out.insert(0, "source_file", path.name)
    out.insert(1, "sample_datetime", parse_sample_datetime_from_filename(path))
    out["best_class"] = best_class.values
    out["best_score"] = best_score.values
    out["collapsed_class"] = out["best_class"].map(lookup["collapsed_class"])
    out["taxonomic_group"] = out["best_class"].map(lookup["taxonomic_group"])

    missing = out["taxonomic_group"].isna()
    if missing.any():
        out.loc[missing, "taxonomic_group"] = "unmapped"
        out.loc[missing, "collapsed_class"] = out.loc[missing, "best_class"]

    if include_group_scores:
        group_lookup = lookup["taxonomic_group"].to_dict()
        score_groups = {}
        for c in class_cols:
            group = group_lookup.get(c, "unmapped")
            score_groups.setdefault(group, []).append(c)
        for group, cols in score_groups.items():
            safe_group = re.sub(r"[^A-Za-z0-9_]+", "_", group)
            out[f"score_sum_{safe_group}"] = scores[cols].sum(axis=1)

    return out


def build_master(raw_dir: Path, map_path: Path, output_path: Path, file_glob: str) -> pd.DataFrame:
    class_map = pd.read_csv(map_path)
    required = {"class_name", "collapsed_class", "taxonomic_group"}
    if not required.issubset(class_map.columns):
        raise ValueError(f"Taxonomy map must contain columns: {sorted(required)}")

    files = sorted(raw_dir.glob(file_glob))
    if not files:
        raise FileNotFoundError(f"No files matched {raw_dir / file_glob}")

    pieces = [process_one_file(path, class_map) for path in files]
    master = pd.concat(pieces, ignore_index=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".parquet":
        master.to_parquet(output_path, index=False)
    else:
        master.to_csv(output_path, index=False)
    return master


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default="data/raw", type=Path)
    parser.add_argument("--map", default="config/class_taxonomy_map.csv", type=Path)
    parser.add_argument("--output", default="data/processed/master_particles.parquet", type=Path)
    parser.add_argument("--glob", default="*.csv")
    args = parser.parse_args()

    master = build_master(args.raw_dir, args.map, args.output, args.glob)
    print(f"Wrote {len(master):,} rows x {len(master.columns):,} columns to {args.output}")
    print(master["taxonomic_group"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
