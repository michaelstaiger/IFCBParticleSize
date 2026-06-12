"""Summarize runtime / settling behavior by taxonomic group and collapsed class."""
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def summarize_runtime(df: pd.DataFrame, group_cols: list[str], runtime_col: str = "RunTime") -> pd.DataFrame:
    work = df.dropna(subset=[runtime_col]).copy()
    return (
        work.groupby(group_cols, dropna=False)[runtime_col]
        .agg(n="count", mean="mean", median="median", sd="std", min="min", max="max")
        .reset_index()
        .sort_values(["n"], ascending=False)
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", default="data/processed/master_particles.parquet", type=Path)
    parser.add_argument("--out-dir", default="data/processed", type=Path)
    parser.add_argument("--runtime-col", default="RunTime")
    args = parser.parse_args()

    df = read_table(args.master)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    by_group = summarize_runtime(df, ["taxonomic_group"], args.runtime_col)
    by_class = summarize_runtime(df, ["taxonomic_group", "collapsed_class"], args.runtime_col)

    by_group.to_csv(args.out_dir / "runtime_summary_by_taxonomic_group.csv", index=False)
    by_class.to_csv(args.out_dir / "runtime_summary_by_collapsed_class.csv", index=False)

    print("Runtime summary by taxonomic group:")
    print(by_group.to_string(index=False))


if __name__ == "__main__":
    main()
