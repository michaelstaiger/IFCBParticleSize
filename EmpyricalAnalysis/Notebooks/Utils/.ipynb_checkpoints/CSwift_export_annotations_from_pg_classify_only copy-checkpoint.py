#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export latest HUMAN classification per ROI (classify_classification)
and aggregated HUMAN tags (classify_tag) for specific datasets.

Output columns:
  pid, classname, raw_tag, class_tag, last_event_time, created_at, dataset

Filters:
  --datasets <name> (repeat)     # if omitted, uses HABON_DATASETS default list
  --datasets-file <path>         # newline-delimited dataset names
  --bins-file <path>             # restrict to bins/PIDs
  --tag <substr> (repeat)        # substring match on aggregated tags
  --user <username> (repeat)     # restrict by classification author
  --since/--until <ISO>
  --limit <N>
  --emit-pids
"""

from __future__ import annotations
import argparse, os, re
from pathlib import Path
from typing import List, Optional, Set
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

# ---------- Postgres connection ----------
PG_HOST = "amvets.whoi.edu"
PG_DB   = "django_db"
PG_USER = "read_user"
PG_PASS = "Amvetsread1323"

# ---------- Default dataset allow-list ----------
HABON_DATASETS = [
    'NESLTER_broadscale','SKQ202309T','SKQ202310S','SKQ202311S','SKQ202312S','SKQ2024_12S',
    'SurfsideBeach','afsc','arctic','azmp','baystatehatchery','bowdoin_class','buddinlet',
    'dmc','dy169','dy184','dy184_discrete','ecoa','fiddlers','globalhab','gom','gsodock',
    'harpswell','hly2401','jamestown','lombos','mdibl','mook','mvco','nauset','newpass',
    'oceanalliance','old_fort_pond','pie','radbot','radbot_ios','radbot_jeffreys_basin',
    'radbot_mvco','tangosund','tioga','vimspier',
]

PID_BIN_ROI = re.compile(r"^(?P<bin>.+)_(?P<roi>\d+)$")

def load_list_file(path: str) -> List[str]:
    vals: List[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if s and not s.startswith("#"):
                vals.append(s)
    return vals

def load_bins_from_file(path: str, limit: Optional[int] = None) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"--bins-file not found: {path}")
    bins: Set[str] = set()
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            token = line.split(",", 1)[0].strip()
            m = PID_BIN_ROI.match(token)
            bins.add(m.group("bin") if m else token)
            if limit and len(bins) >= limit:
                break
    return sorted(bins)

def build_sql(has_bins: bool, has_tag: bool, has_user: bool,
              has_since: bool, has_until: bool, limit: Optional[int]) -> sa.sql.elements.TextClause:
    # dataset slug = last path component of classify_timeseries.url
    TAGS_CTE = """
    WITH tag_last AS (
      SELECT
        t.bin,
        t.roi,
        tl.name  AS taglabel,
        MAX(t.time) AS tag_time
      FROM public.classify_tag t
      JOIN public.classify_taglabel tl ON tl.id = t.tag_id
      JOIN public.classify_timeseries ts ON ts.id = t.timeseries_id::uuid
      WHERE t.negation = FALSE
        AND regexp_replace(ts.url, '.*/([^/]+)/$', '\\1') = ANY(:ds_list)
      GROUP BY t.bin, t.roi, tl.name
    ),
    lt AS (
      SELECT
        bin,
        roi,
        STRING_AGG(taglabel, '||' ORDER BY tag_time DESC) AS raw_tags,
        MAX(tag_time) AS latest_tag_time
      FROM tag_last
      GROUP BY bin, roi
    )
    """

    class_filters = ["regexp_replace(ts.url, '.*/([^/]+)/$', '\\1') = ANY(:ds_list)"]
    if has_bins:
        class_filters.append("c.bin = ANY(:bins)")
    if has_user:
        class_filters.append("au.username = ANY(:user_list)")
    if has_since:
        class_filters.append("COALESCE(c.verification_time, c.time) > :since")
    if has_until:
        class_filters.append("COALESCE(c.verification_time, c.time) <= :until")
    class_where = "WHERE " + " AND ".join(class_filters)

    CLASS_CTE = f"""
    , base_class AS (
      SELECT
        c.id,
        c.bin,
        c.roi,
        c.user_id,
        c.classification_id,
        c.time,
        c.verification_time,
        COALESCE(c.verification_time, c.time) AS class_event_time,
        regexp_replace(ts.url, '.*/([^/]+)/$', '\\1') AS dataset
      FROM public.classify_classification c
      JOIN public.classify_timeseries ts ON ts.id = c.timeseries_id::uuid
      LEFT JOIN public.auth_user au ON au.id = c.user_id
      {class_where}
    ),
    ranked_class AS (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY bin, roi
               ORDER BY class_event_time DESC, id DESC
             ) AS rn
      FROM base_class
    )
    """

    tag_like_clause = "AND (lt.raw_tags IS NOT NULL AND lt.raw_tags ILIKE ANY(:tag_like))" if has_tag else ""
    limit_clause = f"LIMIT {int(limit)}" if (limit and limit > 0) else ""

    SQL = f"""
    {TAGS_CTE}
    {CLASS_CTE}
    SELECT
      rc.bin,
      rc.roi,
      (rc.bin || '_' || LPAD(rc.roi::text, 5, '0')) AS pid,
      cl.name AS classname,
      lt.raw_tags AS raw_tag,
      rc.time AS created_at,
      GREATEST(rc.class_event_time, COALESCE(lt.latest_tag_time, rc.class_event_time)) AS last_event_time,
      rc.dataset
    FROM ranked_class rc
    JOIN public.classify_classlabel cl ON cl.id = rc.classification_id
    LEFT JOIN lt ON lt.bin = rc.bin AND lt.roi = rc.roi
    WHERE rc.rn = 1
      {tag_like_clause}
    ORDER BY rc.bin, rc.roi
    {limit_clause};
    """
    return text(SQL)

def main():
    ap = argparse.ArgumentParser(description="Export latest human classification + aggregated tags (dataset-filtered).")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--datasets", action="append", help="Dataset name(s); repeatable")
    ap.add_argument("--datasets-file", type=str, help="Text file with dataset names")
    ap.add_argument("--bins-file", type=str, help="Restrict to bins/PIDs (optional)")
    ap.add_argument("--tag", action="append", help="Filter by tag substring (optional)")
    ap.add_argument("--user", action="append", help="Filter by classification author username (optional)")
    ap.add_argument("--since", type=str, help="Lower bound ISO timestamp (optional)")
    ap.add_argument("--until", type=str, help="Upper bound ISO timestamp (optional)")
    ap.add_argument("--limit", type=int, help="Limit for debugging (optional)")
    ap.add_argument("--emit-pids", action="store_true", help="Also write PID list alongside the CSV")
    args = ap.parse_args()

    # Dataset list
    ds: List[str] = []
    if args.datasets:
        ds.extend(args.datasets)
    if args.datasets_file:
        ds.extend(load_list_file(args.datasets_file))
    if not ds:
        ds = HABON_DATASETS[:]
    ds = sorted(set(s.strip() for s in ds if s.strip()))
    if not ds:
        raise SystemExit("No datasets provided or parsed.")

    # Filters
    bins = load_bins_from_file(args.bins_file) if args.bins_file else None
    has_bins, has_tag, has_user = bool(bins), bool(args.tag), bool(args.user)
    has_since, has_until = args.since is not None, args.until is not None

    # SQL & params
    sql = build_sql(has_bins, has_tag, has_user, has_since, has_until, args.limit)
    params = {"ds_list": ds}
    if has_bins:  params["bins"] = bins
    if has_tag:   params["tag_like"] = [f"%{t}%" for t in args.tag]
    if has_user:  params["user_list"] = args.user
    if has_since: params["since"] = args.since
    if has_until: params["until"] = args.until

    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DB}"
    eng = sa.create_engine(url)

    with eng.connect() as con:
        df = pd.read_sql(sql, con, params=params)

    # Normalize timestamps
    for col in ("created_at","last_event_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # Normalize strings
    df["classname"] = df["classname"].astype(str).str.replace(r"\s+","_",regex=True)
    df["raw_tag"]   = df["raw_tag"].fillna("").astype(str).str.strip()

    # Build class_tag column
    df["class_tag"] = df["classname"]
    mask = df["raw_tag"].str.len() > 0
    tags_norm = (
        df.loc[mask,"raw_tag"]
          .str.replace(r"\s+","_",regex=True)
          .str.replace("||","_TAG_")
    )
    df.loc[mask,"class_tag"] = df.loc[mask,"classname"] + "_TAG_" + tags_norm

    # Final columns
    out = df[["pid","classname","raw_tag","class_tag","last_event_time","created_at","dataset"]].copy()

    out_path = Path(args.out).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path,index=False)
    print(f"OK: wrote {len(out)} rows → {out_path}")

    if args.emit_pids and not out.empty:
        pid_txt = out_path.with_suffix("").as_posix()+"_pids.txt"
        Path(pid_txt).write_text("\n".join(out["pid"].astype(str))+"\n",encoding="utf-8")
        print(f"OK: wrote PID list → {pid_txt}")

if __name__ == "__main__":
    main()
