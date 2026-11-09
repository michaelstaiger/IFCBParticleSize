"""Estimate an empirical target-particle arrival distribution b(t).

The estimator treats every input file as one IFCB run.  All image rows are
used to reconstruct periods when the instrument was unavailable, while only
rows passing the selected target-class threshold contribute target arrivals.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


def _merge_intervals(intervals: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Return the union of half-open time intervals."""
    valid = sorted((float(start), float(end)) for start, end in intervals if end > start)
    if not valid:
        return []

    merged: list[list[float]] = [[valid[0][0], valid[0][1]]]
    for start, end in valid[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def _inactive_seconds_by_bin(
    intervals: Sequence[tuple[float, float]],
    bin_edges: np.ndarray,
) -> np.ndarray:
    """Calculate unavailable seconds in each time bin for one run."""
    inactive = np.zeros(len(bin_edges) - 1, dtype=float)
    for start, end in _merge_intervals(list(intervals)):
        left = max(0, np.searchsorted(bin_edges, start, side="right") - 1)
        right = min(len(inactive) - 1, np.searchsorted(bin_edges, end, side="left"))
        for index in range(left, right + 1):
            overlap = max(0.0, min(end, bin_edges[index + 1]) - max(start, bin_edges[index]))
            inactive[index] += overlap
    return np.minimum(inactive, np.diff(bin_edges))


def estimate_empirical_b(
    runs: Iterable[pd.DataFrame],
    *,
    target_column: str = "Alexandrium_catenella",
    classifier_threshold: float = 0.9,
    run_duration_seconds: float = 1200.0,
    n_bins: int = 1200,
    runtime_column: str = "RunTime",
    inhibition_duration_column: str = "InhibitTimeDiff",
    sample_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Estimate target arrival probabilities from merged IFCB run tables.

    Parameters
    ----------
    runs:
        Iterable of merged run DataFrames. Each row must represent one image
        event and must contain timing plus all-particle inhibition metadata.
    target_column:
        Classifier-score column for the particle of interest.
    classifier_threshold:
        Rows with ``target_column > classifier_threshold`` are target detections.
    run_duration_seconds, n_bins:
        Common time scale used for all runs. The default gives one-second bins.
    runtime_column:
        Image timestamp in seconds from the start of the run.
    inhibition_duration_column:
        Per-image unavailable duration. This is ``InhibitTimeDiff`` in the
        current merged-file format, not cumulative ``InhibitTime``.
    sample_ids:
        Optional labels, one per input DataFrame.

    Returns
    -------
    pandas.DataFrame
        One row per time bin with exposure and corrected arrival diagnostics:
        ``b_t`` is the normalized empirical arrival distribution.
    """
    runs = list(runs)
    if not runs:
        raise ValueError("At least one run is required.")
    if sample_ids is not None and len(sample_ids) != len(runs):
        raise ValueError("sample_ids must have one value per run.")

    bin_edges = np.linspace(0.0, run_duration_seconds, n_bins + 1)
    bin_widths = np.diff(bin_edges)
    target_counts = np.zeros(n_bins, dtype=float)
    active_exposure_seconds = np.zeros(n_bins, dtype=float)
    sample_count = np.zeros(n_bins, dtype=int)
    target_rows = 0
    skipped_target_rows = 0

    required = {runtime_column, inhibition_duration_column, target_column}
    for run_index, run in enumerate(runs):
        missing = required.difference(run.columns)
        if missing:
            raise ValueError(
                f"Run {run_index} is missing required columns: {sorted(missing)}"
            )

        runtime = pd.to_numeric(run[runtime_column], errors="coerce")
        inhibit = pd.to_numeric(run[inhibition_duration_column], errors="coerce").fillna(0.0)
        valid_time = runtime.notna() & inhibit.notna()
        runtime = runtime[valid_time].to_numpy(dtype=float)
        inhibit = np.maximum(inhibit[valid_time].to_numpy(dtype=float), 0.0)

        intervals = [
            (time, min(time + duration, run_duration_seconds))
            for time, duration in zip(runtime, inhibit)
            if 0.0 <= time < run_duration_seconds and duration > 0.0
        ]
        inactive_seconds = _inactive_seconds_by_bin(intervals, bin_edges)
        active_seconds = np.maximum(bin_widths - inactive_seconds, 0.0)
        active_exposure_seconds += active_seconds
        sample_count += active_seconds > 0.0

        target_score = pd.to_numeric(run[target_column], errors="coerce")
        target_mask = (
            valid_time
            & target_score.notna()
            & (target_score > classifier_threshold)
            & (runtime >= 0.0)
            & (runtime < run_duration_seconds)
        )
        target_times = runtime[target_mask[valid_time].to_numpy()]
        target_bins = np.searchsorted(bin_edges, target_times, side="right") - 1
        target_bins = target_bins[(target_bins >= 0) & (target_bins < n_bins)]
        target_counts += np.bincount(target_bins, minlength=n_bins)
        target_rows += len(target_bins)

        # Rows outside the configured run window are retained as a diagnostic.
        skipped_target_rows += int((
            target_score.notna()
            & (target_score > classifier_threshold)
            & ~target_mask
        ).sum())

    with np.errstate(divide="ignore", invalid="ignore"):
        observed_rate = np.divide(
            target_counts,
            active_exposure_seconds,
            out=np.zeros_like(target_counts),
            where=active_exposure_seconds > 0,
        )

    arrival_mass = observed_rate * bin_widths
    total_mass = arrival_mass.sum()
    if total_mass <= 0.0:
        raise ValueError("No target detections were available after filtering.")

    result = pd.DataFrame({
        "bin_index": np.arange(n_bins),
        "bin_start_seconds": bin_edges[:-1],
        "bin_end_seconds": bin_edges[1:],
        "bin_center_seconds": (bin_edges[:-1] + bin_edges[1:]) / 2.0,
        "active_exposure_seconds": active_exposure_seconds,
        "available_fraction": active_exposure_seconds / (len(runs) * bin_widths),
        "samples_with_active_exposure": sample_count,
        "target_observed_count": target_counts,
        "target_observed_rate_per_active_second": observed_rate,
        "corrected_arrival_mass": arrival_mass,
        "b_t": arrival_mass / total_mass,
    })
    result.attrs.update({
        "n_runs": len(runs),
        "target_column": target_column,
        "classifier_threshold": classifier_threshold,
        "run_duration_seconds": run_duration_seconds,
        "n_bins": n_bins,
        "target_detection_count": target_rows,
        "target_rows_outside_window_or_invalid": skipped_target_rows,
    })
    return result


def estimate_empirical_b_from_files(
    paths: Iterable[str | Path],
    **kwargs,
) -> pd.DataFrame:
    """Load merged CSV files and estimate an empirical target b(t)."""
    paths = list(paths)
    runs = [pd.read_csv(path) for path in paths]
    result = estimate_empirical_b(runs, **kwargs)
    result.attrs["input_files"] = [str(Path(path)) for path in paths]
    return result


def save_empirical_b(result: pd.DataFrame, path: str | Path) -> None:
    """Save the estimated distribution and diagnostics as a CSV file."""
    result.to_csv(path, index=False)

