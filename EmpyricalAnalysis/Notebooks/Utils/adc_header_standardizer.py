"""Utilities to strictly standardize IFCB ADCFileFormat headers.

Design goals:
- Preserve original truth from .hdr files (raw ADCFileFormat line and raw tokens).
- Map known legacy token variants to one canonical header schema.
- Fail loudly in strict mode when tokens are unknown or order/length diverge.
- Support both directory workflows and API-driven download/parse/delete loops.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence
import argparse
import json
import re


CANONICAL_ADC_HEADERS: List[str] = [
    "trigger#",
    "ADCtime",
    "PMTA",
    "PMTB",
    "PMTC",
    "PMTD",
    "PeakA",
    "PeakB",
    "PeakC",
    "PeakD",
    "TimeOfFlight",
    "GrabTimeStart",
    "GrabTimeEnd",
    "RoiX",
    "RoiY",
    "RoiWidth",
    "RoiHeight",
    "StartByte",
    "ComparatorOut",
    "StartPoint",
    "SignalLength",
    "Status",
    "RunTime",
    "InhibitTime",
]

STANDARD_HEADER_PREFIX = "ADCFileFormatStandard:"


def _normalize_key(value: str) -> str:
    """Normalize token for lookup while preserving original text elsewhere."""
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


# Normalized token -> canonical token
DEFAULT_ALIAS_MAP: Dict[str, str] = {
    # Canonical spellings
    **{_normalize_key(t): t for t in CANONICAL_ADC_HEADERS},
    # Legacy spellings / separators / capitalization variants
    "adctime": "ADCtime",
    "adctime": "ADCtime",
    "adc_time": "ADCtime",  # this key is not normalized; included for readability
    "peaka": "PeakA",
    "peakb": "PeakB",
    "peakc": "PeakC",
    "peakd": "PeakD",
    "timeofflight": "TimeOfFlight",
    "timeofflight": "TimeOfFlight",
    "grabtimestart": "GrabTimeStart",
    "grabtimeend": "GrabTimeEnd",
    "roix": "RoiX",
    "roiy": "RoiY",
    "roiwidth": "RoiWidth",
    "roiheight": "RoiHeight",
    "startbyte": "StartByte",
    "startbyte": "StartByte",
    "comparatorout": "ComparatorOut",
    "comparatorout": "ComparatorOut",
    "startpoint": "StartPoint",
    "status": "Status",
    "runtime": "RunTime",
    "inhibittime": "InhibitTime",
    "trigger": "trigger#",
}

# Remove accidental non-normalized keys that were included for readability.
DEFAULT_ALIAS_MAP = {_normalize_key(k): v for k, v in DEFAULT_ALIAS_MAP.items()}


class HeaderMappingError(RuntimeError):
    """Raised when strict mapping/validation fails."""


@dataclass
class TokenMapping:
    index: int
    raw_token: str
    normalized_key: str
    mapped_token: Optional[str]
    mapping_status: str  # exact, alias, unknown


@dataclass
class HeaderParseReport:
    hdr_path: str
    raw_adcfileformat_line: str
    raw_tokens: List[str]
    mapped_tokens: List[Optional[str]]
    canonical_tokens: List[str] = field(default_factory=lambda: list(CANONICAL_ADC_HEADERS))
    token_mappings: List[TokenMapping] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    is_valid: bool = False
    standard_header_action: str = "none"  # none, added, already_present

    def to_json_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["token_mappings"] = [asdict(tm) for tm in self.token_mappings]
        return payload


@dataclass
class FileSetPaths:
    """Represents one fetched file set in API-driven loops."""

    hdr_path: str
    adc_path: Optional[str] = None
    class_csv_path: Optional[str] = None
    dataset: Optional[str] = None
    pid: Optional[str] = None


def extract_adcfileformat_line(hdr_path: str | Path) -> str:
    """Extract the full ADCFileFormat line exactly as stored in the HDR file."""
    hdr_path = Path(hdr_path)
    with hdr_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.startswith("ADCFileFormat:"):
                return line.rstrip("\n")
    raise HeaderMappingError(f"ADCFileFormat line not found in {hdr_path}")


def build_standard_adcfileformat_line(
    canonical_tokens: Optional[Sequence[str]] = None,
) -> str:
    """Build canonical ADCFileFormatStandard line text."""
    tokens = list(canonical_tokens or CANONICAL_ADC_HEADERS)
    return f"{STANDARD_HEADER_PREFIX} {', '.join(tokens)}"


def _extract_optional_standard_line(hdr_path: str | Path) -> Optional[str]:
    """Return existing ADCFileFormatStandard line if present, else None."""
    hdr_path = Path(hdr_path)
    with hdr_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.startswith(STANDARD_HEADER_PREFIX):
                return line.rstrip("\n")
    return None


def parse_adc_tokens(raw_adc_line: str) -> List[str]:
    """Parse raw ADCFileFormat line into ordered header tokens."""
    if not raw_adc_line.startswith("ADCFileFormat:"):
        raise HeaderMappingError("Input does not start with 'ADCFileFormat:'")
    right_side = raw_adc_line.split(":", 1)[1]
    return [tok.strip() for tok in right_side.split(",")]


def map_tokens_to_canonical(
    raw_tokens: Sequence[str],
    alias_map: Optional[Dict[str, str]] = None,
) -> tuple[List[Optional[str]], List[TokenMapping]]:
    """Map raw ordered tokens to canonical ordered tokens."""
    alias_map = alias_map or DEFAULT_ALIAS_MAP

    mapped: List[Optional[str]] = []
    details: List[TokenMapping] = []

    for i, raw in enumerate(raw_tokens):
        nkey = _normalize_key(raw)
        mapped_token = alias_map.get(nkey)

        if mapped_token is None:
            status = "unknown"
        elif raw == mapped_token:
            status = "exact"
        else:
            status = "alias"

        mapped.append(mapped_token)
        details.append(
            TokenMapping(
                index=i,
                raw_token=raw,
                normalized_key=nkey,
                mapped_token=mapped_token,
                mapping_status=status,
            )
        )

    return mapped, details


def validate_mapped_tokens(
    mapped_tokens: Sequence[Optional[str]],
    canonical_tokens: Sequence[str],
) -> List[str]:
    """Return a list of strict validation errors (empty means valid)."""
    errors: List[str] = []

    if any(tok is None for tok in mapped_tokens):
        unknown_idx = [str(i) for i, tok in enumerate(mapped_tokens) if tok is None]
        errors.append(f"Unknown token(s) at index: {', '.join(unknown_idx)}")

    if len(mapped_tokens) != len(canonical_tokens):
        errors.append(
            "Header length mismatch: "
            f"mapped={len(mapped_tokens)} canonical={len(canonical_tokens)}"
        )

    # Positional comparison only when lengths match and no unknowns.
    if not errors and list(mapped_tokens) != list(canonical_tokens):
        mismatches = []
        for i, (got, exp) in enumerate(zip(mapped_tokens, canonical_tokens)):
            if got != exp:
                mismatches.append(f"{i}: got={got} expected={exp}")
        errors.append("Positional mismatch: " + "; ".join(mismatches))

    # Duplicate check can catch accidental alias collisions.
    non_null = [tok for tok in mapped_tokens if tok is not None]
    if len(non_null) != len(set(non_null)):
        errors.append("Duplicate canonical tokens detected after mapping")

    return errors


def parse_and_standardize_hdr(
    hdr_path: str | Path,
    canonical_tokens: Optional[Sequence[str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
    strict: bool = True,
) -> HeaderParseReport:
    """Parse one HDR and produce strict standardization report."""
    canonical_tokens = list(canonical_tokens or CANONICAL_ADC_HEADERS)

    raw_line = extract_adcfileformat_line(hdr_path)
    raw_tokens = parse_adc_tokens(raw_line)
    mapped_tokens, token_mappings = map_tokens_to_canonical(raw_tokens, alias_map=alias_map)
    errors = validate_mapped_tokens(mapped_tokens, canonical_tokens)

    report = HeaderParseReport(
        hdr_path=str(hdr_path),
        raw_adcfileformat_line=raw_line,
        raw_tokens=list(raw_tokens),
        mapped_tokens=list(mapped_tokens),
        canonical_tokens=list(canonical_tokens),
        token_mappings=token_mappings,
        errors=errors,
        is_valid=(len(errors) == 0),
    )

    if strict and errors:
        raise HeaderMappingError(
            f"Strict header mapping failed for {hdr_path}: " + " | ".join(errors)
        )

    return report


def append_standard_header_to_hdr(
    hdr_path: str | Path,
    *,
    canonical_tokens: Optional[Sequence[str]] = None,
    strict: bool = True,
) -> str:
    """Append ADCFileFormatStandard to HDR without changing original content.

    Returns one of:
    - "added": standard line appended
    - "already_present": matching line already existed

    Strict behavior:
    - Fail if an existing ADCFileFormatStandard line differs from canonical.
    - Never overwrite or mutate existing ADCFileFormatStandard values.
    """
    hdr_path = Path(hdr_path)
    canonical_line = build_standard_adcfileformat_line(canonical_tokens=canonical_tokens)
    existing = _extract_optional_standard_line(hdr_path)

    if existing is not None:
        if existing.strip() == canonical_line.strip():
            return "already_present"

        msg = (
            f"Existing {STANDARD_HEADER_PREFIX} differs from canonical in {hdr_path}. "
            "Refusing to overwrite to preserve original truth."
        )
        if strict:
            raise HeaderMappingError(msg)
        return "already_present"

    text = hdr_path.read_text(encoding="utf-8", errors="ignore")
    suffix = "" if text.endswith("\n") else "\n"
    text_out = text + suffix + canonical_line + "\n"
    hdr_path.write_text(text_out, encoding="utf-8")
    return "added"


def process_hdr_directory(
    directory: str | Path,
    pattern: str = "*.hdr",
    recursive: bool = True,
    strict: bool = True,
    canonical_tokens: Optional[Sequence[str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
    report_dir: Optional[str | Path] = None,
    write_standard_to_hdr: bool = False,
) -> List[HeaderParseReport]:
    """Process all HDR files in a directory and optionally write JSON reports."""
    directory = Path(directory)
    globber = directory.rglob if recursive else directory.glob
    hdr_files = sorted(globber(pattern))

    reports: List[HeaderParseReport] = []
    report_dir_path = Path(report_dir) if report_dir else None
    if report_dir_path:
        report_dir_path.mkdir(parents=True, exist_ok=True)

    for hdr_file in hdr_files:
        try:
            report = parse_and_standardize_hdr(
                hdr_path=hdr_file,
                canonical_tokens=canonical_tokens,
                alias_map=alias_map,
                strict=strict,
            )
        except HeaderMappingError as exc:
            if strict:
                raise
            # In non-strict mode, preserve whatever we can for audit.
            raw_line = ""
            raw_tokens: List[str] = []
            try:
                raw_line = extract_adcfileformat_line(hdr_file)
                raw_tokens = parse_adc_tokens(raw_line)
            except Exception:
                pass
            report = HeaderParseReport(
                hdr_path=str(hdr_file),
                raw_adcfileformat_line=raw_line,
                raw_tokens=raw_tokens,
                mapped_tokens=[],
                canonical_tokens=list(canonical_tokens or CANONICAL_ADC_HEADERS),
                token_mappings=[],
                errors=[str(exc)],
                is_valid=False,
            )

        reports.append(report)

        if write_standard_to_hdr:
            if report.is_valid:
                action = append_standard_header_to_hdr(
                    hdr_path=hdr_file,
                    canonical_tokens=canonical_tokens,
                    strict=strict,
                )
                report.standard_header_action = action
            else:
                report.standard_header_action = "none"

        if report_dir_path:
            out_path = report_dir_path / f"{hdr_file.stem}_adc_header_report.json"
            out_path.write_text(json.dumps(report.to_json_dict(), indent=2), encoding="utf-8")

    if report_dir_path:
        summary = summarize_reports(reports)
        (report_dir_path / "summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )

    return reports


def summarize_reports(reports: Sequence[HeaderParseReport]) -> Dict[str, object]:
    """Create high-level summary for batch runs."""
    total = len(reports)
    valid = sum(1 for r in reports if r.is_valid)
    invalid = total - valid

    err_counts: Dict[str, int] = {}
    for rep in reports:
        for err in rep.errors:
            err_counts[err] = err_counts.get(err, 0) + 1

    action_counts: Dict[str, int] = {}
    for rep in reports:
        action_counts[rep.standard_header_action] = (
            action_counts.get(rep.standard_header_action, 0) + 1
        )

    return {
        "total_files": total,
        "valid_files": valid,
        "invalid_files": invalid,
        "error_counts": err_counts,
        "standard_header_actions": action_counts,
    }


def process_api_file_sets(
    file_sets: Iterable[FileSetPaths],
    *,
    strict: bool = True,
    canonical_tokens: Optional[Sequence[str]] = None,
    alias_map: Optional[Dict[str, str]] = None,
    on_report: Optional[Callable[[HeaderParseReport, FileSetPaths], None]] = None,
    delete_after_each: bool = False,
    delete_file_set: Optional[Callable[[FileSetPaths], None]] = None,
    write_standard_to_hdr: bool = False,
) -> Iterator[HeaderParseReport]:
    """Process an API-driven iterator of file sets.

    This is intentionally generic so it can be plugged into a downloader loop
    that fetches one file set at a time from an API.

    Parameters
    ----------
    file_sets:
        Iterable of FileSetPaths yielded by your API download loop.
    on_report:
        Optional callback for immediate persistence/logging per file set.
    delete_after_each:
        If True, invokes delete_file_set(fs) after processing.
    delete_file_set:
        Callback that deletes local temporary files for one file set.
    """
    for fs in file_sets:
        report = parse_and_standardize_hdr(
            hdr_path=fs.hdr_path,
            canonical_tokens=canonical_tokens,
            alias_map=alias_map,
            strict=strict,
        )

        if write_standard_to_hdr and report.is_valid:
            report.standard_header_action = append_standard_header_to_hdr(
                hdr_path=fs.hdr_path,
                canonical_tokens=canonical_tokens,
                strict=strict,
            )

        if on_report is not None:
            on_report(report, fs)

        if delete_after_each:
            if delete_file_set is None:
                raise ValueError("delete_after_each=True requires delete_file_set callback")
            delete_file_set(fs)

        yield report


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Strictly standardize IFCB ADCFileFormat headers from HDR files"
    )
    parser.add_argument("--hdr", type=str, help="Single .hdr file to process")
    parser.add_argument("--directory", type=str, help="Directory of .hdr files to process")
    parser.add_argument("--pattern", type=str, default="*.hdr", help="Glob pattern (default: *.hdr)")
    parser.add_argument("--non-recursive", action="store_true", help="Do not recurse into subdirectories")
    parser.add_argument("--non-strict", action="store_true", help="Do not raise on validation failures")
    parser.add_argument("--report-dir", type=str, default=None, help="Optional output directory for JSON reports")
    parser.add_argument(
        "--write-standard-to-hdr",
        action="store_true",
        help=(
            "Append ADCFileFormatStandard to HDR files when validation passes. "
            "Never overwrites an existing differing standard line."
        ),
    )

    args = parser.parse_args()

    strict = not args.non_strict
    recursive = not args.non_recursive

    if bool(args.hdr) == bool(args.directory):
        raise SystemExit("Provide exactly one of --hdr or --directory")

    if args.hdr:
        report = parse_and_standardize_hdr(args.hdr, strict=strict)
        if args.write_standard_to_hdr and report.is_valid:
            report.standard_header_action = append_standard_header_to_hdr(
                hdr_path=args.hdr,
                strict=strict,
            )
        print(json.dumps(report.to_json_dict(), indent=2))
        return

    reports = process_hdr_directory(
        directory=args.directory,
        pattern=args.pattern,
        recursive=recursive,
        strict=strict,
        report_dir=args.report_dir,
        write_standard_to_hdr=args.write_standard_to_hdr,
    )
    print(json.dumps(summarize_reports(reports), indent=2))


if __name__ == "__main__":
    _cli()
