#!/usr/bin/env python3
"""
Validation of a gs://asap-raw-<dataset_id> bucket.

Checks GCS bucket structure and contents:
  • Validates that billing project is set
  • Validates existence/accessibility of bucket
  • Validates presence of folders:
    - Mandatory: raw (raw/, fastqs/ or fastq/ [with warning]), metadata/
    - Optional: artifacts/
    - Special: spatial/ (for spatial datasets)
      Note: alternative raw/ names are reported as warnings.
  • Identifies potentially empty files or metadata/ with only column headers
  • If metadata/DATA.csv is present
    - Compares file_name vs. actual file names in the bucket
    - Compares sample_id vs. file_name for consistency
    - Fuzzy matching flags likely name typos (e.g. '-' vs. '_', or missing '_001')
  • Checks that sample_id and subject_id values are consistent across metadata tables

Outputs:
A bucket_validation.md report, including:
  - An executive summary with critical issues and important warnings
  - Detailed sections for each folder
Inconsistency reconciliation tables (if issues are found):
  - sample_id_issues.tsv
  - subject_id_issues.tsv
  - data_vs_bucket.tsv
  - sample_id_vs_file_name.tsv

Usage as CLI:
python3 validate_raw_bucket_structure.py -d team-smith-sc-rnaseq

Usage as module:
from validate_raw_bucket_structure import perform_bucket_validation

"""

import os
import re
import csv
import sys
import shutil
import time
import subprocess
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import argparse

repo_root = Path(__file__).resolve().parents[2]
metadata_root = repo_root.parent / "asap-crn-cloud-dataset-metadata"
crn_utils_root = repo_root.parent / "crn-utils" / "src"

sys.path.insert(0, str(repo_root / "util" / "common"))
sys.path.insert(0, str(crn_utils_root))
sys.path.insert(0, str(metadata_root / "utils"))

# wf-common
from gcloud_ops import gsync
from bucket_validation_utils import (
    validate_raw_bucket_and_folder_existence,
    list_bucket_structure,
    CORE_METADATA_FILES,
    SUPP_METADATA_FILES,
    )
from file_utils import (
    get_file_extension,
    check_csv_rows
    )

# crn-utils
from crn_utils.util import load_tables
from crn_utils.update_schema import get_table_update_map

# asap-crn-cloud-dataset-metadata
from logging_extra import log_run_command


# ── default parameters ─────────────────────────────────────────────────

# normalize TABLE names  once for case-insensitive lookup.
# e.g. {"ASSAY_RNASEQ": "ASSAY", "SPATIAL": "ASSAY", "MOUSE": "SUBJECT", ...}
TABLE_UPDATE_MAP_UPPER = {k.upper(): v.upper() for k, v in get_table_update_map().items()}

MIN_FILE_SIZE_BYTES = 3
MIN_CSV_ROWS = 2
NUMBER_SUBDIRS = 2
MANDATORY_FOLDERS = ['metadata', 'raw']
MANDATORY_DISPLAY = ', '.join([f for f in MANDATORY_FOLDERS if f != 'raw'] + ['raw (or fastqs)'])
_log_divider = "=" * 80
_number_examples = 5

RAW_ALTERNATIVES = {
    'raw': 'raw',
    'fastqs': 'fastqs',
    'fastq': "fastq [missing final 's']",
}

CASE_FOLDERS = ['metadata'] + list(RAW_ALTERNATIVES.keys()) + ['artifacts', 'spatial']

data_file_name = "DATA.csv"

# All recognised table stems (uppercase). Files whose stem is absent from this
# set are flagged as unrecognised (likely a filename typo) in the report.
_KNOWN_TABLE_STEMS = (
    {f[:-4].upper() for f in CORE_METADATA_FILES}
    | {f[:-4].upper() for f in SUPP_METADATA_FILES}
    | set(TABLE_UPDATE_MAP_UPPER.keys())
)

# Mandatory column checks: each key is a column that must appear in all listed tables.
# Only tables present in the dataset are checked; absent tables are skipped.
MANDATORY_COLS_PER_TABLE = {
    "sample_id": ["ASSAY", "DATA", "SAMPLE", "ASSAY_RNAseq", "PMDBS", "SPATIAL", "PROTEOMICS"],
    "subject_id": ["CLINPATH", "SAMPLE", "SUBJECT", "MOUSE", "CELL", "PROTEOMICS"],
}

# Illumina FASTQ naming suffixes after normalization (lowercase, hyphens → underscores).
# Full form: <sample>_S<n>_L<n>_[R|I]<n>_<nnn>.fastq.gz
_FULL_ILLUMINA_SUFFIX_RE = re.compile(r'_s\d+_l\d+_[ri]\d+_\d{3}\.fastq\.gz$')
# Read + chunk form (no sample index): strips only _R<n>_<nnn>.fastq.gz so _L<n> stays in stem
_READ_CHUNK_ILLUMINA_SUFFIX_RE = re.compile(r'_[ri]\d+_\d{3}\.fastq\.gz$')
# Read form: <sample>_[R|I]<n>.fastq.gz
_READ_ILLUMINA_SUFFIX_RE = re.compile(r'_[ri]\d+\.fastq\.gz$')

# Known FASTQ extensions stripped from DATA.csv file_name values to derive the sample stem.
# Ordered longest-first so '.fastq.gz' is matched before '.gz'.
_FASTQ_EXTENSIONS = ('.fastq.gz', '.fq.gz', '.fastq', '.fq')

# Suffixes stripped from metadata filenames after download so downstream code
# always sees canonical table names (e.g. ASSAY_complete.csv → ASSAY.csv).
_METADATA_FILE_SUFFIX_STRIP = ['_complete', '.cde_compared']

emoji_success = "✅"
emoji_error = "❌"
emoji_warning = "⚠️"

# ── File massaging ────────────────────────────────────────────────────

def strip_metadata_suffixes(metadata_dir: Path) -> list:
    """
    Rename metadata files by stripping known non-standard suffixes.

    Strips entries in `_METADATA_FILE_SUFFIX_STRIP` from filenames immediately
    before the `.csv` extension (e.g. `ASSAY_complete.csv` → `ASSAY.csv`).
    Skips macOS artefact files beginning with '._'.
    Prints a warning for each rename or skip.

    Parameters
    ----------
    metadata_dir : Path
        Local directory containing downloaded metadata files.

    Returns
    -------
    list of dict
        One entry per renamed file with keys `original`, `renamed`, `suffix`,
        `skipped` (bool), and `reason` (only when `skipped` is True).
    """
    renames = []
    if not metadata_dir or not metadata_dir.exists():
        return renames
    for filepath in sorted(metadata_dir.iterdir()):
        if not filepath.is_file():
            continue
        if filepath.name.startswith('._'):
            continue
        name = filepath.name
        for suffix in _METADATA_FILE_SUFFIX_STRIP:
            new_name = None
            if name.lower().endswith(suffix.lower() + '.csv'):
                new_name = name[:-(len(suffix) + 4)] + '.csv'
            if new_name:
                dest = filepath.parent / new_name
                if dest.exists():
                    renames.append({
                        'original': name, 'renamed': new_name,
                        'suffix': suffix, 'skipped': True,
                        'reason': 'destination already exists',
                    })
                    print(f"    Warning: could not rename '{name}' → '{new_name}': destination already exists")
                else:
                    filepath.rename(dest)
                    renames.append({
                        'original': name, 'renamed': new_name,
                        'suffix': suffix, 'skipped': False,
                    })
                    print(f"    Warning: renamed '{name}' → '{new_name}' (stripped suffix '{suffix}')")
                break
    return renames


def _normalize_filename(name: str) -> str:
    """Lowercase and replace hyphens with underscores for fuzzy comparison."""
    return name.lower().replace('-', '_')


def _norm_sample_id(s: str) -> str:
    """Lowercase and strip all separator chars for SAMPLE↔DATA fuzzy matching."""
    return re.sub(r'[-_ ]', '', s.lower())


def _strip_illumina_suffix(name: str) -> str:
    """
    Normalize a filename and strip its Illumina FASTQ suffix if present.

    Normalization lowercases and replaces hyphens with underscores before
    suffix matching. The returned value is always in normalized form.

    Parameters
    ----------
    name : str
        Raw or already-normalized filename.

    Returns
    -------
    str
        Normalized filename with the Illumina suffix removed, or the normalized
        original if no suffix matched.
    """
    norm = _normalize_filename(name)
    for suffix_re in (_FULL_ILLUMINA_SUFFIX_RE, _READ_CHUNK_ILLUMINA_SUFFIX_RE, _READ_ILLUMINA_SUFFIX_RE):
        stripped = suffix_re.sub('', norm)
        if stripped != norm:
            return stripped
    return norm


def _csv_stem(csv_norm: str) -> str:
    """
    Strip known FASTQ extensions from a normalized DATA file_name to get the sample stem.

    Parameters
    ----------
    csv_norm : str
        Normalized file_name value (lowercase, hyphens → underscores).

    Returns
    -------
    str
        Sample stem, or the full name if no known extension matched.
    """
    for ext in _FASTQ_EXTENSIONS:
        if csv_norm.endswith(ext):
            return csv_norm[:-len(ext)]
    return csv_norm


# ── Analysis ───────────────────────────────────────────────────────────────────

def analyze_metadata(metadata_dir: Path, min_csv_rows: int = 2) -> dict:
    """
    Check metadata CSV files in the root of a local metadata directory for
    sufficient row counts.

    Skips macOS artefact files beginning with '._'.

    Parameters
    ----------
    metadata_dir : Path
        Local directory containing downloaded metadata CSV files.
    min_csv_rows : int
        Minimum required rows (including header).

    Returns
    -------
    dict
        csv_files : dict
            Mapping of filename → check_csv_rows result dict.
        issues : list of str
            Human-readable descriptions of any insufficient or unreadable files.
    """
    results = {'csv_files': {}, 'issues': []}

    if not metadata_dir or not metadata_dir.exists():
        results['issues'].append('Metadata folder not accessible')
        return results

    csv_files = [
        f for f in (list(metadata_dir.glob('*.csv')) + list(metadata_dir.glob('*.CSV')))
        if f.is_file() and not f.name.startswith('._')
    ]

    for csv_file in csv_files:
        csv_result = check_csv_rows(csv_file, min_csv_rows)
        results['csv_files'][csv_file.name] = csv_result
        if csv_result['status'] == 'insufficient':
            results['issues'].append(
                f"{csv_file.name} has only {csv_result['rows']} rows (minimum {min_csv_rows} required)"
            )
        elif csv_result['status'] == 'error':
            results['issues'].append(
                f"{csv_file.name} could not be read: {csv_result['error']}"
            )

    return results


def check_mandatory_column_consistency(metadata_dir: Path, mandatory_cols: dict) -> list[dict]:
    """
    Check mandatory column presence and value consistency across metadata tables.

    For each column in `mandatory_cols`, checks that all mandatory tables present
    in the dataset contain the column, and that their values are consistent.
    Tables absent from the dataset are skipped. Skips macOS artefact files ('._').

    Parameters
    ----------
    metadata_dir : Path
        Local directory containing downloaded metadata CSV files.
    mandatory_cols : dict
        Keys are column names (e.g. 'sample_id'); values are lists of table stems
        (without .csv, any case) that should contain that column.

    Returns
    -------
    list of dict
        One entry per column with keys:
        column_header : str
            The column name.
        presence_status : str
            emoji_success if all present mandatory tables contain the column, emoji_error otherwise.
        values_status : str
            emoji_success exact same value set across all tables,
            emoji_warning same after normalization (case/underscore/hyphen),
            emoji_error differ even after normalization,
            emoji_warning if fewer than 2 tables have the column.
        details : str
            Description of issues when status is not emoji_success.
        tables_checked : list of str
            Table stems that were present and processed.
        col_found_in : dict or None
            table_name → set of raw values; populated only when values_status is emoji_error.
    """
    if not metadata_dir or not metadata_dir.exists():
        return []

    stem_map = {
        f.stem.upper(): f.stem
        for f in sorted(metadata_dir.iterdir())
        if f.is_file() and f.suffix.lower() == '.csv' and not f.name.startswith('._')
    }

    def _norm(v: str) -> str:
        return v.strip().lower().replace('_', '-').replace(' ', '-')

    results = []
    for col_name, mandatory_tables in mandatory_cols.items():
        present = {t: stem_map[t.upper()] for t in mandatory_tables if t.upper() in stem_map}
        if not present:
            continue

        tables = load_tables(metadata_dir, list(present.values()))
        name_to_df = {t: tables[stem] for t, stem in present.items()}

        col_found_in = {}
        col_missing_in = []
        for table_name, df in name_to_df.items():
            col_key = next((c for c in df.columns if c.lower().strip() == col_name.lower()), None)
            if col_key is None:
                col_missing_in.append(table_name)
            else:
                values = set(df[col_key].dropna().astype(str).str.strip()) - {''}
                col_found_in[table_name] = values

        presence_status = emoji_error if col_missing_in else emoji_success
        presence_detail = f"Missing column in: {', '.join(sorted(col_missing_in))}" if col_missing_in else ''

        if len(col_found_in) < 2:
            values_status = emoji_warning
            values_detail = 'Fewer than 2 tables with the column, cannot compare values'
        else:
            raw_sets = list(col_found_in.values())
            if len({frozenset(s) for s in raw_sets}) == 1:
                values_status = emoji_success
                values_detail = ''
            else:
                norm_sets = [frozenset(_norm(v) for v in s) for s in raw_sets]
                if len(set(norm_sets)) == 1:
                    values_status = emoji_warning
                    values_detail = 'Values match after normalization (case/separator differences)'
                else:
                    values_status = emoji_error
                    all_values_set = set().union(*col_found_in.values())
                    missing_counts = [
                        f"{t} : {len(all_values_set - col_found_in[t])}"
                        for t in sorted(col_found_in)
                        if all_values_set - col_found_in[t]
                    ]
                    values_detail = '; '.join(missing_counts) if missing_counts else 'Value sets differ'

        details = '; '.join(d for d in [presence_detail, values_detail] if d)
        results.append({
            'column_header': col_name,
            'presence_status': presence_status,
            'values_status': values_status,
            'details': details,
            'tables_checked': sorted(present.keys()),
            'col_found_in': col_found_in if values_status == emoji_error else None,
        })

    return results


def analyze_folder(files: list, gs_bucket: str,
                   number_subdirs: int, min_file_size: int) -> dict:
    """
    Analyse files in a GCS bucket folder.

    Counts files by extension at each folder depth up to `number_subdirs`,
    computes total size, flags potentially empty files, and detects subfolders.

    Parameters
    ----------
    files : list of dictc
        File-info dicts from `list_bucket_structure` (keys: 'path', 'size', 'size_str').
    gs_bucket : str
        GCS bucket URL (used to compute relative paths).
    number_subdirs : int
        Maximum number of subdirectory levels to include in the folder structure analysis.
    min_file_size : int
        Files smaller than this (bytes) are flagged as potentially empty.

    Returns
    -------
    dict
        total_files, extensions, folder_structure, potentially_empty,
        total_size, has_subfolders, subfolder_count.
    """
    results = {
        'total_files': len(files),
        'extensions': defaultdict(int),
        'folder_structure': defaultdict(lambda: defaultdict(int)),
        'potentially_empty': [],
        'total_size': 0,
        'has_subfolders': False,
        'subfolder_count': 0,
    }
    subfolders = set()

    for file_info in files:
        path = file_info['path']
        ext = get_file_extension(path)
        results['extensions'][ext if ext else 'no_extension'] += 1

        relative_path = path.replace(gs_bucket + '/', '')
        path_parts = relative_path.split('/')
        max_folder_idx = min(number_subdirs, len(path_parts) - 2)

        if max_folder_idx < 0:
            folder_path = path_parts[0] if path_parts else ''
        else:
            folder_path = '/'.join(path_parts[:max_folder_idx + 1])

        results['folder_structure'][folder_path][ext if ext else 'no_extension'] += 1

        if file_info['size'] < min_file_size:
            results['potentially_empty'].append({'path': path, 'size': file_info['size_str']})

        results['total_size'] += file_info['size']

        if len(path_parts) > 2:
            subfolders.add(path_parts[1])
            results['has_subfolders'] = True

    results['subfolder_count'] = len(subfolders)
    return results


def check_three_way_consistency(
    metadata_dir: Path,
    raw_files: list,
    data_csv_name: str,
    extra_folder_files: dict | None = None,
) -> dict:
    """
    Three-way consistency check: SAMPLE.sample_id vs. DATA.sample_id/file_name vs. bucket files.

    For each sample_id in SAMPLE.csv, verifies that matching DATA.csv rows exist and
    that each DATA.file_name is present in the raw/ bucket. Supports exact, Illumina
    suffix, fuzzy (separator/numeric), and prefix bucket matching.
    If SAMPLE.csv is absent or has no sample_id column, falls back to DATA vs. bucket only.

    Parameters
    ----------
    metadata_dir : Path
        Local directory containing downloaded metadata CSV files.
    raw_files : list of dict
        File-info dicts from the raw/ folder (keys: 'path', 'size', 'size_str').
        Pass an empty list when no raw folder exists.
    data_csv_name : str
        Expected name of the DATA file (e.g. 'DATA.csv'), matched case-insensitively.
    extra_folder_files : dict or None, optional
        Additional folders to search for DATA files missing from raw/.
        Keys are folder names (e.g. 'spatial'), values are file-info dict lists.

    Returns
    -------
    dict
        sample_csv_found : bool
        sample_id_col_found : bool
        data_found : bool
        data_sample_id_col_found : bool
        data_file_name_col_found : bool
        rows : list of dict
            One entry per (sample_id, file_name) combination, plus entries for
            sample-only, data-only, and extra-bucket cases.
            Keys: sample_id_sample, sample_id_data, file_name, bucket_file, match_type.
        md5_files : list of str
        n_exact : int
        n_partial : int
        n_fuzzy : int
        n_prefix : int
        n_found_in_extra : int
        n_missing_bucket : int
        n_in_sample_only : int
        n_in_data_only : int
        n_only_bucket : int
        issues : list of str
    """
    result = {
        'sample_csv_found': False,
        'sample_id_col_found': False,
        'data_found': False,
        'data_sample_id_col_found': False,
        'data_file_name_col_found': False,
        'rows': [],
        'md5_files': [],
        'n_exact': 0,
        'n_partial': 0,
        'n_fuzzy': 0,
        'n_prefix': 0,
        'n_found_in_extra': 0,
        'found_in_extra_folders': {},
        'n_missing_bucket': 0,
        'n_in_sample_only': 0,
        'n_in_data_only': 0,
        'n_sample_data_fuzzy': 0,
        'n_only_bucket': 0,
        'issues': [],
    }

    # ── 1. Read SAMPLE.sample_id ──────────────────────────────────────
    sample_ids_from_sample = {}  # lower → original
    if metadata_dir and metadata_dir.exists():
        sample_csv_path = None
        for f in metadata_dir.iterdir():
            if f.name.startswith('._'):
                continue
            if f.stem.upper() == 'SAMPLE' and f.suffix.lower() == '.csv' and f.is_file():
                sample_csv_path = f
                break
        if sample_csv_path:
            result['sample_csv_found'] = True
            for encoding in ('utf-8', 'latin-1'):
                try:
                    with open(sample_csv_path, 'r', encoding=encoding) as fh:
                        reader = csv.DictReader(fh)
                        col = next(
                            (k for k in (reader.fieldnames or []) if k.lower().strip() == 'sample_id'),
                            None,
                        )
                        if col:
                            result['sample_id_col_found'] = True
                            for row in reader:
                                val = row[col].strip()
                                if val:
                                    sample_ids_from_sample[val.lower()] = val
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    result['issues'].append(f"Could not read SAMPLE.csv: {e}")
                    break

    # ── 2. Read DATA.csv ──────────────────────────────────────────────
    data_csv_path = None
    if metadata_dir and metadata_dir.exists():
        for f in metadata_dir.iterdir():
            if f.name.startswith('._'):
                continue
            if f.name.upper() == data_csv_name.upper() and f.is_file():
                data_csv_path = f
                break

    if data_csv_path is None:
        return result
    result['data_found'] = True

    data_by_sample = defaultdict(list)  # lower sample_id → [{'sample_id': str, 'file_name': str}]
    all_file_names = []

    for encoding in ('utf-8', 'latin-1'):
        try:
            with open(data_csv_path, 'r', encoding=encoding) as fh:
                reader = csv.DictReader(fh)
                fieldnames = reader.fieldnames or []
                sample_id_key = next(
                    (k for k in fieldnames if k.lower().strip() == 'sample_id'), None
                )
                file_name_key = next(
                    (k for k in fieldnames if k.lower().strip() == 'file_name'), None
                )
                if sample_id_key:
                    result['data_sample_id_col_found'] = True
                if file_name_key:
                    result['data_file_name_col_found'] = True
                if not sample_id_key:
                    result['issues'].append(f"No 'sample_id' column in {data_csv_name}")
                    return result
                if not file_name_key:
                    result['issues'].append(f"No 'file_name' column in {data_csv_name}")
                    return result
                for row in reader:
                    sid = row[sample_id_key].strip()
                    fn = row[file_name_key].strip()
                    if sid and fn:
                        data_by_sample[sid.lower()].append({'sample_id': sid, 'file_name': fn})
                        all_file_names.append(fn)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            result['issues'].append(f"Could not read {data_csv_name}: {e}")
            return result

    # ── 3. Bucket file list ───────────────────────────────────────────
    bucket_file_names = []
    md5_file_names = []
    for file_info in raw_files:
        basename = os.path.basename(file_info['path'])
        if basename.endswith('.md5'):
            md5_file_names.append(basename)
        else:
            bucket_file_names.append(basename)
    result['md5_files'] = md5_file_names

    # ── 4. Bucket matching ────────────────────────────────────────────
    csv_set = set(all_file_names)
    bucket_set = set(bucket_file_names)
    matched = sorted(csv_set & bucket_set)
    unmatched_csv = csv_set - bucket_set
    unmatched_bucket = bucket_set - csv_set

    # Partial matches (Illumina suffix — files need renaming, not errors)
    partial_matches = []
    partial_csv_names = set()
    partial_bucket_names = set()
    if unmatched_csv and unmatched_bucket:
        _suffix_patterns = [
            (_FULL_ILLUMINA_SUFFIX_RE, 'illumina_suffix_full'),
            (_READ_CHUNK_ILLUMINA_SUFFIX_RE, 'illumina_suffix_read_chunk'),
            (_READ_ILLUMINA_SUFFIX_RE, 'illumina_suffix_read'),
        ]
        illumina_bucket_map = defaultdict(list)
        for name_in_bucket in unmatched_bucket:
            name_norm = _normalize_filename(name_in_bucket)
            for suffix_re, match_type in _suffix_patterns:
                stripped = suffix_re.sub('', name_norm)
                if stripped != name_norm:
                    illumina_bucket_map[stripped].append((name_in_bucket, match_type))
                    break
        for name_in_data in sorted(unmatched_csv):
            stem = _csv_stem(_normalize_filename(name_in_data))
            if stem in illumina_bucket_map:
                entries = sorted(illumina_bucket_map[stem], key=lambda x: x[0])
                bucket_matched = [b for b, _ in entries]
                types = sorted({mt for _, mt in entries})
                match_type = types[0] if len(types) == 1 else 'illumina_suffix_mixed'
                partial_matches.append({
                    'csv_name': name_in_data,
                    'bucket_names': bucket_matched,
                    'match_type': match_type,
                })
                partial_csv_names.add(name_in_data)
                partial_bucket_names.update(bucket_matched)

    in_csv_only_set = unmatched_csv - partial_csv_names
    in_bucket_only_set = unmatched_bucket - partial_bucket_names

    # Fuzzy matches (typos — still errors)
    fuzzy_matches = []
    if in_csv_only_set and in_bucket_only_set:
        bucket_norm_map = {_normalize_filename(b): b for b in in_bucket_only_set}
        for csv_name in sorted(in_csv_only_set):
            csv_norm = _normalize_filename(csv_name)
            if csv_norm in bucket_norm_map:
                fuzzy_matches.append({
                    'csv_name': csv_name,
                    'bucket_name': bucket_norm_map[csv_norm],
                    'match_type': 'separator_mismatch',
                })
                continue
            csv_stem_val = _csv_stem(csv_norm)
            csv_ext = csv_norm[len(csv_stem_val):]
            csv_stem_stripped = re.sub(r'_\d+$', '', csv_stem_val)
            if not csv_stem_stripped:
                continue
            for b_norm, b_name in bucket_norm_map.items():
                b_stem_val = _csv_stem(b_norm)
                b_ext = b_norm[len(b_stem_val):]
                if csv_ext != b_ext:
                    continue
                b_stem_stripped = re.sub(r'_\d+$', '', b_stem_val)
                if csv_stem_stripped == b_stem_stripped:
                    fuzzy_matches.append({
                        'csv_name': csv_name,
                        'bucket_name': b_name,
                        'match_type': 'numeric_suffix_mismatch',
                    })
                    break

    fuzzy_csv_names = {fm['csv_name'] for fm in fuzzy_matches}
    fuzzy_bucket_names = {fm['bucket_name'] for fm in fuzzy_matches}
    remaining_csv = in_csv_only_set - fuzzy_csv_names
    remaining_bucket = in_bucket_only_set - fuzzy_bucket_names

    # Prefix matches (name containment)
    prefix_matches = []
    if remaining_csv and remaining_bucket:
        bucket_stem_map = defaultdict(list)
        for name_in_bucket in remaining_bucket:
            bucket_stem_map[_csv_stem(_strip_illumina_suffix(name_in_bucket))].append(name_in_bucket)
        for name_in_data in sorted(remaining_csv):
            csv_stem_val = _csv_stem(_normalize_filename(name_in_data))
            if not csv_stem_val:
                continue
            for b_stem, b_names in sorted(bucket_stem_map.items()):
                short, long_ = (
                    (csv_stem_val, b_stem) if len(csv_stem_val) <= len(b_stem)
                    else (b_stem, csv_stem_val)
                )
                if long_.startswith(short) and (len(long_) == len(short) or long_[len(short)] == '_'):
                    match_type = (
                        'DATA_prefix_of_bucket' if len(csv_stem_val) <= len(b_stem)
                        else 'bucket_prefix_of_DATA'
                    )
                    prefix_matches.append({
                        'csv_name': name_in_data,
                        'bucket_names': sorted(b_names),
                        'match_type': match_type,
                    })
                    break

    prefix_csv_names = {pm['csv_name'] for pm in prefix_matches}
    prefix_bucket_names = {b for pm in prefix_matches for b in pm['bucket_names']}
    truly_missing = remaining_csv - prefix_csv_names
    in_bucket_only_final = sorted(remaining_bucket - prefix_bucket_names)

    # Extra folder lookup (e.g. spatial/)
    found_in_extra = {}
    missing_in_bucket_list = list(truly_missing)
    if extra_folder_files and missing_in_bucket_list:
        remaining = list(missing_in_bucket_list)
        for folder_name, folder_files in extra_folder_files.items():
            if not remaining:
                break
            norm_map = {
                _normalize_filename(os.path.basename(f['path'])): os.path.basename(f['path'])
                for f in folder_files
                if not f['path'].endswith('/')
            }
            found_here, still_remaining = [], []
            for name in remaining:
                if _normalize_filename(name) in norm_map:
                    found_here.append(name)
                else:
                    still_remaining.append(name)
            if found_here:
                found_in_extra[folder_name] = found_here
            remaining = still_remaining
        missing_in_bucket_list = remaining

    result['found_in_extra_folders'] = {k: len(v) for k, v in found_in_extra.items()}

    # ── 5. Build file_match_map ───────────────────────────────────────
    file_match_map = {}
    for name in matched:
        file_match_map[name] = {'type': 'exact', 'bucket_files': [name]}
    for pm in partial_matches:
        file_match_map[pm['csv_name']] = {'type': pm['match_type'], 'bucket_files': pm['bucket_names']}
    for fm in fuzzy_matches:
        file_match_map[fm['csv_name']] = {'type': fm['match_type'], 'bucket_files': [fm['bucket_name']]}
    for pm in prefix_matches:
        file_match_map[pm['csv_name']] = {'type': pm['match_type'], 'bucket_files': pm['bucket_names']}
    for folder_name, found_files in found_in_extra.items():
        for name in found_files:
            file_match_map[name] = {'type': f'found_in_{folder_name}', 'bucket_files': [name]}
    for name in missing_in_bucket_list:
        file_match_map[name] = {'type': 'missing_in_bucket', 'bucket_files': []}

    # ── 6. SAMPLE ↔ DATA join and row building ────────────────────────
    use_sample = result['sample_csv_found'] and result['sample_id_col_found']
    all_sample_keys = set(sample_ids_from_sample.keys())
    all_data_keys = set(data_by_sample.keys())
    in_both = all_sample_keys & all_data_keys if use_sample else set()
    in_sample_only = (all_sample_keys - all_data_keys) if use_sample else set()
    in_data_only = (all_data_keys - all_sample_keys) if use_sample else set()
    data_keys_to_show = in_both | in_data_only if use_sample else all_data_keys

    # Fuzzy SAMPLE↔DATA matching: case + separator (_/-/ ) differences only.
    # Strips all separator chars before comparing — e.g. "WT 2_12mo" == "WT2_12mo".
    sample_data_fuzzy_pairs = {}  # sample_key (lower) → data_key (lower)
    if use_sample:
        norm_sample = {_norm_sample_id(k): k for k in in_sample_only}
        norm_data = {_norm_sample_id(k): k for k in in_data_only}
        for norm_key, s_key in norm_sample.items():
            if norm_key in norm_data:
                sample_data_fuzzy_pairs[s_key] = norm_data[norm_key]
        in_sample_only -= set(sample_data_fuzzy_pairs.keys())
        in_data_only -= set(sample_data_fuzzy_pairs.values())

    rows = []

    def _make_file_row(sample_id_sample, sample_id_data, entry):
        match = file_match_map.get(entry['file_name'], {'type': 'missing_in_bucket', 'bucket_files': []})
        bucket_file = ', '.join(match['bucket_files']) if match['bucket_files'] else '—'
        return {
            'sample_id_sample': sample_id_sample,
            'sample_id_data': sample_id_data,
            'file_name': entry['file_name'],
            'bucket_file': bucket_file,
            'match_type': match['type'],
        }

    for key in sorted(in_both):
        for entry in data_by_sample[key]:
            rows.append(_make_file_row(sample_ids_from_sample[key], entry['sample_id'], entry))

    for key in sorted(in_sample_only):
        rows.append({
            'sample_id_sample': sample_ids_from_sample[key],
            'sample_id_data': '—',
            'file_name': '—',
            'bucket_file': '—',
            'match_type': 'in_sample_only',
        })

    for key in sorted(in_data_only):
        for entry in data_by_sample[key]:
            row = _make_file_row('—', entry['sample_id'], entry)
            row['match_type'] = 'in_data_only'
            rows.append(row)

    for s_key, d_key in sorted(sample_data_fuzzy_pairs.items()):
        for entry in data_by_sample[d_key]:
            row = _make_file_row(sample_ids_from_sample[s_key], entry['sample_id'], entry)
            row['match_type'] = 'sample_id_fuzzy'
            rows.append(row)

    # DATA-only mode (no SAMPLE.csv): show all data rows
    if not use_sample:
        for key in sorted(all_data_keys):
            for entry in data_by_sample[key]:
                rows.append(_make_file_row('—', entry['sample_id'], entry))

    for name in in_bucket_only_final:
        rows.append({
            'sample_id_sample': '—',
            'sample_id_data': '—',
            'file_name': '—',
            'bucket_file': name,
            'match_type': 'only_in_bucket',
        })

    result['rows'] = rows
    result['n_in_sample_only'] = len(in_sample_only)
    result['n_in_data_only'] = len(in_data_only)
    result['n_sample_data_fuzzy'] = len(sample_data_fuzzy_pairs)
    result['n_only_bucket'] = len(in_bucket_only_final)

    for row in rows:
        mt = row['match_type']
        if mt == 'exact':
            result['n_exact'] += 1
        elif 'illumina' in mt:
            result['n_partial'] += 1
        elif mt in ('separator_mismatch', 'numeric_suffix_mismatch'):
            result['n_fuzzy'] += 1
        elif 'prefix' in mt:
            result['n_prefix'] += 1
        elif mt.startswith('found_in_'):
            result['n_found_in_extra'] += 1
        elif mt == 'missing_in_bucket':
            result['n_missing_bucket'] += 1

    # ── 7. Issues ─────────────────────────────────────────────────────
    if result['n_missing_bucket']:
        result['issues'].append(
            f"{result['n_missing_bucket']} file(s) in {data_csv_name} not found in bucket"
        )
    if result['n_only_bucket']:
        result['issues'].append(
            f"{result['n_only_bucket']} file(s) in bucket not listed in {data_csv_name}"
        )
    if result['n_in_sample_only']:
        result['issues'].append(
            f"{result['n_in_sample_only']} sample_id(s) in SAMPLE.csv not found in {data_csv_name}"
        )
    if result['n_in_data_only']:
        result['issues'].append(
            f"{result['n_in_data_only']} sample_id(s) in {data_csv_name} not found in SAMPLE.csv"
        )

    return result


def write_data_inconsistencies_tsv(result: dict, tsv_path: Path) -> Path | None:
    """
    Write the three-way inconsistencies table to a TSV file.

    Parameters
    ----------
    result : dict
        Dict returned by `check_three_way_consistency`.
    tsv_path : Path
        Destination path for the TSV file.

    Returns
    -------
    Path
        Path to the written TSV, or None if there were no rows.
    """
    rows = [r for r in result.get('rows', []) if r.get('match_type') != 'exact']
    if not rows:
        return None
    with open(tsv_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh, delimiter='\t')
        writer.writerow([
            'SAMPLE.sample_id', 'DATA.sample_id', 'DATA.file_name',
            'Bucket file(s)', 'Match type',
        ])
        for row in rows:
            writer.writerow([
                row['sample_id_sample'],
                row['sample_id_data'],
                row['file_name'],
                row['bucket_file'],
                row['match_type'],
            ])
    return tsv_path


def render_three_way_report(outfile, result: dict, raw_label: str, data_csv_name: str) -> None:
    """
    Write the Sample / Data / Bucket Consistency section to a Markdown report.

    Parameters
    ----------
    outfile : file-like object
        Open writable file to which Markdown is appended.
    result : dict
        Dict returned by `check_three_way_consistency`.
    raw_label : str
        Display label for the raw folder (e.g. 'raw' or 'fastqs').
    data_csv_name : str
        Name of the DATA file used in headings (e.g. 'DATA.csv').

    Returns
    -------
    None
    """
    outfile.write("### Sample / Data / Bucket Consistency\n\n")

    n_exact = result.get('n_exact', 0)
    n_partial = result.get('n_partial', 0)
    n_fuzzy = result.get('n_fuzzy', 0)
    n_prefix = result.get('n_prefix', 0)
    n_extra = result.get('n_found_in_extra', 0)
    extra_folders = result.get('found_in_extra_folders', {})
    n_missing = result.get('n_missing_bucket', 0)
    n_sample_only = result.get('n_in_sample_only', 0)
    n_data_only = result.get('n_in_data_only', 0)
    n_sample_fuzzy = result.get('n_sample_data_fuzzy', 0)
    n_only_bucket = result.get('n_only_bucket', 0)
    md5_files = result.get('md5_files', [])
    tsv_path = result.get('tsv_path')
    use_sample = result.get('sample_csv_found') and result.get('sample_id_col_found')

    summary_parts = []
    if n_exact:
        summary_parts.append(f"{emoji_success} **{n_exact} exact**")
    if n_partial:
        summary_parts.append(f"{emoji_warning} **{n_partial} partial** (Illumina suffix — need renaming)")
    if n_fuzzy:
        summary_parts.append(f"{emoji_warning} **{n_fuzzy} fuzzy** (filename typo)")
    if n_sample_fuzzy:
        summary_parts.append(f"{emoji_warning} **{n_sample_fuzzy} fuzzy** (sample_id separator/case)")
    if n_prefix:
        summary_parts.append(f"{emoji_warning} **{n_prefix} prefix match**")
    if n_extra:
        _folder_label = ", ".join(f"{f}/" for f in sorted(extra_folders)) if extra_folders else "extra folder"
        summary_parts.append(f"{emoji_warning} **{n_extra} found in {_folder_label}**")
    if n_missing:
        summary_parts.append(f"{emoji_error} **{n_missing} missing in bucket**")
    if n_sample_only:
        summary_parts.append(f"{emoji_error} **{n_sample_only} in SAMPLE not in DATA**")
    if n_data_only:
        summary_parts.append(f"{emoji_error} **{n_data_only} in DATA not in SAMPLE**")
    if n_only_bucket:
        summary_parts.append(f"{emoji_error} **{n_only_bucket} only in bucket**")

    has_issues = any([n_partial, n_fuzzy, n_sample_fuzzy, n_prefix, n_extra, n_missing, n_sample_only, n_data_only, n_only_bucket])
    summary = " · ".join(summary_parts)
    if md5_files:
        summary += f" · *{len(md5_files)} .md5 file(s) excluded*"
    outfile.write(summary + "\n\n")

    if not has_issues:
        outfile.write("✓ All entries consistent\n\n")
        if tsv_path:
            outfile.write(f"*Full table: `{Path(tsv_path).name}`*\n\n")
        return

    rows = result.get('rows', [])
    if not rows:
        return

    # Group non-exact rows by match_type category for display
    _DISPLAY_ORDER = [
        ('in_sample_only',          'In SAMPLE, absent from DATA'),
        ('in_data_only_group',      'In DATA, not in SAMPLE'),
        ('sample_data_fuzzy_group', 'Fuzzy match — SAMPLE/DATA sample_id (separator or case)'),
        ('missing_in_bucket',       'Missing in bucket'),
        ('separator_mismatch',      'Fuzzy match — filename separator (hyphen vs. underscore)'),
        ('numeric_suffix_mismatch', 'Fuzzy match — filename numeric suffix'),
        ('illumina_group',          'Partial match — Illumina suffix (need renaming)'),
        ('prefix_group',            'Prefix match — verify'),
        ('extra_group',             'Found in extra folder'),
        ('only_in_bucket',          'Only in bucket'),
    ]

    def _row_category(row):
        mt = row['match_type']
        if mt == 'in_sample_only':
            return 'in_sample_only'
        if mt == 'in_data_only':
            return 'in_data_only_group'
        if mt == 'sample_id_fuzzy':
            return 'sample_data_fuzzy_group'
        if mt == 'only_in_bucket':
            return 'only_in_bucket'
        if mt == 'missing_in_bucket':
            return 'missing_in_bucket'
        if mt in ('separator_mismatch', 'numeric_suffix_mismatch'):
            return mt
        if 'illumina' in mt:
            return 'illumina_group'
        if 'prefix' in mt:
            return 'prefix_group'
        if mt.startswith('found_in_'):
            return 'extra_group'
        return None

    groups = defaultdict(list)
    for row in rows:
        cat = _row_category(row)
        if cat and row['match_type'] != 'exact':
            groups[cat].append(row)

    col_header = (
        "| SAMPLE.sample_id | DATA.sample_id | DATA.file_name | Bucket file(s) | Note |\n"
        "|-----------------|----------------|----------------|----------------|------|\n"
    )

    def _note(row):
        mt = row['match_type']
        if mt == 'in_sample_only':
            return 'Not in DATA'
        if mt == 'in_data_only':
            return 'Not in SAMPLE'
        if mt == 'sample_id_fuzzy':
            return 'Fuzzy match — sample_id separator/case'
        if mt == 'only_in_bucket':
            return 'Only in bucket'
        if mt == 'missing_in_bucket':
            return 'Missing in bucket'
        if mt.startswith('found_in_'):
            folder = mt[len('found_in_'):]
            return f'Found in {folder}/'
        return mt.replace('_', ' ')

    wrote_table_header = False
    for cat_key, cat_label in _DISPLAY_ORDER:
        group = groups.get(cat_key, [])
        if not group:
            continue
        if not wrote_table_header:
            outfile.write(col_header)
            wrote_table_header = True
        for row in group[:_number_examples]:
            sid_s = row['sample_id_sample']
            sid_d = row['sample_id_data']
            fn = f"`{row['file_name']}`" if row['file_name'] != '—' else '—'
            bf = f"`{row['bucket_file']}`" if row['bucket_file'] != '—' else '—'
            outfile.write(f"| {sid_s} | {sid_d} | {fn} | {bf} | {_note(row)} |\n")
        n_hidden = len(group) - _number_examples
        if n_hidden > 0:
            outfile.write(f"| *... and {n_hidden} more ({cat_label})* | | | | |\n")

    if wrote_table_header:
        outfile.write("\n")

    if tsv_path:
        outfile.write(f"*Full reconciliation table: `{Path(tsv_path).name}`*\n\n")


# ── Report helpers ─────────────────────────────────────────────────────────────

def write_column_consistency_tsv(col_found_in: dict, tsv_path: Path, col_name: str) -> Path:
    """
    Write a binary presence matrix TSV for a column across metadata tables.

    Rows = unique raw values present in at least one table that are absent in at
    least one other table (only rows with a 0 are written). Columns = table names.
    Both rows and columns are sorted alphanumerically. The top-left cell is the
    column name.

    Parameters
    ----------
    col_found_in : dict
        Mapping of table_name -> set of raw (non-normalized) values for the column.
    tsv_path : Path
        Destination path for the TSV file.
    col_name : str
        Column name used as the top-left header cell.

    Returns
    -------
    Path or None
        Path to the written TSV, or None if there were no inconsistent rows.
    """
    all_values = sorted(set().union(*col_found_in.values()))
    table_names = sorted(col_found_in.keys())
    data_rows = []
    for value in all_values:
        row = [1 if value in col_found_in[t] else 0 for t in table_names]
        if 0 in row:
            data_rows.append([value] + row)
    if not data_rows:
        return None
    with open(tsv_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh, delimiter='\t')
        writer.writerow([col_name] + table_names)
        writer.writerows(data_rows)
    return tsv_path


def is_critical_issue(issue: str) -> bool:
    """
    Return True if the issue string represents a critical problem.

    Critical conditions
    -------------------
    - BUCKET:          Bucket not accessible
    - METADATA:        Missing core CDE v4.x tables (missing + core)
    - METADATA:        Mandatory column (sample_id / subject_id) inconsistent across tables
    - DATA_VS_BUCKET:  sample_id(s) in SAMPLE.csv have no DATA rows
    - RAW:/METADATA:   Required folder missing (metadata/, raw/ or variant)
    - SPATIAL:         Required spatial/ folder missing (spatial datasets only)

    Parameters
    ----------
    issue : str
        Issue string as generated by `perform_bucket_validation`.

    Returns
    -------
    bool
    """
    issue_lower = issue.lower()

    # Bucket not accessible
    if issue_lower.startswith('bucket:'):
        return True

    # Missing core CDE v4.x metadata tables
    if (issue_lower.startswith('metadata:')
            and 'missing' in issue_lower
            and 'core' in issue_lower):
        return True

    # Mandatory column (sample_id / subject_id) inconsistent across tables
    if issue_lower.startswith('metadata:') and 'has inconsistent values' in issue_lower:
        return True

    # Bucket inconsistencies
    if "bucket inconsistencies" in issue_lower:
        return True

    # Required folder missing (metadata/, raw/ or variant, spatial/ for spatial datasets)
    if 'not found (required' in issue_lower:
        for folder in MANDATORY_FOLDERS:
            if folder.lower() in issue_lower:
                return True
        return 'spatial' in issue_lower

    return False


def get_important_warnings(result: dict) -> list[str]:
    """
    Return a list of important warning strings derived from a QC result.

    Important warning conditions
    ----------------------------
    - Folder name case mismatches (e.g. 'Metadata/' instead of 'metadata/').
    - DATA vs. Bucket has mismatches: partial matches, prefix matches, or files
      missing from either DATA or the bucket.
    - Unexpected folders not in the list of known bucket folders.

    Parameters
    ----------
    result : dict
        QC result dictionary from `perform_bucket_validation`.

    Returns
    -------
    list of str
    """
    warnings = []

    # Folder name case mismatches
    case_warnings = result.get('case_warnings', [])
    if case_warnings:
        parts = [f"'{w['found']}' → '{w['expected']}'" for w in case_warnings]
        warnings.append(f"Folder name case mismatch(es) — {', '.join(parts)}")

    # Three-way inconsistencies, captured to report as important warnings or critical issues
    three_way = result.get('three_way_check', {})
    if three_way.get('data_found'):
        n_partial = three_way.get('n_partial', 0)
        n_fuzzy = three_way.get('n_fuzzy', 0)
        n_sample_fuzzy = three_way.get('n_sample_data_fuzzy', 0)
        n_prefix = three_way.get('n_prefix', 0)
        n_found_in_extra = three_way.get('n_found_in_extra', 0)
        extra_folders = three_way.get('found_in_extra_folders', {})
        if any([n_partial, n_fuzzy, n_sample_fuzzy, n_prefix, n_found_in_extra]):
            parts = []
            if n_partial:
                parts.append(f"{n_partial} partial match(es) (need renaming)")
            if n_fuzzy:
                parts.append(f"{n_fuzzy} fuzzy filename match(es) (typo)")
            if n_sample_fuzzy:
                parts.append(f"{n_sample_fuzzy} fuzzy sample_id match(es) (separator/case)")
            if n_prefix:
                parts.append(f"{n_prefix} prefix match(es) (verify)")
            if n_found_in_extra:
                _folder_label = ", ".join(f"{f}/" for f in sorted(extra_folders)) if extra_folders else "extra folder"
                parts.append(f"{n_found_in_extra} found in {_folder_label}")
            warnings.append("Sample / Data / Bucket inconsistencies — " + ", ".join(parts))

    # Unexpected folders
    unexpected = result.get('unexpected_folders', {})
    if unexpected:
        names = ', '.join(f"'{v}'" for v in sorted(unexpected.values()))
        warnings.append(f"{len(unexpected)} unexpected folder(s) — must be manually checked: {names}")

    return warnings


def _write_executive_summary_md(outfile, result: dict) -> None:
    """
    Write the Executive Summary block to an open Markdown file handle.

    Parameters
    ----------
    outfile : file-like object
        Open file handle to write Markdown to.
    result : dict
        QC result dictionary from `perform_bucket_validation`.

    Returns
    -------
    None
    """
    critical_issues = [i for i in result['issues'] if is_critical_issue(i)]
    important_warnings = get_important_warnings(result)

    outfile.write("### Executive Summary\n\n")

    if critical_issues:
        outfile.write(f"{emoji_error} Critical Issues ({len(critical_issues)})\n\n")
        for issue in critical_issues:
            outfile.write(f"- {issue}\n")
        outfile.write("\n")
    else:
        outfile.write(f"{emoji_success} No critical issues\n\n")

    if important_warnings:
        outfile.write(f"{emoji_warning} Important Warnings ({len(important_warnings)})\n\n")
        for w in important_warnings:
            outfile.write(f"- {w}\n")
        outfile.write("\n")
    else:
        outfile.write(f"{emoji_success} No important warnings\n\n")


def print_executive_summary(result: dict) -> None:
    """
    Print the Executive Summary for a QC result to stdout.

    Parameters
    ----------
    result : dict
        QC result dictionary from `perform_bucket_validation`.

    Returns
    -------
    None
    """
    critical_issues = [i for i in result['issues'] if is_critical_issue(i)]
    important_warnings = get_important_warnings(result)

    print(f"{_log_divider}")
    print("Executive Summary")
    print(f"{_log_divider}")

    if critical_issues:
        print(f"{emoji_error} Critical Issues ({len(critical_issues)})")
        for issue in critical_issues:
            print(f"  - {issue}")
    else:
        print(f"{emoji_success} No critical issues")

    if important_warnings:
        print(f"\n{emoji_warning} Important Warnings ({len(important_warnings)})")
        for w in important_warnings:
            print(f"  - {w}")
    else:
        print(f"{emoji_success} No important warnings")

    print(f"{_log_divider}\n")


def _folder_mismatch_note(issues: list, folder_prefix: str) -> str:
    """
    Return a short mismatch note for a folder, or empty string if none.

    Parameters
    ----------
    issues : list of str
        Issues list from a QC result dict.
    folder_prefix : str
        Folder name used as the issue prefix (e.g. 'metadata').

    Returns
    -------
    str
        Note like "found as 'Metadata'", or '' if no mismatch found.
    """
    prefix = folder_prefix.upper() + ':'
    matches = [i for i in issues if i.upper().startswith(prefix) and 'mismatch' in i.lower()]
    if not matches:
        return ''
    parts = matches[0].split("found '")
    found_name = parts[1].split("'")[0] if len(parts) > 1 else '?'
    return f"found as '{found_name}'"


def _folder_found_status(note: str) -> str:
    """Return a Found status string, with warning emoji if there is a mismatch note."""
    return f"{emoji_warning} Found ({note})" if note else f"{emoji_success} Found"


def _folder_not_found_status(folder: str, mandatory: set, override_msg: str = '') -> str:
    """Return the appropriate 'not found' status string for a folder."""
    if override_msg:
        return f"{emoji_error} NOT FOUND ({override_msg})"
    if folder in mandatory:
        return f"{emoji_error} NOT FOUND (REQUIRED)"
    return f"{emoji_warning} Not found (optional)"


def _get_raw_display(result: dict) -> tuple:
    """
    Return (raw_variant, display_text) for the raw folder in a QC result.

    Parameters
    ----------
    result : dict
        QC result dictionary from `perform_bucket_validation`.

    Returns
    -------
    tuple
        raw_variant : str
        display_text : str
    """
    raw_variant = result.get('raw_folder_variant', 'raw')
    display_text = RAW_ALTERNATIVES.get(raw_variant, raw_variant)
    return raw_variant, display_text


# ── Orchestration ──────────────────────────────────────────────────────────────

def perform_bucket_validation(gs_bucket: str,
               outdir: Path,
               save_metadata: bool = False) -> dict:
    """
    Perform pre-QC on a single GCS bucket and return results.

    Parameters
    ----------
    gs_bucket : str
        GCS bucket URL.
    outdir : Path
        Output directory for TSV files and temp metadata.
    save_metadata : bool
        If True, keep downloaded metadata after processing.

    Returns
    -------
    dict
        QC results including issues, folder analysis, and metadata analysis.
    """
    print(f"\n{_log_divider}")
    print(f"Processing: {gs_bucket}")
    print(f"{_log_divider}\n")

    bucket_name = gs_bucket.removeprefix("gs://")
    is_spatial = 'spatial' in bucket_name.lower()

    results = {
        'gs_bucket': gs_bucket,
        'bucket_name': bucket_name,
        'timestamp': datetime.now().isoformat(),
        'is_spatial': is_spatial,
        'issues': [],
        'metadata': {},
        'folders': {},
    }

    temp_dir = outdir / f"temp_{bucket_name}"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(exist_ok=True, parents=True)

    # Resolve billing project once — needed by gsutil for requester-pays buckets.
    # gcloud storage handles this automatically; gsutil requires explicit -u PROJECT.
    _billing_project = subprocess.run(
        ['gcloud', 'config', 'get-value', 'project'],
        capture_output=True, text=True,
    ).stdout.strip()
    if not _billing_project:
        raise RuntimeError(
            "No active gcloud project configured — required for "
            "requester-pays bucket access via gsutil. "
            "Run: gcloud config set project PROJECT_ID"
        )

    try:
        try:
            validate_raw_bucket_and_folder_existence(gs_bucket)
        except ValueError as e:
            results['issues'].append(f"BUCKET: {e}")
            return results

        structure, folder_name_map, case_warnings = list_bucket_structure(
            gs_bucket, temp_dir, save_metadata, CASE_FOLDERS
        )

        for warning in case_warnings:
            expected_key = warning['expected']
            display_expected = RAW_ALTERNATIVES.get(expected_key, expected_key)
            results['issues'].append(
                f"{warning['found'].upper()}: Folder name mismatch - "
                f"found '{warning['found']}', expected '{display_expected}'"
            )

        results['case_warnings'] = case_warnings

        has_metadata = 'metadata' in structure
        metadata_folder_name = folder_name_map.get('metadata', 'metadata')

        has_raw = False
        raw_folder_name = None
        raw_folder_variant = None

        for alt_name in RAW_ALTERNATIVES.keys():
            if alt_name in structure:
                has_raw = True
                raw_folder_name = folder_name_map.get(alt_name, alt_name)
                raw_folder_variant = alt_name
                break

        has_artifacts = 'artifacts' in structure
        artifacts_folder_name = folder_name_map.get('artifacts', 'artifacts')
        has_spatial = 'spatial' in structure
        spatial_folder_name = folder_name_map.get('spatial', 'spatial')

        unexpected_folders = {
            k: folder_name_map.get(k, k)
            for k in structure
            if k not in set(CASE_FOLDERS)
        }

        results['has_metadata'] = has_metadata
        results['has_raw'] = has_raw
        results['raw_folder_variant'] = raw_folder_variant
        results['has_artifacts'] = has_artifacts
        results['has_spatial'] = has_spatial
        results['unexpected_folders'] = unexpected_folders

        # METADATA CHECK
        metadata_dir = None
        metadata_renames = []
        if has_metadata:
            local_metadata_dir = temp_dir / "metadata"
            local_metadata_dir.mkdir(exist_ok=True, parents=True)
            remote_metadata = f"{gs_bucket}/{metadata_folder_name}/"
            print(f"  Downloading metadata from {remote_metadata}...")
            try:
                gsync(remote_metadata, str(local_metadata_dir), dry_run=False)
                metadata_dir = local_metadata_dir
                metadata_renames = strip_metadata_suffixes(local_metadata_dir)
            except subprocess.CalledProcessError as e:
                if 'matched more than one url' in (e.stderr or '').lower():
                    # Bucket versioning causes gcloud storage rsync to see both live and
                    # noncurrent versions of the metadata/ placeholder object. Fall back
                    # to gsutil rsync, which ignores noncurrent versions by default.
                    print(f"    Retrying with gsutil (bucket versioning conflict)...")
                    try:
                        result = subprocess.run(
                            ['gsutil', '-u', _billing_project, '-m', 'rsync', '-r',
                             remote_metadata, str(local_metadata_dir)],
                            check=True, capture_output=True, text=True,
                        )
                        print(result.stdout + result.stderr)
                        metadata_dir = local_metadata_dir
                        metadata_renames = strip_metadata_suffixes(local_metadata_dir)
                    except subprocess.CalledProcessError as e2:
                        print(f"    Warning: Could not download metadata: {e2.stderr}")
                else:
                    print(f"    Warning: Could not download metadata: {e.stderr}")
            metadata_results = analyze_metadata(metadata_dir, MIN_CSV_ROWS)
            results['metadata'] = metadata_results
            results['metadata_renames'] = metadata_renames
            found_csv_names = set(metadata_results.get('csv_files', {}).keys())
            found_csv_names_upper = {name.upper() for name in found_csv_names}

            legacy_provided = {}
            for f in found_csv_names:
                if not f.lower().endswith('.csv'):
                    continue
                core = TABLE_UPDATE_MAP_UPPER.get(f[:-4].upper())
                if core:
                    legacy_provided.setdefault(core, []).append(f)

            missing_core = []
            file_statuses = []
            for f in CORE_METADATA_FILES:
                core_upper = f[:-4].upper()
                label = f[:-4] if f.lower().endswith('.csv') else f
                if f.upper() in found_csv_names_upper or core_upper in legacy_provided:
                    file_statuses.append(f"{emoji_success} {label}")
                else:
                    missing_core.append(f)
                    file_statuses.append(f"{emoji_error} {label}")

            if missing_core:
                results['issues'].append(
                    f"METADATA: Missing {len(missing_core)} core CDE v4.x table(s) — {' · '.join(file_statuses)}"
                )

            col_check = check_mandatory_column_consistency(metadata_dir, MANDATORY_COLS_PER_TABLE)
            results['mandatory_col_check'] = col_check
            for entry in col_check:
                if emoji_error in (entry['presence_status'], entry['values_status']):
                    results['issues'].append(
                        f"METADATA: '{entry['column_header']}' has inconsistent values"
                    )
                if entry.get('col_found_in'):
                    tsv_path = outdir / f"{entry['column_header']}_issues.tsv"
                    tsv_written = write_column_consistency_tsv(entry['col_found_in'], tsv_path, entry['column_header'])
                    if tsv_written:
                        print(f"  Saved {entry['column_header']} consistency matrix to: {tsv_path}")
                        entry['tsv_path'] = str(tsv_path)

            if metadata_results['issues']:
                results['issues'].extend([f"METADATA: {issue}" for issue in metadata_results['issues']])
        else:
            if 'metadata' in MANDATORY_FOLDERS:
                results['issues'].append("METADATA: Folder not found (REQUIRED)")

        # RAW FOLDER CHECK
        if has_raw:
            print(f"  Analysing '{raw_folder_name}/' folder...")
            raw_analysis = analyze_folder(structure[raw_folder_variant], gs_bucket, NUMBER_SUBDIRS, MIN_FILE_SIZE_BYTES)
            results['folders']['raw'] = raw_analysis

            if (raw_folder_variant != 'raw' and
                    '[' in RAW_ALTERNATIVES[raw_folder_variant]):
                results['issues'].append(
                    f"RAW: Using '{raw_folder_variant}' folder name - "
                    f"{RAW_ALTERNATIVES[raw_folder_variant]}"
                )

            if raw_folder_variant in ['fastq', 'fastqs'] and raw_analysis['has_subfolders']:
                results['issues'].append(
                    f"RAW: Contains {raw_analysis['subfolder_count']} subfolder(s) - "
                    f"files should preferably be at the root of /{raw_folder_name}/"
                )

            if raw_analysis['potentially_empty']:
                count = len(raw_analysis['potentially_empty'])
                results['issues'].append(
                    f"RAW: {count} potentially empty {'file' if count == 1 else 'files'}"
                )
        else:
            if 'raw' in MANDATORY_FOLDERS:
                expected_names = "', '".join(RAW_ALTERNATIVES.keys())
                results['issues'].append(f"RAW: Folder not found (REQUIRED) - expected '{expected_names}'")

        # Three-way consistency: SAMPLE.sample_id ↔ DATA.sample_id/file_name ↔ bucket files
        _has_extra_for_spatial = is_spatial and (has_spatial or has_artifacts)
        if has_metadata and (has_raw or _has_extra_for_spatial) and metadata_dir:
            extra_folder_files = None
            if is_spatial:
                _extra = {}
                if 'spatial' in structure:
                    _extra['spatial'] = structure['spatial']
                if 'artifacts' in structure:
                    _extra['artifacts'] = structure['artifacts']
                if _extra:
                    extra_folder_files = _extra
            _raw_files = structure[raw_folder_variant] if has_raw else []
            _raw_label = f"/{raw_folder_name}/" if has_raw else "(no raw folder)"
            print(f"  Running three-way consistency check (SAMPLE / {data_file_name} / {_raw_label})...")
            three_way = check_three_way_consistency(
                metadata_dir, _raw_files, data_file_name, extra_folder_files=extra_folder_files
            )
            results['three_way_check'] = three_way

            if not three_way['data_found']:
                results['issues'].append(
                    f"Sample / Data / Bucket inconsistencies —  {data_file_name} not found in metadata/ - file comparison skipped"
                )
            else:
                tsv_path = outdir / "data_inconsistencies.tsv"
                tsv_written = write_data_inconsistencies_tsv(three_way, tsv_path)
                if tsv_written:
                    print(f"  Saved consistency table to: {tsv_path}")
                    three_way['tsv_path'] = str(tsv_written)
                # Forward non-count errors (read errors, missing columns)
                for issue in three_way.get('issues', []):
                    if not re.match(r'^\d+', issue):
                        results['issues'].append(f"Sample / Data / Bucket inconsistencies — {issue}")

                # ONE combined critical issue report
                critical_parts = []
                if three_way.get('n_missing_bucket'):
                    critical_parts.append(f"{three_way['n_missing_bucket']} missing in bucket")
                if three_way.get('n_only_bucket'):
                    critical_parts.append(f"{three_way['n_only_bucket']} only in bucket")
                if three_way.get('n_in_sample_only'):
                    critical_parts.append(f"{three_way['n_in_sample_only']} in SAMPLE not in DATA")
                if three_way.get('n_in_data_only'):
                    critical_parts.append(f"{three_way['n_in_data_only']} in DATA not in SAMPLE")
                if critical_parts:
                    results['issues'].append(
                        "Sample / Data / Bucket inconsistencies — " + ", ".join(critical_parts)
                    )

        # ARTIFACTS FOLDER CHECK
        if has_artifacts:
            print(f"  Analysing '{artifacts_folder_name}/' folder...")
            artifacts_analysis = analyze_folder(structure['artifacts'], gs_bucket, NUMBER_SUBDIRS, MIN_FILE_SIZE_BYTES)
            results['folders']['artifacts'] = artifacts_analysis
            if artifacts_analysis['potentially_empty']:
                count = len(artifacts_analysis['potentially_empty'])
                results['issues'].append(
                    f"ARTIFACTS: {count} potentially empty {'file' if count == 1 else 'files'}"
                )
        else:
            if 'artifacts' in MANDATORY_FOLDERS:
                results['issues'].append("ARTIFACTS: Folder not found (REQUIRED)")

        # SPATIAL FOLDER CHECK
        if is_spatial:
            if has_spatial:
                print(f"  Analysing '{spatial_folder_name}/' folder...")
                spatial_analysis = analyze_folder(structure['spatial'], gs_bucket, NUMBER_SUBDIRS, MIN_FILE_SIZE_BYTES)
                results['folders']['spatial'] = spatial_analysis
                if spatial_analysis['potentially_empty']:
                    count = len(spatial_analysis['potentially_empty'])
                    results['issues'].append(
                        f"SPATIAL: {count} potentially empty {'file' if count == 1 else 'files'}"
                    )
            else:
                results['issues'].append("SPATIAL: Folder not found (REQUIRED for spatial datasets)")

    finally:
        if not save_metadata and temp_dir.exists():
            print(f"  Cleaning up temp directory: {temp_dir}")
            shutil.rmtree(temp_dir)
        elif save_metadata and temp_dir.exists():
            print(f"  Metadata saved in: {temp_dir}")

    print(f"\n  Issues found: {len(results['issues'])}")
    if results['issues']:
        for issue in results['issues']:
            print(f"    - {issue}")
    else:
        print("    None")

    return results


def generate_report(all_results: list, report_path: Path) -> None:
    """
    Generate a Markdown pre-QC report for one or more datasets.

    Parameters
    ----------
    all_results : list of dict
        QC result dicts from `perform_bucket_validation`.
    report_path : Path
        Output path for the Markdown file.

    Returns
    -------
    None
    """
    with open(report_path, 'w') as outfile:
        outfile.write("# Bucket validation report\n\n")

        for result in all_results:
            outfile.write(f"### `{result['gs_bucket']}`\n\n")
            outfile.write("---\n\n")

            _write_executive_summary_md(outfile, result)

            outfile.write("---\n\n")

            outfile.write("### Overall Dataset Folder Status\n\n")
            outfile.write("| Folder name | Folder status | Content status |\n")
            outfile.write("|-------------|---------------|----------------|\n")

            if result.get('has_metadata'):
                note = _folder_mismatch_note(result['issues'], 'metadata')
                folder_status = _folder_found_status(note)
                renames = result.get('metadata_renames', [])
                csv_issues = result.get('metadata', {}).get('issues', [])
                content_parts = []
                if renames:
                    content_parts.append(f"{emoji_warning} {len(renames)} renamed")
                if csv_issues:
                    content_parts.append(f"{emoji_warning} {len(csv_issues)} CSV issue(s)")
                content_status = " · ".join(content_parts) if content_parts else emoji_success
            else:
                folder_status = _folder_not_found_status('metadata', set(MANDATORY_FOLDERS))
                content_status = "—"
            outfile.write(f"| metadata | {folder_status} | {content_status} |\n")

            if result.get('has_raw'):
                raw_variant, display_text = _get_raw_display(result)
                note = _folder_mismatch_note(result['issues'], raw_variant)
                if note:
                    folder_status = _folder_found_status(note)
                elif '[' in display_text:
                    folder_status = f"{emoji_warning} Found ({display_text})"
                else:
                    folder_status = f"{emoji_success} Found"
                raw_data = result.get('folders', {}).get('raw', {})
                raw_content_parts = []
                if raw_data.get('potentially_empty'):
                    raw_content_parts.append(f"{emoji_warning} {len(raw_data['potentially_empty'])} empty file(s)")
                if raw_data.get('has_subfolders'):
                    raw_content_parts.append(f"{emoji_warning} subfolders")
                content_status = " · ".join(raw_content_parts) if raw_content_parts else emoji_success
                folder_name_col = raw_variant
            else:
                folder_name_col = 'raw'
                folder_status = _folder_not_found_status('raw', set(MANDATORY_FOLDERS))
                content_status = "—"
            outfile.write(f"| {folder_name_col} | {folder_status} | {content_status} |\n")

            if result.get('has_artifacts'):
                note = _folder_mismatch_note(result['issues'], 'artifacts')
                folder_status = _folder_found_status(note)
                artifacts_data = result.get('folders', {}).get('artifacts', {})
                if artifacts_data.get('potentially_empty'):
                    content_status = f"{emoji_warning} {len(artifacts_data['potentially_empty'])} empty file(s)"
                else:
                    content_status = emoji_success
            else:
                folder_status = _folder_not_found_status('artifacts', set(MANDATORY_FOLDERS))
                content_status = "—"
            outfile.write(f"| artifacts | {folder_status} | {content_status} |\n")

            if result['is_spatial']:
                if result.get('has_spatial'):
                    note = _folder_mismatch_note(result['issues'], 'spatial')
                    folder_status = _folder_found_status(note)
                    spatial_data = result.get('folders', {}).get('spatial', {})
                    if spatial_data.get('potentially_empty'):
                        content_status = f"{emoji_warning} {len(spatial_data['potentially_empty'])} empty file(s)"
                    else:
                        content_status = emoji_success
                else:
                    folder_status = _folder_not_found_status('spatial', set(MANDATORY_FOLDERS), 'REQUIRED for spatial datasets')
                    content_status = "—"
                outfile.write(f"| spatial | {folder_status} | {content_status} |\n")

            for folder_display in sorted(result.get('unexpected_folders', {}).values()):
                outfile.write(f"| {folder_display} | {emoji_warning} Unexpected | — |\n")

            outfile.write("---\n\n")

            if result.get('metadata', {}).get('csv_files'):
                csv_files = result['metadata']['csv_files']

                status_counts = {}
                for csv_info in csv_files.values():
                    s = csv_info['status']
                    status_counts[s] = status_counts.get(s, 0) + 1

                outfile.write("### Metadata Details\n\n")

                summary_parts = []
                if status_counts.get('valid', 0):
                    summary_parts.append(f"{emoji_success} {status_counts['valid']} valid")
                if status_counts.get('insufficient', 0):
                    summary_parts.append(f"{emoji_warning} {status_counts['insufficient']} insufficient")
                if status_counts.get('error', 0):
                    summary_parts.append(f"{emoji_error} {status_counts['error']} error")
                renames = result.get('metadata_renames', [])
                if renames:
                    summary_parts.append(f"{emoji_warning} {len(renames)} renamed")
                outfile.write(f"**{len(csv_files)} file(s):** " + " · ".join(summary_parts) + "\n\n")

                rename_map = {
                    r['renamed']: r['original']
                    for r in result.get('metadata_renames', [])
                    if not r['skipped']
                }
                outfile.write("| TABLE | Rows | Original file name |\n")
                outfile.write("|-------|------|------------------|\n")
                for csv_name, csv_info in sorted(csv_files.items()):
                    stem_upper = csv_name[:-4].upper() if csv_name.lower().endswith('.csv') else csv_name.upper()
                    core_upper = TABLE_UPDATE_MAP_UPPER.get(stem_upper)
                    if core_upper:
                        table_name = core_upper
                        file_name_status = f"{emoji_warning} {csv_name}"
                    elif csv_name in rename_map:
                        table_name = stem_upper
                        file_name_status = f"{emoji_warning} {rename_map[csv_name]}"
                    else:
                        table_name = stem_upper
                        if stem_upper not in _KNOWN_TABLE_STEMS:
                            file_name_status = f"{emoji_error} {csv_name}"
                        else:
                            canonical = stem_upper + '.csv'
                            file_name_status = f"{emoji_success if csv_name == canonical else emoji_warning} {csv_name}"
                    outfile.write(f"| {table_name} | {csv_info['row_count']} | {file_name_status} |\n")

                col_check = result.get('mandatory_col_check', [])
                if col_check:
                    outfile.write("\n#### Mandatory Column Check\n\n")
                    outfile.write("| Column | Presence | Values | Details |\n")
                    outfile.write("|--------|----------|--------|---------|\n")
                    for entry in col_check:
                        details = entry['details']
                        if entry.get('tsv_path'):
                            details += f" — see `{Path(entry['tsv_path']).name}`"
                        outfile.write(
                            f"| `{entry['column_header']}` | {entry['presence_status']} "
                            f"| {entry['values_status']} | {details} |\n"
                        )
                    outfile.write("\n")
                    for entry in col_check:
                        tables = ', '.join(entry['tables_checked'])
                        outfile.write(f"*`{entry['column_header']}` — tables processed: {tables}*  \n")
                    outfile.write("\n")

                outfile.write("---\n\n")

            three_way = result.get('three_way_check', {})
            if three_way.get('data_found'):
                raw_variant, display_text = _get_raw_display(result)
                render_three_way_report(outfile, three_way, raw_variant, data_file_name)
            outfile.write("---\n\n")

            for folder_name, folder_data in result.get('folders', {}).items():
                if folder_name == 'raw':
                    raw_variant, display_text = _get_raw_display(result)
                    display_name = "Raw" if raw_variant == 'raw' else f"Raw ({display_text})"
                else:
                    display_name = folder_name.capitalize()

                outfile.write(f"### {display_name} Folder Breakdown\n\n")

                if folder_data.get('folder_structure'):
                    outfile.write("| Folder Path | Extension | Count |\n")
                    outfile.write("|-------------|-----------|-------|\n")
                    for folder_path in sorted(folder_data['folder_structure'].keys()):
                        extensions = folder_data['folder_structure'][folder_path]
                        for ext, count in sorted(extensions.items(), key=lambda x: x[1], reverse=True):
                            outfile.write(f"| {folder_path} | {ext} | {count} |\n")
                    outfile.write(f"| **TOTAL** | | **{folder_data['total_files']}** |\n")

                if folder_data['potentially_empty']:
                    outfile.write(f"{emoji_warning} **Potentially empty files:** {len(folder_data['potentially_empty'])}\n\n")
                    outfile.write("<details>\n")
                    outfile.write(f"<summary>Show potentially empty files ({len(folder_data['potentially_empty'])} total)</summary>\n\n")
                    for empty_file in folder_data['potentially_empty'][:20]:
                        outfile.write(f"- `{os.path.basename(empty_file['path'])}` ({empty_file['size']})\n")
                    if len(folder_data['potentially_empty']) > 20:
                        outfile.write(f"\n*... and {len(folder_data['potentially_empty']) - 20} more*\n")
                    outfile.write("\n</details>\n")
                else:
                    outfile.write("✓ No potentially empty files  \n")

                outfile.write("---\n\n")

        outfile.write("## Configuration\n\n")
        outfile.write(f"**Minimum file size threshold:** {MIN_FILE_SIZE_BYTES} bytes  \n")
        outfile.write(f"*Files smaller than this are flagged as potentially empty.*\n\n")
        outfile.write(f"**Minimum CSV rows required:** {MIN_CSV_ROWS}  \n")
        outfile.write(f"*metadata/CSV files must have at least this many rows (including header).*\n\n")
        outfile.write(f"**Mandatory folders:** {MANDATORY_DISPLAY}  \n")
        outfile.write(f"*All datasets must contain these folders.*\n\n")
        outfile.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n")

    print(f"\n{_log_divider}")
    print(f"QC Report generated: {report_path}")
    print(f"{_log_divider}\n")


# ── Module exports ─────────────────────────────────────────────────────────────

__all__ = [
    "MIN_FILE_SIZE_BYTES",
    "MIN_CSV_ROWS",
    "MANDATORY_FOLDERS",
    "RAW_ALTERNATIVES",
    "MANDATORY_COLS_PER_TABLE",
    "CORE_METADATA_FILES",
    "SUPP_METADATA_FILES",
    "emoji_success",
    "emoji_error",
    "emoji_warning",
    "strip_metadata_suffixes",
    "write_column_consistency_tsv",
    "write_data_inconsistencies_tsv",
    "analyze_metadata",
    "check_mandatory_column_consistency",
    "analyze_folder",
    "check_three_way_consistency",
    "render_three_way_report",
    "is_critical_issue",
    "get_important_warnings",
    "print_executive_summary",
    "perform_bucket_validation",
    "generate_report",
]


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--dataset-id",
        required=True,
        help="Single dataset ID, like: team-smith-sc-rnaseq\n"
    )
    parser.add_argument(
        "-o",
        "--outdir",
        default=None,
        help="Output directory path.\n"
             "If omitted, output is written to:\n"
             " asap-crn-cloud-dataset-metadata/datasets/<dataset_name>/bucket_validation/\n"
             "If provided, output goes to <outdir>/<dataset_name>/bucket_validation/."
    )
    parser.add_argument(
        "-m",
        "--save-metadata",
        action="store_true",
        default=False,
        help="Keep downloaded metadata files in temp directory after processing.\n"
             "Default: False (temporary files are deleted)."
    )

    args = parser.parse_args()
    save_metadata = args.save_metadata
    dataset_id = args.dataset_id

    if not dataset_id.startswith("team-"):
        parser.error(f"--dataset-id must start with 'team-', got: '{dataset_id}'")

    start_time = time.time()

    gs_bucket = f"gs://asap-raw-{dataset_id}"
    dataset_name = dataset_id.removeprefix("team-")

    if args.outdir:
        outdir = Path(os.path.expanduser(args.outdir)) / dataset_name / "bucket_validation"
    else:
        outdir = metadata_root / "datasets" / dataset_name / "bucket_validation"
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"Minimum file size threshold: {MIN_FILE_SIZE_BYTES} bytes")
    print(f"Minimum CSV rows required: {MIN_CSV_ROWS}")
    print(f"Mandatory folders: {MANDATORY_DISPLAY}")
    print(f"Subdirectory levels to display: {NUMBER_SUBDIRS}")
    print(f"Save metadata temp files: {'Yes' if save_metadata else 'No'}\n")

    try:
        result = perform_bucket_validation(gs_bucket, outdir, save_metadata)
        report_path = outdir / "bucket_validation.md"
        generate_report([result], report_path)
    except Exception as e:
        print(f"\nError processing {gs_bucket}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print_executive_summary(result)

    elapsed = time.time() - start_time
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = elapsed % 60

    print(f"\n{_log_divider}")
    if hours > 0:
        print(f"Total execution time: {hours}h {minutes}m {seconds:.2f}s")
    elif minutes > 0:
        print(f"Total execution time: {minutes}m {seconds:.2f}s")
    else:
        print(f"Total execution time: {seconds:.2f}s")
    print(f"{_log_divider}\n")

    log_run_command(outdir / f"{Path(__file__).stem}.log")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
