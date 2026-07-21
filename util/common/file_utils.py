#!/usr/bin/env python3
"""General-purpose functions to parse file properties (e.g. size, extension)."""

import csv
import os
import re
import statistics
from pathlib import Path


_SUPPORTED_DELIMITERS = [",", ";", "\t", "|"]
_ENCODINGS_TO_TRY = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
_DELIMITER_DETECTION_LINES = 50


def detect_csv_delimiter(file_path: Path, num_lines: int = _DELIMITER_DETECTION_LINES) -> str:
    """
    Detect the delimiter used in a CSV-like file using line-level statistics.

    Tries comma, semicolon, tab, and pipe. Scores each candidate by presence in
    the header, median count per line, and consistency across lines. Falls back
    to comma if no delimiter can be confidently identified.

    Adapted from DelimiterHandler.detect_delimiter() in crn-meta-validate, with
    all Streamlit dependencies removed.

    Parameters
    ----------
    file_path : Path
        Path to the file to inspect.
    num_lines : int
        Maximum number of non-empty lines to evaluate. Default is 50.

    Returns
    -------
    str
        Detected delimiter character. Defaults to ',' if detection is inconclusive.
    """
    try:
        raw = file_path.read_bytes()
    except Exception:
        return ","

    decoded = None
    for enc in _ENCODINGS_TO_TRY:
        try:
            decoded = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if decoded is None:
        decoded = raw.decode("utf-8", errors="ignore")

    lines = [line for line in decoded.splitlines() if line.strip()]
    if not lines:
        return ","

    header_line = lines[0]
    candidate_lines = lines[:max(2, min(len(lines), num_lines))]

    scores = {}
    for delim in _SUPPORTED_DELIMITERS:
        if delim not in header_line:
            scores[delim] = -1.0
            continue
        counts = [line.count(delim) for line in candidate_lines]
        if not counts:
            scores[delim] = -1.0
            continue
        median_count = statistics.median(counts)
        if median_count <= 0:
            scores[delim] = -1.0
            continue
        consistency = sum(1 for c in counts if c == median_count) / float(len(counts))
        est_cols = header_line.count(delim) + 1
        if est_cols <= 1:
            scores[delim] = -1.0
            continue
        scores[delim] = (consistency * 100.0) + float(median_count)

    best = max(scores, key=scores.get)
    return best if scores[best] >= 0 else ","


def parse_file_size_to_bytes(size_str: str) -> int:
    """
    Parse a human-readable size string to bytes.

    Parameters
    ----------
    size_str : str
        Human-readable size string, e.g. '1.5kiB', '2.3MiB'.

    Returns
    -------
    int
        Size in bytes.
    """
    size_str = size_str.strip()
    units = {'B': 1, 'kiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3, 'TiB': 1024**4}
    match_result = re.match(r'^([\d.]+)\s*([a-zA-Z]+)$', size_str)
    if match_result:
        number, unit = match_result.groups()
        return int(float(number) * units.get(unit, 1))
    try:
        return int(float(size_str))
    except ValueError:
        return 0


def get_file_extension(filepath: str) -> str:
    """
    Extract the file extension, stripping compression layers.

    Parameters
    ----------
    filepath : str
        File path or name (e.g. 'sample.fastq.gz', 'archive.tar.gz').

    Returns
    -------
    str
        Extension without leading dot (e.g. 'fastq', 'tar'), or 'no_extension'.
    """
    compressed_exts = ['.gz', '.bz2', '.xz', '.zip', '.Z']
    compression_removed = True
    while compression_removed:
        compression_removed = False
        for comp_ext in compressed_exts:
            if filepath.endswith(comp_ext):
                filepath = filepath[:-len(comp_ext)]
                compression_removed = True
                break
    if filepath.endswith('.tar'):
        filepath = filepath[:-4]
    _, ext = os.path.splitext(filepath)
    if not ext and 'tar' in os.path.basename(filepath):
        return 'tar'
    return ext.lstrip('.') if ext else 'no_extension'


def check_csv_rows(csv_path: Path, min_rows: int = 2) -> dict:
    """
    Check whether a CSV file has at least `min_rows` rows (header + data).

    The delimiter is auto-detected via `detect_csv_delimiter`.

    Parameters
    ----------
    csv_path : Path
        Path to the CSV file.
    min_rows : int
        Minimum required row count.

    Returns
    -------
    dict
        row_count : int
        has_data : bool
        status : str — 'valid', 'insufficient', or 'error'
        error : str or None
    """
    delimiter = detect_csv_delimiter(csv_path)
    try:
        for encoding in ('utf-8', 'latin-1'):
            try:
                with open(csv_path, 'r', encoding=encoding) as f:
                    row_count = sum(1 for _ in csv.reader(f, delimiter=delimiter))
                break
            except UnicodeDecodeError:
                continue
        return {
            'row_count': row_count,
            'rows': row_count,
            'has_data': row_count >= min_rows,
            'status': 'valid' if row_count >= min_rows else 'insufficient',
            'error': None,
        }
    except Exception as e:
        return {'row_count': 0, 'rows': 0, 'has_data': False, 'status': 'error', 'error': str(e)}
