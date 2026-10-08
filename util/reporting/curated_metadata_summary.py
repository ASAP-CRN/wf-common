#!/usr/bin/env python3
"""
Compute CRN Cloud dataset metrics from an ASAP *curated* bucket's CDE CSVs.

Called by crn_cloud_collection_summary once per released dataset (per the
Releases sheet) with the CSVs from gs://asap-curated-<dataset_id>/metadata/release/<version>/.
The curated bucket carries the same CDE tables the CRN Cloud is built from, so
the CRN Cloud Explorer is not queried.

Curated release copies are standardized to CDE v4.x (all released metadata was
brought to v4.x in the June 2026 Major release), so tables and columns are read
by their exact CDE names:
  - SAMPLE.ASAP_subject_id is the subject of every organism (ASAP_PMDBS_*,
    ASAP_MOUSE_*, ASAP_CELL_*); brain region is SAMPLE.region_level_1/2.
  - primary_diagnosis is CLINPATH-only (human); everything else is counted by
    SAMPLE.condition_id.
This helper is not meant for historical (pre-v4.x) release folders.

Cohort datasets (--cohort) mirror the per-team SAMPLE-table exclusions of the
SQL path: n_subjects_unique / n_samples_unique / n_samples_total are NA and no
subject, sample or sample-region membership rows are written.

Writes membership rows directly to the files passed in, and prints two
tab-separated lines on stdout:

    line 1: n_samples, n_subjects_unique, n_samples_unique, n_samples_total,
            n_brain_samples, n_brain_regions, n_brain_donors
    line 2: <25 diagnosis counts>, condition_counts

Line 2 is already in the exact shape the caller appends to the summary TSV.
"""

import argparse
import csv
import os
import sys

NA_VALUES = {"", "na", "n/a", "none", "null", "nan", "unknown", "not applicable"}

SAMPLE_ID = "ASAP_sample_id"
SUBJECT_ID = "ASAP_subject_id"
# region_level_3 is deliberately not used: it includes non-brain values (e.g.
# Intestine, Right colon, Enteric nervous system), so it can't mark a brain sample.
REGION_COLS = ("region_level_1", "region_level_2")


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def clean(value):
    """Strip quotes/whitespace and normalise CDE null placeholders to ''."""
    s = (value or "").strip().strip('"').strip()
    return "" if s.lower() in NA_VALUES else s


class Table:
    """A CDE CSV loaded into memory."""

    def __init__(self, rows, columns):
        self.rows = rows
        self.columns = set(columns)

    def __bool__(self):
        return bool(self.rows)

    def __len__(self):
        return len(self.rows)

    def has(self, *names):
        """True if any of the given columns exists."""
        return any(n in self.columns for n in names)

    def values(self, name, distinct=True):
        """Cleaned, non-empty values of a column ([] if the column is absent)."""
        if name not in self.columns:
            return []
        out = []
        seen = set()
        for r in self.rows:
            v = clean(r.get(name))
            if not v:
                continue
            if distinct:
                if v in seen:
                    continue
                seen.add(v)
            out.append(v)
        return out

    def pairs(self, a, b, distinct=True):
        """Cleaned (a, b) tuples for rows where a is non-empty; b may be ''."""
        if a not in self.columns:
            return []
        out = []
        seen = set()
        for r in self.rows:
            av = clean(r.get(a))
            bv = clean(r.get(b)) if b in self.columns else ""
            if not av:
                continue
            key = (av, bv)
            if distinct:
                if key in seen:
                    continue
                seen.add(key)
            out.append(key)
        return out


def load_table(metadata_dir, name):
    """Load <metadata_dir>/<name>.csv; an empty Table if the dataset has no such table."""
    path = os.path.join(metadata_dir, f"{name}.csv")
    if not os.path.exists(path):
        return Table([], [])
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return Table(list(reader), reader.fieldnames or [])


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def sample_regions(row):
    """(region_level_1, region_level_2) for a SAMPLE row, cleaned."""
    return tuple(clean(row.get(c)) for c in REGION_COLS)


def classify_origin(raw_bucket):
    """Mirror the bucket-name classifier used by the SQL path and calculate_breakdown."""
    b = raw_bucket.lower()
    if "human" in b or "pmdbs" in b:
        return "human"
    if "mouse" in b or "sulzer-fecal-metagenome-fp-spf" in b:
        return "mouse"
    if "cell" in b or "invitro" in b or "ipsc" in b or "mef" in b or "hesc" in b:
        return "cell"
    return "other"


def append_lines(path, lines):
    if not path or not lines:
        return
    with open(path, "a", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")


def tsv_safe(value):
    return str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")


def expand_diagnosis_counts(counts, labels):
    """Counts dict -> tab-separated values in the fixed DIAGNOSIS_COLS order."""
    lowered = {k.lower(): v for k, v in counts.items()}
    return "\t".join(str(lowered.get(label.lower(), 0)) for label in labels)


def format_condition_counts(counts):
    if not counts:
        return "NA"
    return "|".join(f"{k}:{v}" for k, v in sorted(counts.items()))


# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--slug", required=True)
    ap.add_argument("--metadata-dir", required=True)
    ap.add_argument("--raw-bucket", default="")
    ap.add_argument("--cohort", action="store_true",
                    help="Harmonized collection: skip per-team SAMPLE counts and membership rows")
    ap.add_argument("--diag-labels-file", required=True)
    ap.add_argument("--subject-membership", default="")
    ap.add_argument("--sample-membership", default="")
    ap.add_argument("--brain-donor-membership", default="")
    ap.add_argument("--subject-diagnosis-membership", default="")
    ap.add_argument("--sample-region-membership", default="")
    ap.add_argument("--human-ids", default="")
    ap.add_argument("--mouse-ids", default="")
    ap.add_argument("--cell-ids", default="")
    ap.add_argument("--brain-donor-ids", default="")
    ap.add_argument("--sample-ids", default="")
    ap.add_argument("--regions-out", default="")
    args = ap.parse_args()

    slug = args.slug
    with open(args.diag_labels_file, encoding="utf-8") as fh:
        diagnosis_labels = [l.rstrip("\n") for l in fh if l.strip()]

    sample = load_table(args.metadata_dir, "SAMPLE")
    assay = load_table(args.metadata_dir, "ASSAY")
    clinpath = load_table(args.metadata_dir, "CLINPATH")

    # ---- n_samples: distinct (sample, modality) from ASSAY, else distinct sample
    pairs = assay.pairs(SAMPLE_ID, "modality")
    n_samples = len(pairs) if pairs else "NA"
    if n_samples == "NA" and sample:
        vals = sample.values(SAMPLE_ID)
        n_samples = len(vals) if vals else "NA"

    # ---- SAMPLE-derived unique/total counts (per-team datasets only)
    n_subjects_unique = "NA"
    n_samples_unique = "NA"
    n_samples_total = "NA"
    if sample and not args.cohort:
        n_subjects_unique = len(sample.values(SUBJECT_ID))
        n_samples_unique = len(sample.values(SAMPLE_ID))
        n_samples_total = len(sample)

    # ---- Subject IDs for global dedup + subject membership
    origin = classify_origin(args.raw_bucket or slug)
    dest_ids = {
        "human": args.human_ids,
        "mouse": args.mouse_ids,
        "cell": args.cell_ids,
    }.get(origin, "")

    if origin != "other" and not args.cohort:
        subject_ids = sample.values(SUBJECT_ID)
        if subject_ids:
            append_lines(dest_ids, subject_ids)
            append_lines(args.subject_membership,
                         [f"{tsv_safe(i)}\t{tsv_safe(slug)}" for i in subject_ids])
        else:
            print(f"  [WARN] No {origin} IDs collected for {slug} from curated CSVs",
                  file=sys.stderr)

    # ---- Sample IDs for global dedup + sample membership
    sample_ids = sample.values(SAMPLE_ID) if not args.cohort else []
    if sample_ids:
        append_lines(args.sample_ids, sample_ids)
        append_lines(args.sample_membership,
                     [f"{tsv_safe(i)}\t{tsv_safe(slug)}" for i in sample_ids])

    # ---- Brain samples / regions from SAMPLE.region_level_1/2, else SAMPLE.tissue
    n_brain_samples = "NA"
    n_brain_regions = "NA"
    regions_seen = set()

    brain_samples = set()
    for r in sample.rows:
        sid = clean(r.get(SAMPLE_ID))
        a, b = sample_regions(r)
        # Datasets vary in which level they populate: some fill region_level_1,
        # others (e.g. the GeoMx spatial sets) only region_level_2. Count the
        # most general populated level so neither layout reports zero regions.
        if a or b:
            regions_seen.add(a or b)
            if sid:
                brain_samples.add(sid)
    if brain_samples:
        n_brain_samples = len(brain_samples)
        n_brain_regions = len(regions_seen)

    if n_brain_samples == "NA":
        brain_samples = {
            clean(r.get(SAMPLE_ID))
            for r in sample.rows
            if clean(r.get(SAMPLE_ID)) and "brain" in clean(r.get("tissue")).lower()
        }
        if brain_samples:
            n_brain_samples = len(brain_samples)

    if args.regions_out and regions_seen:
        append_lines(args.regions_out, sorted(regions_seen))

    # ---- Sample region membership rows (per-team datasets only)
    if args.sample_region_membership and not args.cohort:
        region_map = {}
        for r in sample.rows:
            sid = clean(r.get(SAMPLE_ID))
            a, b = sample_regions(r)
            if sid and (a or b):
                region_map[sid] = (clean(r.get(SUBJECT_ID)), a, b)
        rows = [
            "\t".join(tsv_safe(v) for v in (subj, sid, a, b, slug))
            for sid, (subj, a, b) in region_map.items()
        ]
        append_lines(args.sample_region_membership, rows)
        if rows:
            print(f"  Wrote {len(rows)} sample-region rows from curated CSVs", file=sys.stderr)

    # ---- Brain donors: CLINPATH subjects that also have a region-annotated SAMPLE row
    n_brain_donors = "NA"
    if clinpath:
        clin_subjects = set(clinpath.values(SUBJECT_ID))
        brain_subjects = {
            clean(r.get(SUBJECT_ID))
            for r in sample.rows
            if any(sample_regions(r)) and clean(r.get(SUBJECT_ID))
        }
        donors = sorted(clin_subjects & brain_subjects)
        n_brain_donors = len(donors)
        if donors:
            append_lines(args.brain_donor_ids, donors)
            append_lines(args.brain_donor_membership,
                         [f"{tsv_safe(d)}\t{tsv_safe(slug)}" for d in donors])

    # ---- Diagnosis / condition counts: CLINPATH.primary_diagnosis, else SAMPLE.condition_id
    diag_counts = {}
    diag_pairs = []          # (subject_id, diagnosis) for the membership file
    source = None

    for r in clinpath.rows:
        dx = clean(r.get("primary_diagnosis"))
        if not dx:
            continue
        diag_counts[dx] = diag_counts.get(dx, 0) + 1
        sv = clean(r.get(SUBJECT_ID))
        if sv:
            diag_pairs.append((sv, dx))
    if diag_counts:
        source = "CLINPATH"

    if not diag_counts:
        per_condition = {}
        for r in sample.rows:
            cv = clean(r.get("condition_id"))
            if not cv:
                continue
            sv = clean(r.get(SUBJECT_ID))
            per_condition.setdefault(cv, set()).add(sv or f"__row{id(r)}")
            if sv:
                diag_pairs.append((sv, cv))
        diag_counts = {k: len(v) for k, v in per_condition.items()}
        if diag_counts:
            source = "SAMPLE.condition_id"

    if source:
        print(f"  Diagnosis/condition source: {source} (curated CSV)", file=sys.stderr)

    primary_diagnosis_counts = expand_diagnosis_counts(diag_counts, diagnosis_labels)
    condition_counts = format_condition_counts(diag_counts)

    if diag_counts and set(primary_diagnosis_counts.split("\t")) == {"0"}:
        print(f"  [INFO] condition values not in diagnosis vocab — raw counts: "
              f"{condition_counts}", file=sys.stderr)

    # Per-subject diagnosis membership, human/pmdbs datasets only (matches SQL path)
    rb = (args.raw_bucket or slug).lower()
    if diag_pairs and ("human" in rb or "pmdbs" in rb):
        seen = set()
        rows = []
        for sv, dx in diag_pairs:
            if (sv, dx) in seen:
                continue
            seen.add((sv, dx))
            rows.append("\t".join(tsv_safe(v) for v in (sv, dx, slug)))
        append_lines(args.subject_diagnosis_membership, rows)

    print(f"  unique subjects={n_subjects_unique} "
          f"unique samples={n_samples_unique} total samples={n_samples_total}",
          file=sys.stderr)

    scalars = [n_samples, n_subjects_unique, n_samples_unique, n_samples_total,
               n_brain_samples, n_brain_regions, n_brain_donors]
    sys.stdout.write("\t".join(str(f) for f in scalars) + "\n")
    sys.stdout.write(primary_diagnosis_counts + "\t" + condition_counts + "\n")


if __name__ == "__main__":
    main()
