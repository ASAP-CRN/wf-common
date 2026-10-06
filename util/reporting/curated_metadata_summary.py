#!/usr/bin/env python3
"""
Compute CRN-Cloud-comparable dataset metrics from an ASAP *curated* bucket's CDE CSVs.

Used by crn_cloud_collection_summary as the fallback path when a slug is not
published in the CRN Cloud (dnastack raises UnknownCollectionError). The curated
bucket carries the same CDE tables the CRN Cloud SQL schema is built from, so the
same metrics can be derived directly from metadata/release/<version>/*.csv.

Differences from the SQL path that callers should be aware of:
  - CSV headers are ASAP_-prefixed CamelCase (ASAP_sample_id) whereas the SQL
    columns are lowercase (asap_sample_id). All lookups here are case-insensitive.
  - Legacy CDE v3/v4 exports carry an unnamed pandas index as the first column.
  - CDE v5 dropped MOUSE.csv/PMDBS.csv: subjects of every organism live in
    SUBJECT.csv, and brain region moved to SAMPLE.region_level_1/2.

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


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def clean(value):
    """Strip quotes/whitespace and normalise CDE null placeholders to ''."""
    s = (value or "").strip().strip('"').strip()
    return "" if s.lower() in NA_VALUES else s


class Table:
    """A CDE CSV loaded into memory with case-insensitive column access."""

    def __init__(self, rows, columns):
        self.rows = rows
        # lowercase name -> original header
        self.columns = {c.lower(): c for c in columns if c}

    def __bool__(self):
        return bool(self.rows)

    def __len__(self):
        return len(self.rows)

    def has(self, *names):
        """True if any of the given column names exists (case-insensitive)."""
        return any(n.lower() in self.columns for n in names)

    def col(self, *names):
        """Return the first matching original header, or None."""
        for n in names:
            key = n.lower()
            if key in self.columns:
                return self.columns[key]
        return None

    def values(self, *names, distinct=True):
        """Cleaned, non-empty values of the first matching column."""
        header = self.col(*names)
        if header is None:
            return []
        out = []
        seen = set()
        for r in self.rows:
            v = clean(r.get(header))
            if not v:
                continue
            if distinct:
                if v in seen:
                    continue
                seen.add(v)
            out.append(v)
        return out

    def pairs(self, a_names, b_names, distinct=True):
        """Cleaned (a, b) tuples where both columns exist; b may be ''."""
        a = self.col(*a_names)
        b = self.col(*b_names)
        if a is None:
            return []
        out = []
        seen = set()
        for r in self.rows:
            av = clean(r.get(a))
            bv = clean(r.get(b)) if b else ""
            if not av:
                continue
            key = (av, bv)
            if distinct:
                if key in seen:
                    continue
                seen.add(key)
            out.append(key)
        return out


def load_table(metadata_dir, *basenames):
    """
    Load the first CSV matching any of `basenames` (case-insensitive, with or
    without an extra suffix such as ASSAY_RNAseq). Returns an empty Table if
    nothing matches.
    """
    try:
        entries = os.listdir(metadata_dir)
    except OSError:
        return Table([], [])

    wanted = [b.lower() for b in basenames]
    match = None
    # Exact stem match wins over a prefixed variant (ASSAY before ASSAY_RNAseq)
    for entry in sorted(entries):
        if not entry.lower().endswith(".csv"):
            continue
        stem = entry[:-4].lower()
        if stem in wanted:
            match = entry
            break
    if match is None:
        for entry in sorted(entries):
            if not entry.lower().endswith(".csv"):
                continue
            stem = entry[:-4].lower()
            if any(stem.startswith(w + "_") for w in wanted):
                match = entry
                break
    if match is None:
        return Table([], [])

    path = os.path.join(metadata_dir, match)
    try:
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as fh:
            reader = csv.DictReader(fh)
            columns = [c for c in (reader.fieldnames or []) if c and c.strip()]
            rows = list(reader)
    except OSError:
        return Table([], [])
    return Table(rows, columns)


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

SUBJECT_ID_CANDIDATES = ("ASAP_subject_id", "ASAP_mouse_id", "ASAP_cell_id", "subject_id")


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
    subject = load_table(args.metadata_dir, "SUBJECT")
    mouse = load_table(args.metadata_dir, "MOUSE")
    cell = load_table(args.metadata_dir, "CELL")
    clinpath = load_table(args.metadata_dir, "CLINPATH")
    pmdbs = load_table(args.metadata_dir, "PMDBS")
    condition = load_table(args.metadata_dir, "CONDITION")

    # ---- n_samples: distinct (sample, modality) from ASSAY, else distinct sample
    if assay and assay.has("ASAP_sample_id"):
        pairs = assay.pairs(("ASAP_sample_id",), ("modality",))
        n_samples = len(pairs) if pairs else "NA"
    else:
        n_samples = "NA"
    if n_samples == "NA" and sample:
        vals = sample.values("ASAP_sample_id")
        n_samples = len(vals) if vals else "NA"

    # ---- SAMPLE-derived unique/total counts
    n_subjects_unique = "NA"
    n_samples_unique = "NA"
    n_samples_total = "NA"
    subject_col_used = "none"
    if sample:
        col = sample.col(*SUBJECT_ID_CANDIDATES)
        if col is not None:
            subject_col_used = col
            n_subjects_unique = len(sample.values(col))
        n_samples_unique = len(sample.values("ASAP_sample_id"))
        n_samples_total = len(sample)

    # ---- Subject IDs for global dedup + subject membership
    origin = classify_origin(args.raw_bucket or slug)
    dest_ids = {
        "human": args.human_ids,
        "mouse": args.mouse_ids,
        "cell": args.cell_ids,
    }.get(origin, "")

    subject_ids = []
    if origin != "other":
        # Priority mirrors the SQL path: organism-specific table first, then SAMPLE/SUBJECT.
        if origin == "mouse":
            search = [(mouse, ("ASAP_mouse_id",)), (sample, ("ASAP_subject_id", "ASAP_mouse_id")),
                      (subject, ("ASAP_subject_id", "ASAP_mouse_id"))]
        elif origin == "cell":
            search = [(cell, ("ASAP_cell_id",)), (sample, ("ASAP_subject_id", "ASAP_cell_id")),
                      (subject, ("ASAP_subject_id", "ASAP_cell_id"))]
        else:
            search = [(sample, ("ASAP_subject_id",)), (subject, ("ASAP_subject_id",))]
        for table, cols in search:
            if not table:
                continue
            vals = table.values(*cols)
            if vals:
                subject_ids = vals
                break

    if subject_ids:
        append_lines(dest_ids, subject_ids)
        append_lines(args.subject_membership,
                     [f"{tsv_safe(i)}\t{tsv_safe(slug)}" for i in subject_ids])
    else:
        print(f"  [WARN] No {origin} IDs collected for {slug} from curated CSVs",
              file=sys.stderr)

    # ---- Sample IDs for global dedup + sample membership
    sample_ids = sample.values("ASAP_sample_id") if sample else []
    if sample_ids:
        append_lines(args.sample_ids, sample_ids)
        append_lines(args.sample_membership,
                     [f"{tsv_safe(i)}\t{tsv_safe(slug)}" for i in sample_ids])

    # ---- Brain samples / regions
    # PMDBS.csv is the CDE v3/v4 source; v5 moved region onto SAMPLE.region_level_1/2.
    n_brain_samples = "NA"
    n_brain_regions = "NA"
    regions_seen = set()

    if pmdbs and pmdbs.has("ASAP_sample_id"):
        n_brain_samples = len(pmdbs.values("ASAP_sample_id"))
        regions_seen = set(pmdbs.values("brain_region"))
        n_brain_regions = len(regions_seen)
    elif sample and sample.has("region_level_1", "region_level_2"):
        rl1 = sample.col("region_level_1")
        rl2 = sample.col("region_level_2")
        sid_col = sample.col("ASAP_sample_id")
        brain_samples = set()
        for r in sample.rows:
            sid = clean(r.get(sid_col)) if sid_col else ""
            a = clean(r.get(rl1)) if rl1 else ""
            b = clean(r.get(rl2)) if rl2 else ""
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

    if n_brain_samples == "NA" and sample and sample.has("tissue"):
        tissue_col = sample.col("tissue")
        sid_col = sample.col("ASAP_sample_id")
        brain_samples = {
            clean(r.get(sid_col))
            for r in sample.rows
            if sid_col and clean(r.get(sid_col)) and "brain" in clean(r.get(tissue_col)).lower()
        }
        if brain_samples:
            n_brain_samples = len(brain_samples)

    if args.regions_out and regions_seen:
        append_lines(args.regions_out, sorted(regions_seen))

    # ---- Sample region membership rows
    if sample and args.sample_region_membership:
        sid_col = sample.col("ASAP_sample_id")
        subj_col = sample.col("ASAP_subject_id")
        rl1 = sample.col("region_level_1")
        rl2 = sample.col("region_level_2")
        region_map = {}
        if sid_col and (rl1 or rl2):
            for r in sample.rows:
                sid = clean(r.get(sid_col))
                if not sid:
                    continue
                region_map[sid] = (
                    clean(r.get(subj_col)) if subj_col else "",
                    clean(r.get(rl1)) if rl1 else "",
                    clean(r.get(rl2)) if rl2 else "",
                )
        # PMDBS.brain_region fills in only where SAMPLE had no region at all
        if pmdbs:
            p_sid = pmdbs.col("ASAP_sample_id")
            p_br = pmdbs.col("brain_region")
            if p_sid and p_br:
                for r in pmdbs.rows:
                    sid = clean(r.get(p_sid))
                    br = clean(r.get(p_br))
                    if not sid or not br:
                        continue
                    ex_subj, ex_a, ex_b = region_map.get(sid, ("", "", ""))
                    if not ex_a and not ex_b:
                        region_map[sid] = (ex_subj, br, "")
        rows = [
            "\t".join(tsv_safe(v) for v in (subj, sid, a, b, slug))
            for sid, (subj, a, b) in region_map.items()
            if a or b
        ]
        append_lines(args.sample_region_membership, rows)
        if rows:
            print(f"  Wrote {len(rows)} sample-region rows from curated CSVs", file=sys.stderr)

    # ---- Brain donors: CLINPATH subjects that also have a brain sample
    n_brain_donors = "NA"
    if clinpath and clinpath.has("ASAP_subject_id"):
        clin_subjects = set(clinpath.values("ASAP_subject_id"))
        brain_subjects = set()
        if pmdbs and sample:
            p_samples = set(pmdbs.values("ASAP_sample_id"))
            s_sid = sample.col("ASAP_sample_id")
            s_subj = sample.col("ASAP_subject_id")
            if s_sid and s_subj:
                for r in sample.rows:
                    if clean(r.get(s_sid)) in p_samples:
                        sv = clean(r.get(s_subj))
                        if sv:
                            brain_subjects.add(sv)
        elif sample and sample.has("region_level_1", "region_level_2"):
            rl1 = sample.col("region_level_1")
            rl2 = sample.col("region_level_2")
            s_subj = sample.col("ASAP_subject_id")
            if s_subj:
                for r in sample.rows:
                    a = clean(r.get(rl1)) if rl1 else ""
                    b = clean(r.get(rl2)) if rl2 else ""
                    if a or b:
                        sv = clean(r.get(s_subj))
                        if sv:
                            brain_subjects.add(sv)
        donors = sorted(clin_subjects & brain_subjects)
        n_brain_donors = len(donors)
        if donors:
            append_lines(args.brain_donor_ids, donors)
            append_lines(args.brain_donor_membership,
                         [f"{tsv_safe(d)}\t{tsv_safe(slug)}" for d in donors])

    # ---- Diagnosis / condition counts, same priority order as the SQL path
    diag_counts = {}
    diag_pairs = []          # (subject_id, diagnosis) for the membership file
    source = None

    for table, name in ((clinpath, "CLINPATH"), (subject, "SUBJECT")):
        if not table or not table.has("primary_diagnosis"):
            continue
        dx_col = table.col("primary_diagnosis")
        subj_col = table.col("ASAP_subject_id", "subject_id")
        for r in table.rows:
            dx = clean(r.get(dx_col))
            if not dx:
                continue
            diag_counts[dx] = diag_counts.get(dx, 0) + 1
            if subj_col:
                sv = clean(r.get(subj_col))
                if sv:
                    diag_pairs.append((sv, dx))
        if diag_counts:
            source = name
            break

    if not diag_counts and sample and sample.has("condition_id"):
        cond_col = sample.col("condition_id")
        subj_col = sample.col(*SUBJECT_ID_CANDIDATES)
        per_condition = {}
        for r in sample.rows:
            cv = clean(r.get(cond_col))
            if not cv:
                continue
            sv = clean(r.get(subj_col)) if subj_col else ""
            per_condition.setdefault(cv, set()).add(sv or f"__row{id(r)}")
            if sv:
                diag_pairs.append((sv, cv))
        diag_counts = {k: len(v) for k, v in per_condition.items()}
        if diag_counts:
            source = "SAMPLE.condition_id"

    if not diag_counts and condition:
        cond_col = condition.col("condition", "condition_id")
        if cond_col:
            for r in condition.rows:
                cv = clean(r.get(cond_col))
                if cv:
                    diag_counts[cv] = diag_counts.get(cv, 0) + 1
            if diag_counts:
                source = "CONDITION"

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

    print(f"  subject_col={subject_col_used} unique subjects={n_subjects_unique} "
          f"unique samples={n_samples_unique} total samples={n_samples_total}",
          file=sys.stderr)

    scalars = [n_samples, n_subjects_unique, n_samples_unique, n_samples_total,
               n_brain_samples, n_brain_regions, n_brain_donors]
    sys.stdout.write("\t".join(str(f) for f in scalars) + "\n")
    sys.stdout.write(primary_diagnosis_counts + "\t" + condition_counts + "\n")


if __name__ == "__main__":
    main()
