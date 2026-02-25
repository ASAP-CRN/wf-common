# util scripts

| Script | Description | Context | Example usage |
| :- | :- | :- | :- |
| [`common.py`](./common.py) | Common lists and functions used across scripts. | Ability to reuse common lists and functions. | NA |
| [`bucket_validation_utils.py`](./bucket_validation_utils.py) | Functions to validate raw bucket and local metadata structure and contents before transferring data. | Checks preceding data transfers. | NA |
| [`generate_inputs`](./generate_inputs) | Generate inputs JSON for WDL pipelines. | Ability to generate the inputs JSON for WDL pipelines given a project TSV (sample information), inputs JSON template, workflow name, and cohort dataset name. | `./generate_inputs --project-tsv lee.metadata.tsv --inputs-template inputs.json --workflow-name pmdbs_sc_rnaseq_analysis --cohort-dataset sc-rnaseq` |
| [`validate_raw_bucket_structure.py`](./validate_raw_bucket_structure.py) | Ensure that the raw bucket has the appropriate directories after contributor upload. | Contributions require at least the `metadata/` directory and minimal metadata .CSVs, and this will further check for additional optional contributed directories. | `python3 validate_raw_bucket_structure.py -t jakobsson -ds pmdbs-sn-rnaseq` |
| [`download_raw_bucket_metadata_to_local`](./download_raw_bucket_metadata_to_local) | Sync raw bucket metadata to the local metadata directory. | Once authors have contributed their metadata to the raw bucket, this script downloads this data locally so that QC can be performed. | `./download_raw_bucket_metadata_to_local -t jakobsson -ds pmdbs-sn-rnaseq` |
| [`transfer_qc_metadata_to_raw_bucket`](./transfer_qc_metadata_to_raw_bucket) | Sync local metadata directory to the raw bucket. | After receiving author-contributed metadata from a raw bucket, QC/processing steps must be done locally. This script is run after QC is complete, so that the locally changed metadata directories are sync'd to the raw bucket. If any later changes are made to the metadata, this script will need to be re-run to ensure that the raw bucket contains the most up to date copies of the QC'd metadata. | `./transfer_qc_metadata_to_raw_bucket -t jakobsson -ds pmdbs-sn-rnaseq -rv v4.0.0`|
| [`promote_raw_data`](./promote_raw_data) | Transfer QC'ed metadata, CRN Team contributed artifacts, and other CRN Team contributed data (e.g., spatial) from raw data buckets to staging (for Urgent/Minor releases) *or* production buckets (for Minor/Major releases). | Ability to transfer QC'ed metadata and CRN Team contributed data from raw buckets to staging/production buckets. This script is run for all releases: Urgent, Minor, and Major. It also removes the `internal-qc-data` label from the released raw buckets for Urgent/Minor releases. The rationale behind moving this type of data to production buckets (i.e., CURATED) for Urgent/Minor releases is because there are no pipeline/curated outputs, so the staging buckets are not used. The rationale behind moving this type of data to staging buckets (i.e., DEV/UAT) for Minor/Major releases is because there are pipeline/curated outputs, so the [`promote_staging_data`](./promote_staging_data) is used and will eventually copy the data over to production buckets. Minor releases are applicable to both here because sometimes datasets are only platformed in a Minor release, but there are other times where datasets are run through *existing* pipelines. **Note: this script must be run before [`promote_staging_data`](./promote_staging_data).** | `./promote_raw_data --type-of-release urgent --all-datasets --release-version v4.0.0` |
| [`promote_staging_data`](./promote_staging_data) | Promote staging data to production data buckets and apply the appropriate permissions. | Ability to run data integrity tests when trying to promote data from staging (i.e., DEV/UAT) to production buckets (i.e., CURATED). This script is only run for Minor and Major releases. It also applies the appropriate permissions to the buckets (e.g., adding Verily's ASAP Cloud Readers to released raw buckets) and removes the `internal-qc-data` label from the released raw buckets. The buckets/datasets are detected based on the workflow name provided and the workflow/pipeline version that's used to store current curated outputs in raw workflow_execution bucket. This dict, `unembargoed_dev_buckets_and_workflow_version_outputs`, is in `common.py` | `./promote_staging_data -w pmdbs_sc_rnaseq` |
| [`markdown_generator.py`](./markdown_generator.py) | Functions that generate a Markdown report. | This script is used in the [`promote_staging_data`](./promote_staging_data) script to generate a Markdown report that contains data integrity results when trying to promote data from staging (i.e., DEV/UAT) to production buckets (i.e., CURATED). | NA |
| [`crn_cloud_collection_summary`](./crn_cloud_collection_summary) | Track the ASAP raw/curated buckets, size, sample breakdown, and subject breakdown in the CRN Cloud. | This script retrieves the raw and curated buckets, dataset sizes, sample and subject breakdown, and associated data types and origins using the dnastack CLI for querying in Explorer/CRN Cloud. It produces an output file in pwd named `crn_cloud_collection_summary.${date}.tsv` with columns: `gcp_raw_bucket`, `gcp_raw_bucket_size`, `gcp_curated_bucket`, `gcp_curated_bucket_size`, `sample_count`, `subject_count`, `team_name`, `brain_sample_count`, `brain_region_count`. | `./crn_cloud_collection_summary` |
| [`internal_qc_dataset_collection_summary`, `brain_donor_count`](./internal_qc_dataset_collection_summary) | Track datasets in internal QC by getting their ASAP raw buckets, size, sample, and subject breakdown in GCP. | This script retrieves the raw buckets, dataset sizes, sample and subject breakdown, and associated data types, origins, and teams. It produces an output file in pwd named `internal_qc_dataset_collection_summary.${date}.tsv` with columns: `gcp_raw_bucket`, `gcp_raw_bucket_size`, `sample_count`, `subject_count`. | `./internal_qc_dataset_collection_summary` |
| [`transfer_release_resources_to_raw_bucket.py`](./transfer_release_resources_to_raw_bucket.py) | Sync local release-resources config/, release_stats/ and publisher_cards/ to dataset ASAP raw buckets. | After producing Publisher card text and summary figures, this script syncs locally stored files (presumably living at asap-crn-cloud-dataset-metadata/) into each dataset gs:// raw bucket. If any later changes are made to the release-resources, this script will need to be re-run to ensure that the raw bucket contains the most up to date copies. | `./transfer_release_resources_to_raw_bucket.py -i /path/to/release_<release_version>.json -p`|

## Deprecated util scripts

| Script | Description | Context |
| :- | :- | :- |
| [`transfer_raw_data`](./archive/transfer_raw_data) | Transfer data in generic raw buckets to dataset-specific raw buckets (e.g., `gs://asap-raw-data-team-lee` vs. `gs://asap-dev-team-lee-pmdbs-sn-rnaseq`. | Originally, "generic" raw buckets were created because we only had one data type (i.e., sc RNAseq). Later on, we started implementing new data types (e.g., bulk RNAseq, spatial transcriptomics, etc.) and restructured the bucket naming and organization. Therefore, this script is used to move raw data from the generic raw buckets to data-specific raw buckets. It is not applicable to new datasets where we collaborate with the CRN Teams to determine the dataset name. |

# Contributor Data Workflow

This section describes the workflow for processing contributor submissions, from initial upload through QC and back to the raw bucket.

## Workflow Steps

### 1. Validate Bucket Structure

**Script:** [`validate_raw_bucket_structure.py`](./validate_raw_bucket_structure.py)

Validates that the raw bucket has the required directory structure and metadata files after contributor upload.

```bash
python3 validate_raw_bucket_structure.py -t jakobsson -ds pmdbs-sn-rnaseq
```

### 2. Download Metadata Locally

**Script:** [`download_raw_bucket_metadata_to_local`](./download_raw_bucket_metadata_to_local)

Downloads metadata from the raw bucket to your local workspace for QC. Handles both initial submissions (loose CSV files) and post-QC structures (organized directories).

```bash
./download_raw_bucket_metadata_to_local -t jakobsson -ds pmdbs-sn-rnaseq -p
```

**What it does:**

- **Initial submission:** Downloads `metadata/*.csv` → local `metadata/original/`
- **Re-sync:** Downloads entire `metadata/` tree plus `file_metadata/` and `DOI/` if present
- **Optional:** Also downloads `file_metadata/` and `DOI/` if present in bucket

### 3. Perform QC Locally

Quality control is performed locally in the [asap-crn-cloud-dataset-metadata](https://github.com/ASAP-CRN/asap-crn-cloud-dataset-metadata) repository.

**QC outputs:**

```
metadata/
├── original/     # Contributor submission
├── cde/          # CDE-versioned copies
├── release/      # Release-versioned metadata (e.g., v4.0.0/)
└── latest/       # Copy of the latest release version
```

### 4. Transfer QC'd Metadata Back to Bucket

**Script:** [`transfer_qc_metadata_to_raw_bucket`](./transfer_qc_metadata_to_raw_bucket)

Syncs the local metadata directory (including all QC'd subdirectories) back to the raw bucket.

```bash
./transfer_qc_metadata_to_raw_bucket -t jakobsson -ds pmdbs-sn-rnaseq -rv v4.0.0 -p
```

**What it transfers:**

- Entire `metadata/` directory tree
- `file_metadata/` (if present)
- `DOI/` (if present)

**Note:** Use `-p` flag to execute (defaults to dry-run for safety).

### 5. Build release-resources

Build Publisher collection cards text and figures using:
**Script:** `make_release.py` in the [asap-crn-cloud-dataset-metadata](https://github.com/ASAP-CRN/asap-crn-cloud-dataset-metadata) repository.

```bash
/path/to/make_release -i /path/to/release_<release_version>.json -p
```

**release-resources outputs:**
```
release-resources/
└─ {release_version}/
    ├─ cde/
    ├─ release_stats/
    │  └─ {dataset_name}/
    └─ publisher_cards/
       └─ {dataset_name}/
           ├─ figures/
           └─ text/
```

### 6. Transfer release-resources to Dataset Raw Buckets

**Script:** [`transfer_release_resources_to_raw_bucket.py`](./transfer_release_resources_to_raw_bucket.py)

Syncs the local release-resources directory (including all QC'd subdirectories) back to the raw bucket.

```bash
./transfer_release_resources_to_raw_bucket.py -i /path/to/config/release_<release_version>.json -p
```

**What it transfers:**

- Entire `config/release_<release_version>.json`
- `publisher_cards/` html files
- `release_stats/` final svg files

**Note:** Use `-p` flag to execute (defaults to dry-run for safety).


---

## Important Notes

- **Dry-run by default:** Most scripts require `-p` (promote) flag to actually execute transfers
- **Structure migration:** First transfer after QC from local to the raw bucket establishes the new directory structure (`original/`, `cde/`, `release/`, `latest/`) in the bucket
- **Re-running scripts:** Safe to re-run download/transfer scripts - the rysnc command will replace changed files and add new source files to destination, but will not remove files that exist in destination but not source
- **Missing files:** Scripts warn about missing CORE metadata tables but allow incomplete submissions (for flexibility during initial upload)

# Expected structure and files of a contribution

Contributors are expected to deposit their data and metadata in a structured manner in the given dataset's raw bucket. The bucket structure is organized into **required**, **recommended**, and **optional** directories. Note that a contribution consists of the metadata and deposited data, however the form of this data (processed outputs or raw data) will vary by assay. Thus, a submission should at minumum have `metadata/` and a data directory such as `raw/` or `fastqs/`, and preferably include the author's own processed data in `artifacts/`.

## Directory Structure

### Required Directories

- **`metadata/`** - Contains 'core' and 'supplemental' metadata tables (see [Metadata Files](#metadata-files) below)

### Recommended Directories

- **`artifacts/`** - Processed outputs of data pipelines

### Optional Directories

- **`fastqs/`** - FASTQ files for relevant sequencing assays
- **`spatial/`** - Outputs of spatial transcriptomic assays
- **`scripts/`** - Analysis and processing code used by the contributors
- **`raw/`** - Catch-all for raw/unprocessed data for non-sequencing-based assays
- **`workflow_execution/`** - Created by DNAstack during pipeline execution

## Metadata Files

Metadata tables are grouped into two categories:

### Core Metadata Tables

The CDE metadata schema can be found here: [CDE Google Sheet](https://docs.google.com/spreadsheets/d/1c0z5KvRELdT2AtQAH2Dus8kwAyyLrR0CROhKOjpU4Vc/edit?gid=43504703#gid=43504703)

Expected for every submission (CDE 4.0+):

- `ASSAY.csv`
- `CONDITION.csv`
- `DATA.csv`
- `PROTOCOL.csv`
- `SAMPLE.csv`
- `STUDY.csv`
- `SUBJECT.csv`

### Additional Metadata Tables

Context-specific information or tables from releases prior to CDE 4.0 (which consolidated some tables, e.g., `MOUSE` + `CELL` → `SUBJECT`):

- `PMDBS.csv`
- `CLINPATH.csv`
- `MOUSE.csv`
- `CELL.csv`
- `PROTEOMICS.csv`
- `ASSAY_RNAseq.csv`
- `SPATIAL.csv`
- `SDRF.csv`

## Post-Submission Structure

After receiving a contribution, the `metadata/` directory is reorganized and versioned during QC. See [asap-crn-cloud-dataset-metadata](https://github.com/ASAP-CRN/asap-crn-cloud-dataset-metadata) for details on the QC process and final structure:

```
metadata/
├── original/     # Original contributor submission
├── cde/          # CDE-versioned copies
├── release/      # Release-versioned metadata
└── latest/       # Copy of the latest release version
```

# Scripts used to copy data for different Data Release scenarios

| Data Release Scenario | Script Used |
| :- | :- |
| Urgent | <ul><li>[`transfer_qc_metadata_to_raw_bucket`](./transfer_qc_metadata_to_raw_bucket)</li><li>[`promote_raw_data`](./promote_raw_data)</li></ul> |
| Minor | <ul><li>[`transfer_qc_metadata_to_raw_bucket`](./transfer_qc_metadata_to_raw_bucket)</li><li>[`promote_raw_data`](./promote_raw_data)</li><li>[`promote_staging_data`](./promote_staging_data)</li></ul> |
| Major | <ul><li>[`transfer_qc_metadata_to_raw_bucket`](./transfer_qc_metadata_to_raw_bucket)</li><li>[`promote_raw_data`](./promote_raw_data)</li><li>[`promote_staging_data`](./promote_staging_data)</li></ul> |

**Scripts used in different Data Release Scenarios diagram:**
![Scripts used in different Data Release Scenarios diagram](./data_promotion_diagram.svg "Data promotion diagram")

Note: Previous Minor Releases did not contain pipeline/curated outputs (SOW 2); however, moving forward there will be outputs (SOW 3 - onwards) [06/12/2025]. Minor Releases apply to both diagrams, as some datasets may include either pipeline/curated outputs depending on the data modality. If a dataset was previously released in an Urgent or Minor Release and is later scheduled for a Major Release, the curated buckets will be overwritten with the most recent version of the data.

# Helpful resources

- [ASAP Bucket Permissions](https://docs.google.com/document/d/13HwO-Ws3BWbQsnOf6rtXBPsQ5WnXQY2lwfjJ7fm74uU/edit?tab=t.0#heading=h.5tsvungosc7b)
- [ASAP Dataset Organization, Naming and Bucket Structure](https://docs.google.com/document/d/1gWXYMDX_XMO3SV5wJk9PYHwNgIytuvAQ8N2sykN93fw/edit?tab=t.0#heading=h.5tsvungosc7b)
