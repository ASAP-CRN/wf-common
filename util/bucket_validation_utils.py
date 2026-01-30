#!/usr/bin/env python3

# Functions to help validate expected structure of GCP Buckets containing ASAP
# CRN Cloud datasets

import logging
import subprocess
from pathlib import Path
from common import list_dirs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---- Bucket structure constants

REQUIRED_BUCKET_DIRS = ["metadata/"]
RECOMMENDED_BUCKET_DIRS = ["artifacts/"]
OPTIONAL_BUCKET_DIRS = ["fastqs/", 
                        "scripts/", 
                        "raw/", 
                        "spatial/",
                        "workflow_execution/" # created by DNAstack, included for reporting
                        ]

# NOTE: CORE files are expected in all datasets from CDE 4.X onwards.
# ----- SUPPLEMENTARY files may or may not be present depending on the context,
# ----- or represent tables from earlier CDE versions.
CORE_METADATA_FILES = ["ASSAY.csv",
                       "CONDITION.csv",
                       "DATA.csv",
                       "PROTOCOL.csv",
                       "SAMPLE.csv",
                       "STUDY.csv", 
                       "SUBJECT.csv"
                       ]

SUPP_METADATA_FILES = ["PMDBS.csv",
                       "CLINPATH.csv",
                       "MOUSE.csv",
                       "CELL.csv",
                       "PROTEOMICS.csv",
                       "ASSAY_RNAseq.csv",
                       "SPATIAL.csv",
                       "SDRF.csv"
                       ]


# ---- Bucket validation functions


def check_bucket_exists(bucket_url: str) -> None:
    """
    Verify that the bucket exists and is accessible.
    
    Args:
    bucket_url: of the form gs://asap-raw-team-jakobsson-pmdbs-rnaseq
    
    Raises ValueError if the bucket does not exist or cannot be accessed
    """
    command = ["gcloud", "storage", "buckets", "describe", bucket_url]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise ValueError(f"Bucket not accessible: {bucket_url}, see: {e}")
    

def parse_gcloud_list_output(raw_output: str,
                             prefix_to_strip: str,
                             filter_type: str = "dirs") -> list[str]:
    """
    Strip the bucket name and pathing prefix and extract just directories or files.
    
    Args:
    raw_output: Raw output string from gcloud storage list command
    prefix_to_strip: The bucket path prefix to remove from each line
    filter_type: "dirs" to return only directories (lines ending with /) or "files"
    """

    if filter_type == "dirs":
        condition = lambda line: line.strip().endswith("/")
    elif filter_type == "files":
        condition = lambda line: line.strip() and not line.strip().endswith("/")
    else:
        raise ValueError(f"Invalid filter_type: {filter_type}. Must be 'dirs' or 'files'.")
    
    items = [
        line.strip().replace(prefix_to_strip, "")
        for line in raw_output.strip().split("\n")
        if condition(line)
    ]
    return items


def list_and_format_bucket_dirs(path: str) -> list[str]:
    """List directories within the given bucket and remove pathing from names"""
    output = list_dirs(path)
    prefix = path.rstrip('/') + '/' # Ensure only one trailing slash
    dirs = parse_gcloud_list_output(
        raw_output=output,
        prefix_to_strip=prefix,
        filter_type="dirs"
    )
    return dirs


def list_and_format_bucket_files(path: str) -> list[str]:
    """List files within the given bucket path and remove path prefix from names"""
    output = list_dirs(path)
    prefix = path.rstrip('/') + '/' # Ensure only one trailing slash
    files = parse_gcloud_list_output(
        raw_output=output,
        prefix_to_strip=prefix,
        filter_type="files"
    )
    return files


def check_original_metadata_files_in_bucket(bucket_name: str) -> bool:
    """
    Check that the minimal metadata files are present in the bucket. Checks both
    metadata/ (first submission structure) and metadata/original/ (post-QC structure).
    
    Returns True if all core files are found, False otherwise.
    """
    metadata_dir = f"{bucket_name}/metadata/"
    original_dir = f"{metadata_dir}original/"
    
    for check_dir in [metadata_dir, original_dir]:
        try:
            files = list_and_format_bucket_files(check_dir)
            csv_files = [f for f in files if f.endswith(".csv")]

            if csv_files:
                logging.info(f"Checking metadata files in bucket directory: {check_dir}")
                
                missing_core_metadata = [f for f in CORE_METADATA_FILES if f not in csv_files]
                supp_metadata = [f for f in SUPP_METADATA_FILES if f in csv_files]
                extra_files = [f for f in csv_files if f not in CORE_METADATA_FILES + SUPP_METADATA_FILES]
                
                if missing_core_metadata:
                    logging.warning(f"Missing required metadata files: {', '.join(missing_core_metadata)}")
                else:
                    logging.info(f"All required metadata files found")
                    
                if supp_metadata:
                    logging.info(f"Supplementary metadata files found: {', '.join(supp_metadata)}")
                    
                if extra_files:
                    logging.info(f"Unexpected extra metadata files found: {', '.join(extra_files)}")
                
                # Return True if no core files are missing    
                return len(missing_core_metadata) == 0
    
        except subprocess.CalledProcessError:
            continue # Try metadata/original/, implies QC started already
        
    logging.error(f"No metadata files found in {metadata_dir} or {original_dir}")
    return False


    
def get_bucket_structure(bucket_name: str) -> tuple[dict, dict, dict]:
    """"
    Check which required, recommended, and optional directories are present in a bucket.
    
    Returns:
    Tuple of three dicts tracking the presence of required, recommended, and optional dirs.
    """
    bucket_dirs = list_and_format_bucket_dirs(bucket_name)
    
    required_results = {dir_name: dir_name in bucket_dirs for dir_name in REQUIRED_BUCKET_DIRS}
    recommended_results = {dir_name: dir_name in bucket_dirs for dir_name in RECOMMENDED_BUCKET_DIRS}
    optional_results = {dir_name: dir_name in bucket_dirs for dir_name in OPTIONAL_BUCKET_DIRS}

    return required_results, recommended_results, optional_results


def get_missing_directories(results: dict) -> list[str]:
    """Helper to get list of missing directories from validaton results dict"""
    return [dir_name for dir_name, exists in results.items() if not exists]


def validate_raw_bucket_structure(bucket_name: str) -> None:
    """
    Validate raw bucket directory structure.
    
    Args:
    bucket_name: of the form gs://asap-raw-team-jakobsson-pmdbs-rnaseq
    
    Raise ValueError if the bucket does not exist or required directories are missing
    """
    check_bucket_exists(bucket_name)
    
    required, recommended, optional = get_bucket_structure(bucket_name)

    missing_required = get_missing_directories(required)
    missing_recommended = get_missing_directories(recommended)
    present_optional = [dir for dir, present in optional.items() if present]
    
    # Logging results
    if missing_required:
        raise ValueError(
            f"MISSING required directories in {bucket_name}: {', '.join(missing_required)}"
        )
    else:
        logging.info(f"All required directories present in {bucket_name}")
    
    if missing_recommended:
        logging.warning(f"MISSING recommended directories: {', '.join(missing_recommended)}")
    
    if present_optional:
        logging.info(f"Optional directories found: {', '.join(present_optional)}")


def detect_raw_bucket_structure(bucket_name: str) -> str:
    """
    Detect whether the raw bucket uses first submission or post-QC structure.
    
    Args:
    bucket_name: of the form gs://asap-raw-team-jakobsson-pmdbs-rnaseq
    
    Returns:
    "initial" - loose CSV files at metadata/ level, implies intial submission
    "complete" - full directory structure (metadata/original, cde/, release/, latest/)
    """
    metadata_dir = f"{bucket_name}/metadata/"
    
    try:
        dirs = list_and_format_bucket_dirs(metadata_dir)
        
        # Check for post-QC subdirs
        has_original = "original/" in dirs
        has_release = "release/" in dirs
        
        if has_original and has_release:
            logging.info(f"Detected post-QC bucket structure for: {bucket_name}")
            return "complete"
        else:
            logging.info(f"Detected first submission bucket structure for: {bucket_name}")
            return "initial"
        
    except subprocess.CalledProcessError:
        raise ValueError(f"Could not list metadata directory: {metadata_dir}")


# ---- Local dataset validation functions


def check_local_metadata_repo_exists(metadata_root: Path) -> None:
    """Ensure that the local asap-crn-cloud-dataset-metadata repo exists"""
    if not metadata_root.exists():
        raise ValueError(
            f"Local asap-crn-cloud-dataset-metadata repo not found at: {metadata_root}. "
            f"This repo is expected to be at the same level as wf-common."
        )


def check_dataset_dir_exists(dataset_dir: Path) -> None:
    """Ensure that the local dataset directory exists"""
    if not dataset_dir.exists():
        raise ValueError(f"Local dataset directory not found at: {dataset_dir}")
    
    
def check_original_metadata_exists_locally(metadata_dir: Path) -> bool:
    """
    Check that metadata/original/ exists locally and contains CSV files.
    """
    original_dir = Path(metadata_dir) / "original"
    
    if not original_dir.exists():
        return False
    
    # Check for at least one CSV file
    csv_files = list(original_dir.glob("*.csv"))
    return len(csv_files) > 0


def validate_local_metadata_structure(
    metadata_dir: Path, 
    release_version: str,
    is_cohort: bool = False
) -> dict:
    """
    Validate the local metadata/ directory structure for a dataset.
    
    Args:
    dataset_dir: Path to the local dataset directory
    target_release: Target release version string, e.g. "v4.0.1"
    is_cohort: Whether the dataset is a cohort (default: False)
    
    Returns:
    Dict of booleans indicating the presence of key metadata subdirs:
    original/, cde/, release/, and release/{release_version}/
    
    Raises ValueError if any required directories are missing.
    """
    metadata_dir = Path(metadata_dir)
    original_dir = metadata_dir / "original"
    cde_dir = metadata_dir / "cde"
    release_dir = metadata_dir / "release"
    release_version_dir = release_dir / release_version
    
    if not metadata_dir.exists():
        raise ValueError(f"metadata/ directory not found: {metadata_dir}")
    
    results = {
        'original': original_dir.exists(),
        'cde': None,
        'release': release_dir.exists(),
        'release_version': release_version_dir.exists()
    }
    
    if not results['original']:
        raise ValueError(f"metadata/original/ directory not found: {original_dir}")
    
    # CDE/ is not created for cohorts, only release/ metadata copies are made
    if is_cohort:
        logging.info("Skipping CDE directory check for cohort dataset")
    else:
        results['cde'] = cde_dir.exists()
        if not results['cde']:
            raise ValueError(f"metadata/cde/ directory not found: {cde_dir}")
        else: 
            # Absence of versioned dirs within cde/ implies incomplete QC
            cde_versions = [dir for dir in cde_dir.iterdir() if dir.is_dir()]
            if not cde_versions:
                raise ValueError(f"No versioned directories found in metadata/cde/: {cde_dir}")
    
    # release/ may not exist if QC has started but not completed
    if not results['release']:
        raise ValueError(f"metadata/release/ directory not found: {release_dir}")
    else:
        # target release dir must exist within release/
        if not results['release_version']:
            raise ValueError(
                f"release/ directory exists but target release dir not found: {release_version_dir}")
            
    logging.info(f"Local metadata structure validated for dataset at: {metadata_dir}")
    return results
