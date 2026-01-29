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
OPTIONAL_BUCKET_DIRS = ["fastqs/", "scripts/", "raw/"]

# NOTE: It is possible that a contribution may not initially have all of these!
# ----- This also assume CDE 4.X+, earlier CDE may have alternate tables such
# ----- as MOUSE or CELL instead of SUBJECT.
MINIMAL_METADATA_FILES = ["STUDY.csv", 
                          "SUBJECT.csv", 
                          "CONDITION.csv",
                          "SAMPLE.csv", 
                          "DATA.csv", 
                          "PROTOCOL.csv",
                          "ASSAY.csv"]

ADDITIONAL_METADATA_FILES = ["PMDBS.csv",
                             "CLINPATH.csv",
                             "MOUSE.csv",
                             "CELL.csv",
                             "PROTEOMICS.csv",
                             "ASSAY_RNAseq.csv",
                             "SPATIAL.csv",
                             "SDRF.csv"]


# ---- Bucket validation functions
# TODO: format names as own functions

def check_bucket_exists(bucket_url: str) -> None:
    """Terminate early if the target bucket does not exist"""
    command = ["gcloud", "storage", "buckets", "describe", bucket_url]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise ValueError(f"Bucket not found: {bucket_url}, see: {e}")
    


def list_and_format_bucket_dirs(bucket_name: str) -> list[str]:
    """List within the given bucket and remove pathing from names"""
    output = list_dirs(bucket_name)
    dirs = [
        line.strip().replace(f"{bucket_name}/", "")
        for line in output.strip().split("\n")
        if line.strip().endswith("/")
    ]
    return dirs



# TODO: strictness of value error vs logging error
# TODO: try catch around list_dirs... baked into function?
# TODO: revisit extra/additional file logging!
def check_metadata_files_in_bucket(bucket_name: str) -> None:
    """
    Check that the minimal metadata files are present in the bucket's metadata/ dir
    """
    metadata_dir = f"{bucket_name}/metadata/"
    
    # Getting all files in metadata/
    try:
        output = list_dirs(metadata_dir)
    except subprocess.CalledProcessError as e:
        raise ValueError(
            f"metadata/ directory not found in bucket: {bucket_name}. "
            f"Expected path: {metadata_dir}"
        )

    files_in_metadata = [
        line.strip().replace(metadata_dir, "")
        for line in output.strip().split("\n")
        if not line.strip().endswith("/")
    ]
    
    # Check for required files
    missing_files = []
    for file_name in MINIMAL_METADATA_FILES:
        if file_name not in files_in_metadata:
            logging.error(f"Missing required metadata file: {file_name}")
            missing_files.append(file_name)
        else:
            logging.info(f"Found required metadata file: {file_name}")
    
    # Log any extra files found
    additional_files = set(files_in_metadata) - set(MINIMAL_METADATA_FILES)
    if additional_files:
        for file_name in additional_files:
            logging.info(f"Found additional metadata file: {file_name}")
    
    if missing_files:
        raise ValueError(f"Missing required metadata files: {missing_files}")

    

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


# TODO: revisit strictness of raise error and adding this as a flag
# TODO: exclusion of metadata files?
# TODO: extra dirs?
def validate_raw_bucket_structure(bucket_name: str) -> None:
    """
    Validate raw bucket directory structure and required metadata files.
    
    Args:
    bucket_name: of the form gs://asap-raw-team-jakobsson-pmdbs-rnaseq
    
    Raise a ValueError if the bucket does not exist, required directories are
    missing, or required metadata files are missing. 
    """
    check_bucket_exists(bucket_name)
    
    required, recommended, optional = get_bucket_structure(bucket_name)

    missing_required = get_missing_directories(required)
    missing_recommended = get_missing_directories(recommended)
    present_optional = [dir for dir, present in optional.items() if present]
    
    # Logging results
    if missing_required:
        logging.error(
            f"MISSING required directories in {bucket_name}: {', '.join(missing_required)}"
        )
    else:
        logging.info(f"All required directories present in {bucket_name}")
    
    if missing_recommended:
        logging.warning(f"MISSING recommended directories: {', '.join(missing_recommended)}")
    
    if present_optional:
        logging.info(f"Optional directories found: {', '.join(present_optional)}")
            
    # Check that minimal metadata files are present
    # check_metadata_files_present(bucket_name)


