# !/usr/bin/env python3

# Ensure that the raw bucket file structure is valid after contributors have
# uploaded their data.
# Usage: python3 validate_raw_bucket_structure.py -t jakobsson -ds pmdbs-bulk-rnaseq
# Can also be imported for use in other scripts.

import argparse
import logging
import subprocess
from common import check_bucket_exists, strip_team_name, list_dirs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# TODO: minimal metadata is currently a point of conversation, may be updated
required_dirs = ["metadata/"]
recommended_dirs = ["artifacts/"]
optional_dirs = ["fastqs/", "scripts/"]
minimal_metadata = ["STUDY.csv", "SAMPLE.csv", "DATA.csv", "PROTOCOL.csv"]


def list_and_format_bucket_dirs(bucket_name: str) -> list[str]:
    """List within the given bucket and remove pathing from names"""
    output = list_dirs(bucket_name)
    dirs = [
        line.strip().replace(f"{bucket_name}/", "")
        for line in output.strip().split("\n")
        if line.strip().endswith("/")
    ]
    return dirs


def get_bucket_structure(bucket_name: str) -> tuple[dict, dict, dict]:
    """"
    Return three dicts that track whether the required, recommended,and optional
    directories are present in the bucket.
    """
    bucket_dirs = list_and_format_bucket_dirs(bucket_name)
    
    required_results = {}
    recommended_results = {}
    optional_results = {}
    
    # Required directory check
    for dir_name in required_dirs:
        required_results[dir_name] = dir_name in bucket_dirs
            
    # Recommended directory check    
    for dir_name in recommended_dirs:
        recommended_results[dir_name] = dir_name in bucket_dirs
            
    # Optional directory check
    for dir_name in optional_dirs:
        optional_results[dir_name] = dir_name in bucket_dirs
            
    return required_results, recommended_results, optional_results
        
    
def check_metadata_files_present(bucket_name: str) -> None:
    """Check that the minimal metadata files are present in the metadata/"""
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
    for file_name in minimal_metadata:
        if file_name not in files_in_metadata:
            logging.error(f"Missing required metadata file: {file_name}")
            missing_files.append(file_name)
        else:
            logging.info(f"Found required metadata file: {file_name}")
    
    # Log any extra files found
    additional_files = set(files_in_metadata) - set(minimal_metadata)
    if additional_files:
        for file_name in additional_files:
            logging.info(f"Found additional metadata file: {file_name}")
    
    if missing_files:
        raise ValueError(f"Missing required metadata files: {missing_files}")


def get_missing_directories(results: dict) -> list[str]:
    """Get list of missing directories from results dict"""
    return [dir_name for dir_name, exists in results.items() if not exists]


def validate_raw_bucket_structure(bucket_name: str) -> None:
    """Validate raw bucket directory structure and required metadata files.
    
    Raise a ValueError if the bucket does not exist, required directories are
    missing, or required metadata files are missing. 
    """
    check_bucket_exists(bucket_name)
    required_results, recommended_results, optional_results = get_bucket_structure(bucket_name)
    
    # Check required directories
    for dir_name, exists in required_results.items():
        if not exists:
            logging.error(f"Missing required directory: {dir_name}")
        else:
            logging.info(f"Found required directory: {dir_name}")
    
    missing_required = get_missing_directories(required_results)
    if missing_required:
        raise ValueError(f"Missing required directories: {missing_required}")
    
    # Log recommended directories
    for dir_name, exists in recommended_results.items():
        if not exists:
            logging.warning(f"Missing recommended directory: {dir_name}")
        else:
            logging.info(f"Found recommended directory: {dir_name}")
    
    # Log optional directories if present
    for dir_name, exists in optional_results.items():
        if exists:
            logging.info(f"Found optional directory: {dir_name}")
            
    # Check that minimal metadata files are present
    check_metadata_files_present(bucket_name)
    

def main(args):
    
    team_name = strip_team_name(args.team_name)
    dataset_name = args.dataset_name
    bucket_name = f"gs://asap-raw-team-{team_name}-{dataset_name}"
    
    logging.info(f"Validating raw bucket structure for: {bucket_name}")
    
    try:
        validate_raw_bucket_structure(bucket_name)
        logging.info("Raw bucket structure is valid.")
    except ValueError as e:
        logging.error(f"Raw bucket structure validation failed: {e}")
        exit(1)
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Validate raw bucket directory structure"
    )
    
    parser.add_argument(
        "-t",
        "--team_name",
        required=True,
        help="The team name of the dataset"
    )
    parser.add_argument(
        "-ds",
        "--dataset_name",
        required=True,
        help="The name of the dataset"
    )
    
    args = parser.parse_args()
    main(args)