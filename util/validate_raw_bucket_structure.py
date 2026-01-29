# !/usr/bin/env python3

# Ensure that the raw bucket file structure is valid after contributors have
# uploaded their data.
# Usage: python3 validate_raw_bucket_structure.py -t jakobsson -ds pmdbs-bulk-rnaseq

import argparse
import logging
from common import strip_team_name
from bucket_validation_utils import (
    validate_raw_bucket_structure, 
    check_original_metadata_files_in_bucket
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main(args):
    
    team_name = strip_team_name(args.team_name)
    dataset_name = args.dataset_name
    bucket_name = f"gs://asap-raw-team-{team_name}-{dataset_name}"
    
    logging.info(f"Validating raw bucket structure for: {bucket_name}")
    
    # Ensure the bucket directory structure is valid
    validate_raw_bucket_structure(bucket_name)
    
    # Check metadata files in bucket. Warns if CORE tables missing, errors only if no files found.
    files_valid = check_original_metadata_files_in_bucket(bucket_name)
    
    if files_valid:
        logging.info(f"Raw bucket validation successful: {bucket_name}")
    else:
        logging.warning(f"Raw bucket validation completed with warnings")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Validate raw bucket directory structure after contributor upload"
    )
    
    parser.add_argument(
        "-t",
        "--team_name",
        required=True,
        help="The team name of the dataset (e.g., jakobsson)"
    )
    parser.add_argument(
        "-ds",
        "--dataset_name",
        required=True,
        help="The name of the dataset (e.g., pmdbs-sn-rnaseq)"
    )
    
    args = parser.parse_args()
    main(args)