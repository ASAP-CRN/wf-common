#!/usr/bin/env python3

"""
This CLI transfers locally saved release-resources configs and outputs to dataset raw Google bucket
(i.e. to gs://asap-raw-team-<team_name>-<dataset_name>/release-resources/<release_version>/)

- Local requirements:
  {your_asap_repos_root}/
  └─ asap-crn-cloud-dataset-metadata/release-resources/<release_version>/
     ├─ config/
     ├─ publisher_cards/
     └─ release_stats/

Usage:
    python3 transfer_release_resources_to_raw_bucket.py -i <infile_json> -p (omit for dry run)
    <infile_json> should be a JSON file with the following structure:
    "general": {
    "release_version": "v4.0.1",
    "dataset_names": [
        "smith-pmdbs-sc-rnaseq", 
        "smith-pmdbs-bulk-rnaseq"
    ],
    }

Authors: Javier Diaz
"""

import argparse
import logging
from pathlib import Path
from common import gcopy
import os, sys
from collections import defaultdict
import json

def find_repo_root(path: Path = None) -> Path:
    """Find git repository root."""
    if path is None:
        path = Path(__file__).resolve()
    for parent in [path, *path.parents]:
        if (parent / '.git').exists():
            return parent
    raise FileNotFoundError(f"No git repository found for {path}")

repo_root = str(find_repo_root())
dss_meta_root = str(os.path.join(Path(repo_root).parents[0], "asap-crn-cloud-dataset-metadata"))
sys.path.insert(0, dss_meta_root)

from bucket_validation_utils import (
    check_bucket_exists,
    validate_local_release_resources_structure
)

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
)

def main(args):

    dry_run = not args.promote

    ### Load options from config/json file
    config_json = args.infile_json
    try:
        with open(config_json, "r") as f:
            config = json.load(f)
        logging.info(f"Read {config_json} file")
    except FileNotFoundError:
        logging.error(f"{config_json} json file does NOT exist!")
        sys.exit(1)

    # general parameters
    release_version = config['general']['release_version']
    dataset_names = config['general']['dataset_names']

    # Early exit if local release-resources directory is missing
    release_resources_dir = Path(dss_meta_root) / "release-resources" / release_version
    if not release_resources_dir.exists():
        logging.error(f"Local release-resources directory not found: {release_resources_dir}")
        sys.exit(1)

    # Check that buckets exist
    for dataset_name in dataset_names:
        bucket_name = f"gs://asap-raw-team-{dataset_name}"
        check_bucket_exists(bucket_name)

    # Define key files expected per subdir in local release-resources for each dataset, to validate before transfer
    validated_files_per_dataset = {}
    for dataset_name in dataset_names:
        bucket_name = f"gs://asap-raw-team-{dataset_name}"
        files_per_subdir = defaultdict(lambda : defaultdict(dict))
        files_per_subdir["config"] = files_per_subdir.get("config", []) + [f"release_{release_version}.json"]
        files_per_subdir["publisher_cards/text"] = files_per_subdir.get("publisher_cards/text", []) + [f"{dataset_name}_CARD.html"]
        files_per_subdir["publisher_cards/figures/combined"] = files_per_subdir.get("publisher_cards/figures/combined", []) + [f"{dataset_name}-ALL.svg"]
        files_per_subdir["release_stats"] = files_per_subdir.get("release_stats", []) + [f"{dataset_name}/release_stats.json"]

        validated_files_per_dataset[dataset_name] = validate_local_release_resources_structure(
            release_resources_dir=release_resources_dir,
            files_per_subdir=files_per_subdir
            )

    # Transfers validated files per dataset
    # to gs://asap-raw-team-<team_name>-<dataset_name>/release-resources/<release_version>/
    for dataset_name in dataset_names:
        bucket_name = f"gs://asap-raw-team-{dataset_name}"
        release_resources_bucket = f"{bucket_name}/release_resources/{release_version}" # Note: "release_resources" (with underscore)
        validate_files = validated_files_per_dataset[dataset_name]
        for local_file_path in validate_files:
            bucket_file_path = f"{release_resources_bucket}/{local_file_path.relative_to(release_resources_dir)}"
            if dry_run:
                logging.info(f"Would copy {local_file_path} to {bucket_file_path}")
            else:
                logging.info(f"Transferring file: {local_file_path} to {bucket_file_path}")
                gcopy(source_path=str(local_file_path), 
                      destination_path=bucket_file_path,
                      recursive=False)
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
		description=(
            "This CLI transfers locally saved release-resources configs and outputs to dataset raw Google bucket \n"
            "(i.e. to gs://asap-raw-team-<team_name>-<dataset_name>/release-resources/<release_version>/)\n\n"
            "- Local requirements:\n"
            "  {your_asap_repos_root}/\n"
            "  └─ asap-crn-cloud-dataset-metadata/release-resources/<release_version>/\n"
            "     ├─ config/\n"
            "     ├─ publisher_cards/\n"
            "     └─ release_stats/\n\n"
            "Usage:\n"
            "    python3 transfer_release_resources_to_raw_bucket.py -i <infile_json> -p (omit for dry run)\n\n"
            "    <infile_json> should be a JSON file with the following structure:\n"
            '    "general": {\n'
            '     "release_version": "v4.0.1",\n'
            '     "dataset_names": [\n'
            '        "smith-pmdbs-sc-rnaseq", \n'
            '        "smith-pmdbs-bulk-rnaseq"\n'
            '     ],\n'
            '    }\n'
        ),
        formatter_class=argparse.RawTextHelpFormatter,
	)
    
    parser.add_argument(
        "-i", 
        "--infile_json", 
        required=True, 
        help="/path_to/release_{release_version}.json file"
    )
    parser.add_argument(
        "-p",
		"--promote",
		action="store_true",
		required=False,
		help="Promote data (omit for dry run).\n\n"
	)

    args = parser.parse_args()
    main(args)
