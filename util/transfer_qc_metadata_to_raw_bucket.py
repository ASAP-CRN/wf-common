#!/usr/bin/env python3
# Transfer locally saved QC'd metadata to the raw Google bucket
# Usage: python3 transfer_qc_metadata_to_raw_bucket.py -t jakobsson -ds pmdbs-bulk-rnaseq
# Defaults to dry run unless -p flag is added!
# NOTE on assumptions made:
# 1.   You have cloned asap-crn-cloud-dataset-metadata and its root is at the
# ---- same level as wf-common
# 2.   The metadata/ and file_metadata/ directories of the target dataset exist
# ---- locally in asap-crn-cloud-dataset-metadata and contain the QC'd metadata

import argparse
import logging
import re
import subprocess
from pathlib import Path
from common import gsync


logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
)

# NOTE: Assumes that you have cloned asap-crn-cloud-dataset-metadata. Going
# ----  forward we may split, but for now this contains both code to do dataset 
# ----  QC as well as storing the resulting output, to be sync'd to a raw bucket
repo_root = Path(__file__).resolve().parents[1]
metadata_root = repo_root.parent / "asap-crn-cloud-dataset-metadata"


def check_team_name(team_name: str) -> str:
    """Strip 'team' prefix if present"""
    norm = re.sub(r'^\s*team[-_ ]*', '', team_name.strip(), flags=re.IGNORECASE)
    norm = norm.strip().lower()
    if not norm:
        raise ValueError("team_name is empty after stripping 'team' prefix")
    return norm


def bucket_exists(bucket_url: str) -> None:
    command = ["gcloud", "storage", "buckets", "describe", bucket_url]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise ValueError(f"Bucket not found: {bucket_url}, see: {e}")


def main(args):

    dry_run = not args.promote
    
    team_name = check_team_name(args.team_name)
    dataset_name = args.dataset_name
    dataset_name_long = f"{team_name}-{dataset_name}"
    
    dataset_dir = metadata_root / "datasets" / dataset_name_long
    metadata_dir = dataset_dir / "metadata"
    file_metadata_dir = dataset_dir / "file_metadata"
    
    bucket_name = f"gs://asap-raw-team-{team_name}-{dataset_name}"
    file_metadata_bucket = f"{bucket_name}/file_metadata"
    metadata_bucket = f"{bucket_name}/metadata"
    
    bucket_exists(bucket_name)

    if file_metadata_dir.exists():
        logging.info(f"Transferring local file_metadata directory to [{file_metadata_bucket}]")
        gsync(source_path=file_metadata_dir, destination_path=file_metadata_bucket, dry_run=dry_run)
    else:
        raise ValueError(f"Local file metadata directory not found: {file_metadata_dir}")
    
    # metadata/ expected to have further subdirs: this syncs recursively
    if metadata_dir.exists():
        logging.info(f"Transferring local metadata directory to [{metadata_bucket}]")
        gsync(source_path=metadata_dir, destination_path=metadata_bucket, dry_run=dry_run)
    else:
        raise ValueError(f"Local metadata directory not found: {metadata_dir}")
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
		description="Sync local metadata directories to raw bucket"
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
    parser.add_argument(
        "-p",
		"--promote",
		action="store_true",
		required=False,
		help="Promote data (default is dry run)."
	)

    args = parser.parse_args()
    main(args)
