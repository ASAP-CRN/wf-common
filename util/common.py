#!/usr/bin/env python3

import logging
import subprocess
import pandas as pd
from io import StringIO
from google.cloud import storage


def list_teams():
	logging.info("Available teams:")
	for team in ALL_TEAMS:
		logging.info(team)


# TODO - if it's the same bucket, I should call it in main first then run all these functions
## Use the results from this for other functions to avoid listing blobs many times
def list_gs_files(bucket_name, workflow_name):
	bucket = client.get_bucket(bucket_name)
	blobs = bucket.list_blobs(prefix=workflow_name)
	blob_names = []
	gs_files = []
	for blob in blobs:
		blob_names.append(blob.name)
		gs_files.append(f"gs://{bucket_name}/{blob.name}")
	return bucket, blob_names, gs_files


def read_manifest_files(bucket, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name) # This has to be called again because 'Iterator has already started'
	manifest_dfs = []
	for blob in blobs:
		if blob.name.endswith("MANIFEST.tsv"):
			content = blob.download_as_text()
			manifest_df = pd.read_csv(StringIO(content), sep="\t")
			manifest_dfs.append(manifest_df)
	combined_df = pd.concat(manifest_dfs, ignore_index=True)
	return combined_df


def md5_check(bucket, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name)
	hashes = {}
	for blob in blobs:
		hashes[blob] = blob.md5_hash
	return hashes


def non_empty_check(bucket, workflow_name, RED_X, GREEN_CHECKMARK):
	blobs = bucket.list_blobs(prefix=workflow_name)
	not_empty_tests = {}
	for blob in blobs:
		if blob.size <= 10:
			logging.error(f"Found a file less than or equal to 10 bytes: [{blob.name}]")
			not_empty_tests[blob.name] = f"{RED_X}"
		else:
			not_empty_tests[blob.name] = f"{GREEN_CHECKMARK}"
	return not_empty_tests


def associated_metadata_check(combined_manifest_df, file_list, RED_X, GREEN_CHECKMARK):
	metadata_present_tests = {}
	for file in file_list:
		if file.endswith("MANIFEST.tsv"):
			metadata_present_tests[file] = "N/A"
		else:
			metadata_file = f"{file}.meta.tsv"
			if metadata_file in combined_manifest_df.values.flatten():
				metadata_present_tests[file] = f"{GREEN_CHECKMARK}"
			else:
				logging.error(f"File does not have associated metadata and is absent from MANIFEST: [{file}]")
				metadata_present_tests[file] = f"{RED_X}"
	return metadata_present_tests
	

def gsync(source_path, destination_path, dry_run):
	dry_run_arg = "-n" if dry_run else ""
	command = [
		"gsutil",
		"-m",
		"rsync",
		"-d",
		"-r",
		dry_run_arg,
		source_path,
		destination_path
	]
	subprocess.run(command, check=True)
