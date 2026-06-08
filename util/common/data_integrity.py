#!/usr/bin/env python3
"""Data-integrity checks for staging -> production promotion.

Operates on google.cloud.storage Bucket objects: gathers release file lists,
reads MANIFEST.tsv files, runs MD5 / non-empty / associated-metadata checks,
and compares staging vs. curated blob names and hashes.
"""

import re
import logging
import pandas as pd
from io import StringIO


def list_gs_files(bucket, release_version, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name) # This skips the curated metadata and artifacts directories
	blob_names = []
	gs_files = []
	sample_list_loc = []
	pattern = re.compile(rf"{workflow_name}/release/{release_version}/") # This checks for the most recent release version
	for blob in blobs:
		if pattern.match(blob.name):
			blob_names.append(blob.name)
			gs_files.append(f"gs://{bucket.name}/{blob.name}")
			if blob.name.endswith("sample_list.tsv"):
				sample_list_loc.append(f"gs://{bucket.name}/{blob.name}")
	return blob_names, gs_files, sample_list_loc


def read_manifest_files(bucket, release_version, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name) # This has to be called again because 'Iterator has already started'
	manifest_dfs = []
	pattern = re.compile(rf"{workflow_name}/release/{release_version}/")
	for blob in blobs:
		if blob.name.endswith("MANIFEST.tsv") and pattern.match(blob.name):
			gs_path = f"gs://{bucket.name}/{blob.name}"
			logging.info(f"Reading manifest: {gs_path}")
			content = blob.download_as_text()
			try:
				manifest_df = pd.read_csv(StringIO(content), sep="\t")
			except pd.errors.ParserError as e:
				raise pd.errors.ParserError(
					f"Failed to parse {gs_path}: {e}"
				) from e
			manifest_dfs.append(manifest_df)
	combined_df = pd.concat(manifest_dfs, ignore_index=True)
	return combined_df


def md5_check(bucket, release_version, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name)
	hashes = {}
	pattern = re.compile(rf"{workflow_name}/release/{release_version}/")
	for blob in blobs:
		if pattern.match(blob.name):
			hashes[blob] = blob.md5_hash
	return hashes


def non_empty_check(bucket, release_version, workflow_name, GREEN_CHECKMARK, RED_X):
	blobs = bucket.list_blobs(prefix=workflow_name)
	not_empty_tests = {}
	pattern = re.compile(rf"{workflow_name}/release/{release_version}/")
	for blob in blobs:
		if pattern.match(blob.name):
			if blob.size <= 10:
				logging.error(f"Found a file less than or equal to 10 bytes: [{blob.name}]")
				not_empty_tests[blob.name] = f"{RED_X}"
			else:
				not_empty_tests[blob.name] = f"{GREEN_CHECKMARK}"
	return not_empty_tests


def associated_metadata_check(combined_manifest_df, blob_list, GREEN_CHECKMARK, RED_X):
	metadata_present_tests = {}
	for file in blob_list:
		if file.endswith("MANIFEST.tsv"):
			metadata_present_tests[file] = "N/A"
		else:
			if any(file.split('/')[-1] in filename for filename in combined_manifest_df["filename"].tolist()):
				metadata_present_tests[file] = f"{GREEN_CHECKMARK}"
			else:
				logging.error(f"File does not have associated metadata and is absent from MANIFEST: [{file}]")
				metadata_present_tests[file] = f"{RED_X}"
	return metadata_present_tests


def compare_blob_names(results, staging):
	staging_blob_names = results[staging]["blob_names"]
	curated_blob_names = results["curated"]["blob_names"]
	staging_md5_hashes = results[staging]["md5_hashes"]
	staging_bucket_name = next(iter(staging_md5_hashes)).bucket.name
	same_files = ["N/A"]
	new_files = ["N/A"]
	deleted_files = ["N/A"]
	if sorted(staging_blob_names) == sorted(curated_blob_names):
		logging.info(f"The blob_names in '{staging}' are equal to those in 'curated.")
	else:
		logging.info(f"The blob_names in '{staging}' are not equal to those in 'curated'")
		same_files = [file for file in staging_blob_names if file in curated_blob_names]
		new_files = [f"gs://{staging_bucket_name}/{file}" for file in staging_blob_names if file not in curated_blob_names]
		deleted_files = [f"gs://{staging_bucket_name}/{file}" for file in curated_blob_names if file not in staging_blob_names]
		if new_files:
			logging.info(f"New files in '{staging}': {new_files}")
		if deleted_files:
			logging.info(f"Deleted files in '{staging}': {deleted_files}")
	return same_files, new_files, deleted_files


def compare_md5_hashes(results, staging, same_files):
	staging_md5_hashes = results[staging]["md5_hashes"]
	curated_md5_hashes = results["curated"]["md5_hashes"]
	staging_bucket_name = next(iter(staging_md5_hashes)).bucket.name
	staging_file_hashes = {key.name: value for key, value in staging_md5_hashes.items()}
	curated_file_hashes = {key.name: value for key, value in curated_md5_hashes.items()}
	modified_files = {}
	for file in same_files:
		staging_hash = staging_file_hashes.get(file)
		curated_hash = curated_file_hashes.get(file)
		if staging_hash and curated_hash:
			if staging_hash != curated_hash:
				modified_files[f"gs://{staging_bucket_name}/{file}"] = {
					"staging_hash": staging_hash
				}
				logging.info(f"Modified: {file}")
	return modified_files


__all__ = [
    "list_gs_files", "read_manifest_files", "md5_check", "non_empty_check",
    "associated_metadata_check", "compare_blob_names", "compare_md5_hashes",
]
