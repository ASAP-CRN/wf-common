#!/usr/bin/env python3

import logging
import subprocess
import pandas as pd
import re
from io import StringIO
from google.cloud import storage


###########################################################################
##### PROMOTE CURATED METADTA AND ARTIFACTS - STAGING TO PROD SECTION #####
###########################################################################
unembargoed_team_dev_buckets = [
	# Human PMDBS Single Cell RNAseq
	"gs://asap-dev-team-hafler-pmdbs-sn-rnaseq-pfc",
	"gs://asap-dev-team-hardy-pmdbs-sn-rnaseq",
	"gs://asap-dev-team-scherzer-pmdbs-sn-rnaseq-mtg",
	"gs://asap-dev-team-jakobsson-pmdbs-sn-rnaseq",
	"gs://asap-dev-team-lee-pmdbs-sn-rnaseq",
	#"gs://asap-dev-cohort-pmdbs-sc-rnaseq",
	# Human PMDBS Bulk RNAseq
	"gs://asap-dev-team-hardy-pmdbs-bulk-rnaseq",
	"gs://asap-dev-team-lee-pmdbs-bulk-rnaseq-mfg",
	"gs://asap-dev-team-wood-pmdbs-bulk-rnaseq",
	#"gs://asap-dev-cohort-pmdbs-bulk-rnaseq",
	# Single-nucleus RNAseq hybsel
	"gs://asap-dev-team-scherzer-pmdbs-sn-rnaseq-mtg-hybsel",
]

embargoed_team_dev_buckets = [
	"gs://asap-dev-team-sulzer-pmdbs-sn-rnaseq",
]


def list_dirs(bucket_name):
	command = [
		"gsutil",
		"ls",
		bucket_name
	]
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	return result.stdout


#######################################
##### DATA INTEGRITY TEST SECTION #####
#######################################
ALL_TEAMS = [
	"cohort",
	"team-hafler",
	"team-hardy",
	"team-jakobsson",
	"team-lee",
	"team-scherzer",
	"team-sulzer",
	"team-voet",
	"team-wood"
]

def list_teams():
	logging.info("Available teams:")
	for team in ALL_TEAMS:
		logging.info(team)


def list_gs_files(bucket, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name) # This skips the metadata and artifacts directories
	blob_names = []
	gs_files = []
	sample_list_loc = []
	for blob in blobs:
		blob_names.append(blob.name)
		gs_files.append(f"gs://{bucket.name}/{blob.name}")
		if blob.name.endswith("sample_list.tsv"):
			sample_list_loc.append(f"gs://{bucket.name}/{blob.name}")
	return blob_names, gs_files, sample_list_loc


def read_manifest_files(bucket, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name) # This has to be called again because 'Iterator has already started'
	manifest_dfs = []
	pattern = re.compile(rf"{workflow_name}/metadata/\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}-\d{{2}}-\d{{2}}Z/MANIFEST.tsv$")
	for blob in blobs:
		print(blob.name)
		if blob.name.endswith("MANIFEST.tsv") and not pattern.match(blob.name):
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


def non_empty_check(bucket, workflow_name, GREEN_CHECKMARK, RED_X):
	blobs = bucket.list_blobs(prefix=workflow_name)
	not_empty_tests = {}
	for blob in blobs:
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


###########################################
##### COMPARE STAGING TO PROD SECTION #####
###########################################
def compare_blob_names(results, staging):
	staging_blob_names = results[staging]["blob_names"]
	curated_blob_names = results["curated"]["blob_names"]
	staging_md5_hashes = results[staging]["md5_hashes"]
	staging_bucket_name = next(iter(staging_md5_hashes)).bucket.name
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


##############################################################
##### PROMOTE WORKFLOW OUTPUTS - STAGING TO PROD SECTION #####
##############################################################
def gmove(source_path, destination_path):
	command = [
		"gsutil",
		"-m",
		"mv",
		source_path,
		destination_path
	]
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	logging.info(result.stdout)
	logging.error(result.stderr)


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
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	logging.info(result.stdout)
	logging.error(result.stderr)


def remove_internal_qc_label(bucket_name):
	command = [
		"gcloud",
		"storage",
		"buckets",
		"update",
		bucket_name,
		"--remove-labels=internal-qc-data"
	]
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	logging.info(result.stdout)
	logging.error(result.stderr)


def add_verily_read_access(bucket_name):
	command = [
		"gcloud",
		"storage",
		"buckets",
		"add-iam-policy-binding",
		bucket_name,
		"--member=group:asap-cloud-readers@verily-bvdp.com",
		"--role=roles/storage.objectViewer",
		"--project",
		"dnastack-asap-parkinsons"
	]
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	logging.info(result.stdout)
	logging.error(result.stderr)
