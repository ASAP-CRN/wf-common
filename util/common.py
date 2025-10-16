#!/usr/bin/env python3

import logging
import subprocess
import json
import pandas as pd
import re
from io import StringIO
from google.cloud import storage


######################################################################
##### PROMOTE QC'ED METADATA AND ARTIFACTS - RAW TO PROD SECTION #####
######################################################################
# Urgent and Minor Release or platforming exercise during a Major Release
completed_platforming_raw_buckets = [
	# Single Nucleus RNAseq hybsel
	"gs://asap-raw-team-scherzer-pmdbs-sn-rnaseq-mtg-hybsel",
	# Mouse Single Nucleus/Cell RNAseq
	"gs://asap-raw-team-biederer-mouse-sc-rnaseq",
	"gs://asap-raw-team-cragg-mouse-sn-rnaseq-striatum",
	# Human PMDBS Bulk RNAseq
	"gs://asap-raw-team-jakobsson-pmdbs-bulk-rnaseq",
]

embargoed_platforming_raw_buckets = [
	# Multimodal Seq
	# "gs://asap-raw-team-wood-pmdbs-multimodal-seq" # AM: assumed unembargoed as on completed list
]

unembargoed_platforming_raw_buckets = [
	"gs://asap-raw-team-jakobsson-invitro-bulk-rnaseq-dopaminergic",
	"gs://asap-raw-team-jakobsson-invitro-bulk-rnaseq-microglia",
	"gs://asap-raw-team-wood-pmdbs-multimodal-seq",
	"gs://asap-raw-team-scherzer-pmdbs-sn-rnaseq-mtg",
	"gs://asap-raw-team-scherzer-pmdbs-sn-rnaseq-mtg-hybsel",
	"gs://asap-raw-team-scherzer-pmdbs-spatial-visium-mtg",
	"gs://asap-raw-team-scherzer-pmdbs-genetics"
]


def remove_internal_qc_label(bucket_name, billing_project=None):
	command = [
		"gcloud",
		"storage",
		"buckets",
		"update",
		bucket_name,
		"--remove-labels=internal-qc-data"
	]
	if billing_project:
		command += ["--billing-project", billing_project]
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	return result.stdout


def get_team_name(bucket_name):
	match = re.search(r"team-(.*?)-(mouse|pmdbs|invitro)", bucket_name)
	team = match.group(1)
	return team


def run_command(command, billing_project=None):
	if billing_project:
		command += ["--billing-project", billing_project]
	try:
		result = subprocess.run(command, check=True, capture_output=True, text=True)
		return result.stdout
	except subprocess.CalledProcessError as e:
		if "No policy binding found" in e.stderr:
			print(f"[INFO] No existing storage.admin binding to remove for {team_gg}")
		else:
			print(f"[ERROR] Command failed:\n{e.stderr}")
			raise


def check_admin_binding(type, bucket_name, billing_project=None):
	team_name = get_team_name(bucket_name)
	role_admin = "roles/storage.admin"
	if type == "gg":
		team_gg = "asap-team-" + team_name + "@dnastack.com"
		member = f"group:{team_gg}"
		policy_json = run_command([
			"gcloud",
			"storage",
			"buckets",
			"get-iam-policy",
			bucket_name,
			"--format=json"
		], billing_project=billing_project)
		policy = json.loads(policy_json)
		has_admin_binding = any(
			binding["role"] == role_admin and member in binding.get("members", [])
			for binding in policy.get("bindings", [])
		)
		return member, role_admin, has_admin_binding
	if type == "sa":
		team_sa = "raw-admin-" + team_name + "@dnastack-asap-parkinsons.iam.gserviceaccount.com"
		member = f"serviceAccount:{team_sa}"
		policy_json = run_command([
			"gcloud",
			"storage",
			"buckets",
			"get-iam-policy",
			bucket_name,
			"--format=json"
		], billing_project=billing_project)
		policy = json.loads(policy_json)
		has_admin_binding = any(
			binding["role"] == role_admin and member in binding.get("members", [])
			for binding in policy.get("bindings", [])
		)
		return member, role_admin, has_admin_binding


def change_gg_storage_admin_to_read_write(bucket_name, billing_project=None):
	member, role_admin, has_admin_binding = check_admin_binding("gg", bucket_name, billing_project=billing_project)
	if has_admin_binding:
		print(f"[INFO] Removing Storage Admin access and granting Storage Object Creator and Viewer to CRN Teams for [{bucket_name}] on Google Group")
		run_command([
			"gcloud",
			"storage",
			"buckets",
			"remove-iam-policy-binding",
			bucket_name,
			f"--member={member}",
			f"--role={role_admin}"
		], billing_project=billing_project)
		run_command([
		"gcloud",
		"storage",
		"buckets",
		"add-iam-policy-binding",
		bucket_name,
		f"--member={member}",
		"--role=roles/storage.objectViewer"
		], billing_project=billing_project)
		run_command([
			"gcloud",
			"storage",
			"buckets",
			"add-iam-policy-binding",
			bucket_name,
			f"--member={member}",
			"--role=roles/storage.objectCreator"
		], billing_project=billing_project)
	else:
		print(f"[INFO] Storage Object Creator and Viewer already granted to CRN Teams' permissions for [{bucket_name}] on Google Group")


def change_sa_storage_admin_to_read_write(bucket_name, billing_project=None):
	member, role_admin, has_admin_binding = check_admin_binding("sa", bucket_name, billing_project=billing_project)
	if has_admin_binding:
		print(f"[INFO] Removing Storage Admin access and granting Storage Object Creator and Viewer to CRN Teams for [{bucket_name}] on Service Account")
		run_command([
			"gcloud",
			"storage",
			"buckets",
			"remove-iam-policy-binding",
			bucket_name,
			f"--member={member}",
			f"--role={role_admin}"
		], billing_project=billing_project)
		run_command([
			"gcloud",
			"storage",
			"buckets",
			"add-iam-policy-binding",
			bucket_name,
			f"--member={member}",
			"--role=roles/storage.objectViewer"
		], billing_project=billing_project)
		run_command([
			"gcloud",
			"storage",
			"buckets",
			"add-iam-policy-binding",
			bucket_name,
			f"--member={member}",
			"--role=roles/storage.objectCreator"
		], billing_project=billing_project)
	else:
		print(f"[INFO] Storage Object Creator and Viewer already granted to CRN Teams' permissions for [{bucket_name}] on Service Account")


##########################################################################
##### PROMOTE QC'ED METADATA AND ARTIFACTS - STAGING TO PROD SECTION #####
##########################################################################
# Minor and Major Release that includes pipeline/curated outputs
unembargoed_team_dev_buckets = [
	# Human PMDBS Single Nucleus/Cell RNAseq
	"gs://asap-dev-team-hafler-pmdbs-sn-rnaseq-pfc",
	"gs://asap-dev-team-hardy-pmdbs-sn-rnaseq",
	"gs://asap-dev-team-scherzer-pmdbs-sn-rnaseq-mtg",
	"gs://asap-dev-team-jakobsson-pmdbs-sn-rnaseq",
	"gs://asap-dev-team-lee-pmdbs-sn-rnaseq",
	"gs://asap-dev-cohort-pmdbs-sc-rnaseq",
	# Human PMDBS Bulk RNAseq
	"gs://asap-dev-team-hardy-pmdbs-bulk-rnaseq",
	"gs://asap-dev-team-lee-pmdbs-bulk-rnaseq-mfg",
	"gs://asap-dev-team-wood-pmdbs-bulk-rnaseq",
	"gs://asap-dev-cohort-pmdbs-bulk-rnaseq",
	# Human PMDBS Spatial Transcriptomics Nanostring GeoMx
	"gs://asap-dev-team-edwards-pmdbs-spatial-geomx-th",
	# Mouse Spatial Transcriptomics 10x Visium
	"gs://asap-dev-team-cragg-mouse-spatial-visium-striatum",
]

embargoed_team_dev_buckets = [
	# Human PMDBS Single Nucleus/Cell RNAseq
	"gs://asap-dev-team-sulzer-pmdbs-sn-rnaseq",
	# Human PMDBS Spatial Transcriptomics Nanostring GeoMx
	"gs://asap-dev-team-vila-pmdbs-spatial-geomx-thlc",
	"gs://asap-dev-team-vila-pmdbs-spatial-geomx-unmasked",
]


def list_dirs(bucket_name, billing_project=None):
    command = [
        "gcloud",
        "storage",
        "ls",
    ]
    if billing_project:
        command += ["--billing-project", billing_project]
    command.append(bucket_name)
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
	"team-wood",
	"team-biederer",
	"team-cragg",
	"team-edwards",
	"team-vila",
]

def list_teams():
	logging.info("Available teams:")
	for team in ALL_TEAMS:
		logging.info(team)


def list_gs_files(bucket, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name) # This skips the curated metadata and artifacts directories
	blob_names = []
	gs_files = []
	sample_list_loc = []
	pattern = re.compile(rf"{workflow_name}/archive/")
	for blob in blobs:
		if not pattern.match(blob.name):
			blob_names.append(blob.name)
			gs_files.append(f"gs://{bucket.name}/{blob.name}")
			if blob.name.endswith("sample_list.tsv"):
				sample_list_loc.append(f"gs://{bucket.name}/{blob.name}")
	return blob_names, gs_files, sample_list_loc


def read_manifest_files(bucket, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name) # This has to be called again because 'Iterator has already started'
	manifest_dfs = []
	pattern = re.compile(rf"{workflow_name}/archive/")
	for blob in blobs:
		if blob.name.endswith("MANIFEST.tsv") and not pattern.match(blob.name):
			content = blob.download_as_text()
			manifest_df = pd.read_csv(StringIO(content), sep="\t")
			manifest_dfs.append(manifest_df)
	combined_df = pd.concat(manifest_dfs, ignore_index=True)
	return combined_df


def md5_check(bucket, workflow_name):
	blobs = bucket.list_blobs(prefix=workflow_name)
	hashes = {}
	pattern = re.compile(rf"{workflow_name}/archive/")
	for blob in blobs:
		if not pattern.match(blob.name):
			hashes[blob] = blob.md5_hash
	return hashes


def non_empty_check(bucket, workflow_name, GREEN_CHECKMARK, RED_X):
	blobs = bucket.list_blobs(prefix=workflow_name)
	not_empty_tests = {}
	pattern = re.compile(rf"{workflow_name}/archive/")
	for blob in blobs:
		if not pattern.match(blob.name):
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


##############################################################
##### PROMOTE WORKFLOW OUTPUTS - STAGING TO PROD SECTION #####
##############################################################
def gcopy(source_path, destination_path, recursive=False, billing_project=None):
    command = ["gcloud", "storage", "cp"]
    if recursive:
        command.append("--recursive")
    if billing_project:
        command += ["--billing-project", billing_project]
    command += [source_path, destination_path]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    logging.info(result.stdout)
    logging.error(result.stderr)


def gmove(source_path, destination_path, billing_project=None):
    command = ["gcloud", "storage", "mv"]
    if billing_project:
        command += ["--billing-project", billing_project]
    command += [source_path, destination_path]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    logging.info(result.stdout)
    logging.error(result.stderr)


def gsync(source_path, destination_path, dry_run, billing_project=None):
    command = ["gcloud", "storage", "rsync", "-r"]
    if dry_run:
        command.append("--dry-run")
    if billing_project:
        command += ["--billing-project", billing_project]
    command += [source_path, destination_path]
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
