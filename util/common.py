#!/usr/bin/env python3

import logging
import subprocess
import json
import pandas as pd
import os
import re
import gspread
from io import StringIO
from google.cloud import storage
from google.oauth2.service_account import Credentials


#####################################################
##### PULL FROM LIVE GOOGLE SPREADSHEET AS SSOT #####
#####################################################
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SHEET_ID = "1-Up6AWBw3jc7EO3xVg6jrpK8a_NN1LVaVGXRpPPjesk"
RELEASES_TAB_NAME = "Releases_automated"

def get_releases_df(credentials_path: str = os.path.expanduser("~/.config/gspread/credentials.json")) -> pd.DataFrame:
	creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
	gc = gspread.authorize(creds)
	ws = gc.open_by_key(SHEET_ID).worksheet(RELEASES_TAB_NAME)
	return pd.DataFrame(ws.get_all_records())


######################################################################
##### PROMOTE QC'ED METADATA AND ARTIFACTS - RAW TO PROD SECTION #####
######################################################################
# Urgent and Minor Release or platforming exercise during a Major Release
completed_platforming_raw_buckets = [
	# Human Single Nucleus RNA-seq hybsel
	"gs://asap-raw-team-scherzer-pmdbs-sn-rnaseq-mtg-hybsel",
	# Mouse Single Nucleus/Cell RNA-seq
	"gs://asap-raw-team-schlossmacher-mouse-sn-rnaseq-osn-aav-transd",
	"gs://asap-raw-team-alessi-mouse-sn-rnaseq-dorsal-striatum-g2019s",
	"gs://asap-raw-team-lee-mouse-sn-rnaseq-midbrain-g2019s-hf-diet",
	# Human PMDBS Single Nucleus/Cell RNA-seq (other)
	"gs://asap-raw-team-scherzer-pmdbs-genetics",
	# Invitro Bulk RNA-seq
	"gs://asap-raw-team-jakobsson-invitro-bulk-rnaseq-dopaminergic",
	"gs://asap-raw-team-jakobsson-invitro-bulk-rnaseq-microglia",
	# Invitro Proteomics
	"gs://asap-raw-team-alessi-invitro-ms-p-hek293-gtip",
	# Mouse Bulk RNA-seq (non-brain)
	"gs://asap-raw-team-lee-mouse-liver-bulk-rnaseq-g2019s",
	"gs://asap-raw-team-lee-mouse-bulk-rnaseq-striatum-g2019s-hf-diet",
	# Fecal Metagenome
	"gs://asap-raw-team-schapira-fecal-metagenome-human-baseline",
	# Human Colon Spatial CosMx
	"gs://asap-raw-team-liddle-human-colon-spatial-cosmx-rna-1000p",
	"gs://asap-raw-team-liddle-human-colon-spatial-cosmx-protein-64p"
]


embargoed_platforming_raw_buckets = [
	# Metagenomics
	"gs://asap-raw-team-schapira-fecal-metagenome-human-baseline",
]

unembargoed_platforming_raw_buckets = [
]


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
	return result.stdout


def get_team_name(bucket_name):
	match = re.search(r"team-(.*?)-(mouse|pmdbs|invitro|fecal|human)", bucket_name)
	team = match.group(1)
	return team


def strip_team_prefix(entity_id: str) -> str:
    """Strip 'team' prefix if present: dataset directories do not have 'team' prefix"""
    norm_id = entity_id.strip().lower()
    norm_id = re.sub(r'^team[-_ ]*', '', norm_id)
    if not norm_id:
        raise ValueError(f"ID: [{entity_id}] is empty after stripping 'team' prefix: [{norm_id}]")
    return norm_id


def run_command(command):
	try:
		result = subprocess.run(command, check=True, capture_output=True, text=True)
		return result.stdout
	except subprocess.CalledProcessError as e:
		if "No policy binding found" in e.stderr:
			print(f"[INFO] No existing storage.admin binding to remove for {team_gg}")
		else:
			print(f"[ERROR] Command failed:\n{e.stderr}")
			raise


def check_admin_binding(bucket_name):
	team_name = get_team_name(bucket_name)
	role_admin = "roles/storage.admin"
	team_gg = "asap-team-" + team_name + "@dnastack.com"
	member = f"group:{team_gg}"
	policy_json = run_command([
		"gcloud",
		"storage",
		"buckets",
		"get-iam-policy",
		bucket_name,
		"--format=json"
	])
	policy = json.loads(policy_json)
	has_admin_binding = any(
		binding["role"] == role_admin and member in binding.get("members", [])
		for binding in policy.get("bindings", [])
	)
	return member, role_admin, has_admin_binding


def change_gg_storage_admin_to_read_write(bucket_name):
	member, role_admin, has_admin_binding = check_admin_binding(bucket_name)
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
		])
		run_command([
		"gcloud",
		"storage",
		"buckets",
		"add-iam-policy-binding",
		bucket_name,
		f"--member={member}",
		"--role=roles/storage.objectViewer"
		])
		run_command([
			"gcloud",
			"storage",
			"buckets",
			"add-iam-policy-binding",
			bucket_name,
			f"--member={member}",
			"--role=roles/storage.objectCreator"
		])
	else:
		print(f"[INFO] Storage Object Creator and Viewer already granted to CRN Teams' permissions for [{bucket_name}] on Google Group")


##########################################################################
##### PROMOTE QC'ED METADATA AND ARTIFACTS - STAGING TO PROD SECTION #####
##########################################################################
# Minor and Major Release that includes pipeline/curated outputs
unembargoed_dev_buckets_and_workflow_version_outputs = {
	# Human PMDBS Single Nucleus/Cell RNA-seq
	"gs://asap-dev-team-hafler-pmdbs-sn-rnaseq-pfc": "v3.0.0",
	"gs://asap-dev-team-hardy-pmdbs-sn-rnaseq": "v3.0.0",
	"gs://asap-dev-team-scherzer-pmdbs-sn-rnaseq-mtg": "v3.0.0",
	"gs://asap-dev-team-jakobsson-pmdbs-sn-rnaseq": "v3.0.0",
	"gs://asap-dev-team-lee-pmdbs-sn-rnaseq": "v3.0.0",
	"gs://asap-dev-team-sulzer-pmdbs-sn-rnaseq": "v3.1.0",
	"gs://asap-dev-cohort-pmdbs-sc-rnaseq": "v3.1.0",
	# Mouse Single Nucleus/Cell RNA-seq
	"gs://asap-dev-team-biederer-mouse-sc-rnaseq": "v4.0.0",
	"gs://asap-dev-team-cragg-mouse-sn-rnaseq-striatum": "v4.0.0",
	"gs://asap-dev-cohort-mouse-sc-rnaseq": "v4.0.0",
	# Human PMDBS Single Nucleus/Cell ATAC-seq
	"gs://asap-dev-team-voet-pmdbs-sn-atacseq-10x": "v1.0.0",
	# Human PMDBS Bulk RNA-seq
	"gs://asap-dev-team-hardy-pmdbs-bulk-rnaseq": "v1.1.1",
	"gs://asap-dev-team-lee-pmdbs-bulk-rnaseq-mfg": "v1.1.1",
	"gs://asap-dev-team-wood-pmdbs-bulk-rnaseq": "v1.1.1",
	"gs://asap-dev-team-jakobsson-pmdbs-bulk-rnaseq": "v1.1.1",
	"gs://asap-dev-cohort-pmdbs-bulk-rnaseq": "v1.1.1",
	# Human PMDBS Spatial Transcriptomics Nanostring GeoMx
	"gs://asap-dev-team-edwards-pmdbs-spatial-geomx-th": "v1.0.0",
	# Human Spatial Transcriptomics 10x Visium
	"gs://asap-dev-team-scherzer-pmdbs-spatial-visium-mtg": "v1.0.1",
	# Mouse Spatial Transcriptomics 10x Visium
	"gs://asap-dev-team-cragg-mouse-spatial-visium-striatum": "v1.0.0",
}

embargoed_dev_buckets = [
	# Human PMDBS Multimodal Seq
	"gs://asap-raw-team-wood-pmdbs-multimodal-seq",
	# Human PMDBS Spatial Transcriptomics Nanostring GeoMx
	"gs://asap-dev-team-vila-pmdbs-spatial-geomx-thlc",
	"gs://asap-dev-team-vila-pmdbs-spatial-geomx-unmasked",
]


def list_dirs(bucket_name):
	command = [
		"gcloud",
		"storage",
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
			content = blob.download_as_text()
			manifest_df = pd.read_csv(StringIO(content), sep="\t")
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
def gcopy(source_path, destination_path, recursive=False):
	command = [
		"gcloud",
		"storage",
		"cp",
		source_path,
		destination_path
	]
	if recursive:
		command.insert(3, "--recursive")
	result = subprocess.run(command, check=True, capture_output=True, text=True)

	# These if's are because gcloud returns important info in stderr (e.g. "Copying /path/to/file1 to gs://bucket/file1...") 
	# even if the command is successful. Since ERROR may be misleading, it's better to logging.info both stdout and stderr
	if result.stdout:
		logging.info(result.stdout)
	if result.stderr:
		logging.info(result.stderr)

def gmove(source_path, destination_path):
	command = [
		"gcloud",
		"storage",
		"mv",
		source_path,
		destination_path
	]
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	if result.stdout:
		logging.info(result.stdout)
	if result.stderr:
		logging.info(result.stderr)


def gremove(destination_path):
	command = [
		"gcloud",
		"storage",
		"rm",
		destination_path
	]
	try:
		result = subprocess.run(command, check=True, capture_output=True, text=True)
	except subprocess.CalledProcessError:
		logging.info(f"No files found at {destination_path}; skipping deletion.")
		return
	if result.stdout:
		logging.info(result.stdout)
	if result.stderr:
		logging.info(result.stderr)


def gsync(source_path, destination_path, dry_run):
	command = [
		"gcloud",
		"storage",
		"rsync",
		"-r",
		source_path,
		destination_path
	]
	if dry_run:
		command.insert(4, "--dry-run")
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	if result.stdout:
		logging.info(result.stdout)
	if result.stderr:
		logging.info(result.stderr)


def gsync_del(source_path, destination_path, dry_run):
	command = [
		"gcloud",
		"storage",
		"rsync",
		"--delete-unmatched-destination-objects",
		"-r",
		source_path,
		destination_path
	]
	if dry_run:
		command.insert(4, "--dry-run")
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	if result.stdout:
		logging.info(result.stdout)
	if result.stderr:
		logging.info(result.stderr)


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
	if result.stdout:
		logging.info(result.stdout)
	if result.stderr:
		logging.info(result.stderr)