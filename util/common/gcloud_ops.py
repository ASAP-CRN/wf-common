#!/usr/bin/env python3
"""Elementary gcloud / Cloud Storage CLI wrappers and bucket IAM/label helpers.

Thin wrappers around `gcloud storage ...` subprocess calls (copy/move/remove/
rsync/list) plus the bucket permission and label operations used during data
promotion. Also includes the small bucket/dataset name-parsing helpers.
"""

import json
import logging
import subprocess
import re


def get_team_name(bucket: str) -> str:
	return bucket.split("-team-", 1)[1].split("-")[0]


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
			print(f"[INFO] No existing storage.admin binding to remove (command: {' '.join(command)})")
		else:
			print(f"[ERROR] Command failed:\n{e.stderr}")
			raise


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


def list_dirs(bucket_name):
	command = [
		"gcloud",
		"storage",
		"ls",
		bucket_name
	]
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	return result.stdout


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


__all__ = [
    "get_team_name", "strip_team_prefix", "run_command",
    "remove_internal_qc_label", "check_admin_binding",
    "change_gg_storage_admin_to_read_write", "list_dirs",
    "gcopy", "gmove", "gremove", "gsync", "gsync_del",
    "add_verily_read_access",
]
