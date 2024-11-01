#!/usr/bin/env python3

import subprocess
from google.cloud import storage

def list_teams():
	print("Available teams:")
	for team in ALL_TEAMS:
		print(team)

# TODO - if it's the same bucket, I should call it in main first then run all these functions
def list_gs_files(bucket_name, prefix=""):
	bucket = client.get_bucket(bucket_name)
	blobs = bucket.list_blobs(prefix=prefix)
	gs_files = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]
	return gs_files

# TODO
def get_manifest_info(bucket_type, manifest_loc):

def md5_check(bucket_type, source_path):
	bucket = client.get_bucket(bucket_type)
	source_path = source_path.lstrip("/")
	blobs = bucket.list_blobs(prefix=source_path)
	hashes = []
	for blob in blobs:
		blob_md5 = blob.md5_hash
		hashes.append(blob_md5)
	return hashes

def non_empty_check(bucket_name, source_path, RED_X, GREEN_CHECKMARK):
	source_path = source_path.lstrip("/")
	blobs = list(bucket.list_blobs(prefix=source_path))
	not_empty_tests = ""
	for blob in blobs:
		if blob.size <= 10:
			print(f"Found a file less than or equal to 10 bytes: [{blob.name}]")
			not_empty_tests += f"{RED_X}\n"
		else
			not_empty_tests += f"{GREEN_CHECKMARK}\n"
	return not_empty_tests

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