#!/usr/bin/env python3

import subprocess
from google.cloud import storage

def list_teams():
	print("Available teams:")
	for team in ALL_TEAMS:
		print(team)

def list_gs_files(bucket_name, prefix=""):
	client = storage.Client()
	bucket = client.get_bucket(bucket_name)
	blobs = bucket.list_blobs(prefix=prefix)
	gs_files = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]
	return gs_files

def 

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