#!/usr/bin/env python3

import subprocess

def list_teams():
	print("Available teams:")
	for team in ALL_TEAMS:
		print(team)

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