#!/usr/bin/env python3

import argparse
import sys
import logging
import subprocess
from google.cloud import storage
from common import (
	unembargoed_team_dev_buckets,
	embargoed_team_dev_buckets,
	list_dirs,
)


logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
)


def gsync_artifacts(source_path, destination_path, dry_run):
	command = [
		"gsutil",
		"-m",
		"rsync",
		"-r",
		"-x",
		"cellranger_counts|bam_files",
		source_path,
		destination_path
	]
	if dry_run:
		command.insert(4, "-n")
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	logging.info(result.stdout)
	logging.error(result.stderr)


def main(args):
	if args.list:
		logging.info(f"Unembargoed team buckets:\n" + "\n".join(unembargoed_team_dev_buckets))
		logging.info(f"Embargoed team buckets:\n" + "\n".join(embargoed_team_dev_buckets))
		sys.exit(0)

	all_team_dev_buckets = unembargoed_team_dev_buckets + embargoed_team_dev_buckets
	dry_run = not args.promote

	for dev_bucket in all_team_dev_buckets:
		raw_bucket = dev_bucket.replace("dev", "raw")
		dirs = list_dirs(raw_bucket)
		if "artifacts" in dirs:
			logging.info(f"Promoting artifacts in raw to [{dev_bucket}]")
			gsync_artifacts(f"{raw_bucket}/artifacts", f"{dev_bucket}/artifacts", dry_run)
			if dev_bucket in unembargoed_team_dev_buckets:
				uat_bucket = dev_bucket.replace("dev", "uat")
				logging.info(f"Team dataset is lifted from internal QC- also promoting artifacts in raw to [{uat_bucket}]")
				gsync_artifacts(f"{raw_bucket}/artifacts", f"{uat_bucket}/artifacts", dry_run)
		else:
			logging.info(f"Raw bucket does not have artifacts directory [{raw_bucket}]; skipping")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description="Promote artifacts in raw buckets to staging."
	)

	parser.add_argument(
		"-l",
		"--list",
		action="store_true",
		required=False,
		help="List current team dataset dev buckets."
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
