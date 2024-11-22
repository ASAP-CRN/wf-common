#!/usr/bin/env python3

import argparse
import sys
import logging
import subprocess
from google.cloud import storage
from common import (
	release_unembargoed_team_buckets,
	release_embargoed_team_buckets,
	list_buckets,
	list_dirs,
)


logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
)


def gsync_artifacts(source_path, destination_path, dry_run):
	dry_run_arg = "-n" if dry_run else ""
	command = [
		"gsutil",
		"-m",
		"rsync",
		"-r",
		"-x",
		"cellranger_counts|bam_files",
		dry_run_arg,
		source_path,
		destination_path
	]
	result = subprocess.run(command, check=True, capture_output=True, text=True)
	logging.info(result.stdout)
	logging.error(result.stderr)


def main(args):
	buckets = list_buckets()
	dev_buckets = [bucket for bucket in buckets if "asap-dev" in bucket]
	gs_dev_buckets = []
	for bucket in dev_buckets:
		gs_dev_buckets.append(f"gs://{bucket}")

	if args.list:
		logging.info(f"\n" + "\n".join(gs_dev_buckets))
		sys.exit(0)

	dry_run = not args.promote

	for dev_bucket in gs_dev_buckets:
		dirs = list_dirs(dev_bucket)
		raw_bucket = dev_bucket.replace("dev", "raw")
		if "artifacts" in dirs:
			if dev_bucket in release_unembargoed_team_buckets + release_embargoed_team_buckets:
				logging.info(f"Promoting artifacts in raw to [{dev_bucket}]")
				gsync_artifacts(f"{raw_bucket}/artifacts", f"{dev_bucket}/artifacts", dry_run)
			elif dev_bucket in release_unembargoed_team_buckets:
				uat_bucket = dev_bucket.replace("dev", "uat")
				logging.info(f"Team dataset is lifted from internal QC. Promoting raw to [{uat_bucket}]")
				gsync_metadata(f"{raw_bucket}/metadata/release", f"{uat_bucket}/metadata/release", dry_run)
				gsync_artifacts(f"{raw_bucket}/artifacts", f"{uat_bucket}/artifacts", dry_run)
			else:
				logging.warning(f"Bucket is not in release_unembargoed_team_buckets or release_embargoed_team_buckets: [{dev_bucket}]")
		else:
			logging.info(f"Raw bucket does not have artifacts directory [{raw_bucket}]")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description="Promote metadata and artifacts in raw buckets to staging."
	)

	parser.add_argument(
		"-l",
		"--list",
		action="store_true",
		required=False,
		help="List all dev buckets."
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
