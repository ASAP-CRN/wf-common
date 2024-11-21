#!/usr/bin/env python3

import argparse
import sys
import logging
from google.cloud import storage
from common import (
	release_unembargoed_team_buckets,
	release_embargoed_team_buckets,
	list_buckets,
	gsync,
)


def main(args):
	buckets = list_buckets()
	dev_buckets = [bucket for bucket in dev_buckets if "dev" in bucket]
	gs_dev_buckets = []
	for bucket in dev_buckets:
		gs_dev_buckets.append(f"gs://{bucket}")

	if args.list:
		logging.info(gs_dev_buckets)
		sys.exit(0)

	dry_run = not args.promote

	for dev_bucket in gs_dev_buckets:
		raw_bucket = dev_bucket.replace("dev", "raw")
		if dev_bucket in release_unembargoed_team_buckets + release_embargoed_team_buckets:
			logging.info(f"Team dataset is still in internal QC. Promoting raw to [{dev_bucket}]")
			#gsync(f"{raw_bucket}/metadata/release", f"{dev_bucket}/metadata/release", dry_run)
		elif dev_bucket in release_unembargoed_team_buckets:
			uat_bucket = dev_bucket.replace("dev", "uat")
			logging.info(f"Team dataset is lifted from internal QC. Promoting raw to [{uat_bucket}]")
			#gsync(raw_bucket, uat_bucket, dry_run)


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
