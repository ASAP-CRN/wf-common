#!/usr/bin/env python3

from argparse import ArgumentParser
from google.cloud import storage
from pathlib import Path
import re
from common import parse_dataset_id


storage_client = storage.Client()

def list_fastqs(dataset, bucket_info, output_dir):
    team, source, assay = parse_dataset_id(dataset)

    bucket_name = bucket_info["bucket"]
    fastq_path = bucket_info["prefix"]

    print(f"Listing blobs for bucket {bucket_name}")
    blobs = storage_client.list_blobs(bucket_name, prefix=fastq_path)

    Path(f"{output_dir}/{team}/{source}-{assay}").mkdir(parents=True, exist_ok=True)
    fastq_loc_file = f"{output_dir}/{team}/{source}-{assay}/{dataset}.fastq_locs.txt"

    with open(fastq_loc_file, "w") as f:
        for blob in blobs:
            if re.search(r".*\.(fastq|fq)(\.gz|)", blob.name):
                f.write(f"gs://{bucket_name}/{blob.name}\n")

    print(f"Wrote {fastq_loc_file}")


def main(args):
    team_bucket_info = {}
    for dataset in args.dataset_id:
        team_bucket_info[dataset] = {
            "bucket": f"asap-raw-{dataset}",
            "prefix": "fastqs/"
        }
    for dataset, bucket_info in team_bucket_info.items():
        list_fastqs(dataset, bucket_info, args.output_dir)


if __name__ == "__main__":
    parser = ArgumentParser(description="Get a flat list of fastqs for ASAP teams")
    
    parser.add_argument(
        "-d",
        "--dataset-id",
        type=str,
        nargs='+',
        required=True,
        help="Space-delimited dataset ID(s) in bucket name (e.g. team-jakobsson-pmdbs-sc-rnaseq).",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        required=False,
        default="team_metadata",
        help="Directory to output fastq locations to",
    )

    args = parser.parse_args()

    main(args)
