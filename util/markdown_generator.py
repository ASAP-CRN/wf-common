#!/usr/bin/env python3

import subprocess
from datetime import datetime
from common import (
	compare_blob_names,
	compare_md5_hashes,
)


def get_combined_manifest_loc(path):
	command = f"gsutil ls {path} | sort | tail -1"
	file_loc = subprocess.check_output(command, shell=True, text=True, stderr=subprocess.PIPE)
	return file_loc.strip()


def generate_markdown_report(
	timestamp,
	staging,
	team,
	source,
	dataset,
	workflow,
	file_info,
	not_empty_tests,
	metadata_present_tests,
	test_boolean,
	test_result
):
	staging_bucket = f"gs://asap-{staging}-{team}-{source}-{dataset}"
	production_bucket = f"gs://asap-curated-{team}-{source}-{dataset}"

	staging_combined_manifest = file_info[staging]["combined_manifest_df"]
	production_combined_manifest = file_info["curated"]["combined_manifest_df"]

	staging_timestamps = "\n".join(f"- {item}" for item in staging_combined_manifest["timestamp"].unique())
	production_timestamps = "\n".join(f"- {item}" for item in production_combined_manifest["timestamp"].unique())

	staging_workflow_version = ", ".join(staging_combined_manifest["workflow_version"].unique())
	production_workflow_version = ", ".join(production_combined_manifest["workflow_version"].unique())

	staging_workflow_release = ", ".join(staging_combined_manifest["workflow_release"].unique())
	#production_workflow_release = ", ".join(production_combined_manifest["workflow_release"].unique())

	staging_sample_loc = file_info[staging]["sample_list_loc"][0]
	production_sample_loc = file_info["curated"]["sample_list_loc"][0]

	# Compare different envs
	same_files, new_files, deleted_files = compare_blob_names(file_info, staging)
	new_files_rows = "\n".join(f"| {filename} |" for filename in new_files)
	deleted_files_rows = "\n".join(f"| {filename} |" for filename in deleted_files)
	modified_files = compare_md5_hashes(file_info, staging, same_files)
	if same_files == ["N/A"]:
		modified_files_rows = "| N/A | N/A |"
	else:
		modified_files_rows = "\n".join(f"| {filename} | {info['staging_hash']} |" 
								for filename, info in modified_files.items())

	data_integrity_test_rows = "\n".join(
		f"| {file} | {timestamp} | {not_empty_tests[file]} | {metadata_present_tests[file]} |"
		for file in not_empty_tests
	)

	previous_manifest_loc = get_combined_manifest_loc(f"{staging_bucket}/{workflow}/metadata/")
	if previous_manifest_loc == "":
		previous_manifest_loc = "N/A"
	else:
		previous_manifest_loc = f"{previous_manifest_loc}MANIFEST.tsv"

	markdown_content = f"""# Info
## Initial environment
**Environment:** [{staging}]

**Bucket:** {staging_bucket}

**Processing timestamp(s):**
{staging_timestamps}

**Harmonized {workflow} workflow version:** [{staging_workflow_version}]({staging_workflow_release})

**Sample set:** {staging_sample_loc}

**Tests passed:** {test_boolean}

## Target environment
**Environment:** [curated]

**Bucket:** {production_bucket}

**Processing timestamp(s):**
{production_timestamps}

**Harmonized {workflow} workflow version:** [{production_workflow_version}]

**Sample set:** {production_sample_loc}

**Tests passed:** N/A


# Definitions
### Table 1: Definitions
| Term | Definition |
|---------|---------|
| Initial environment | This is where the staging data lives with the intent of promoting it to production. |
| Target environment | This is where the current production data lives with the intent of replacing it with the staging data in the initial environment. |
| New files | Set of new files (i.e. they didn’t exist in previous runs/workflow versions). |
| Modified files | Set of files that have different checksums. |
| Deleted files | Set of files that no longer exist in this version of the pipeline (expected, not an error in the pipeline). |
| Not empty test | A test that checks if all files in buckets are empty or less than or equal to 10 bytes in size. |
| Metadata present test | A test that checks if all files in buckets have an associated metadata. The metadata file (MANIFEST.tsv) is generated in the workflow. |


# Files changed
## New (i.e. only in staging)
| filename |
|---------|
{new_files_rows}

## Modified
| filename | hash (md5) |
|---------|---------|
{modified_files_rows}

## Deleted (i.e. only in prod)
| filename |
|---------|
{deleted_files_rows}


# File tests
### Table 2: Summary of data integrity tests results
Summarizes the results of all data integrity tests on all files and when the tests were run. If all tests pass for all files, the data will be promoted and the "all tests passed" column will show a ✅. If any test fails for any file, the data will not be promoted and the "all tests passed" column will show a ❌.
| timestamp | all tests passed |
|---------|---------|
| {timestamp} | {test_result} |

### Table 3: Data integrity tests results for each file
Individual data integrity test results for each file (a comprehensive variation of [Table 2](#table-2-summary-of-data-integrity-tests-results)) and when the tests were run. Tests involve checking if files are not empty and have an associated metadata (more details in [Table 1](#table-1-definitions)). All tests for all files must pass in order for data to be promoted.
| filename | timestamp | not empty test | metadata present test |
|---------|---------|---------|-------------|
{data_integrity_test_rows}


# Combined manifest file locations
**New manifest:** {staging_bucket}/{workflow}/metadata/{timestamp}/MANIFEST.tsv

**Previous manifest:** {previous_manifest_loc}
"""

	with open(f"{team}_{source}_{dataset}_data_promotion_report.md", "w") as file:
		file.write(markdown_content)
