#!/usr/bin/env python3

from datetime import datetime


def generate_markdown_report(team):
    team = "your_team"
    STAGING_BUCKET_TYPE = "staging_type"
    staging_bucket = "gs://your-staging-bucket"
    staging_preprocess_timestamps = ["2023-01-01T12:00:00Z", "2023-01-02T12:00:00Z"]
    staging_preprocess_workflow_versions = ["v1.0", "v1.1"]
    staging_cohort_analysis_timestamps = "2023-01-03T12:00:00Z"
    staging_cohort_analysis_workflow_versions = "v2.0"
    staging_sample_list_loc = "/path/to/sample_list"
    all_tests_result_status = "✅"
    prod_preprocess_timestamps = ["2023-02-01T12:00:00Z"]
    prod_preprocess_workflow_versions = ["v1.0"]
    prod_cohort_analysis_timestamps = "2023-02-03T12:00:00Z"
    prod_cohort_analysis_workflow_versions = "v2.0"
    prod_sample_list_loc = "/path/to/prod_sample_list"
    new_files_table = "| new_file1 |\n| new_file2 |\n"
    mod_files_table = "| mod_file1 |\n| mod_file2 |\n"
    deleted_files_table = "| del_file1 |\n| del_file2 |\n"
    table1 = "| 2023-01-01T12:00:00Z | ✅ |\n| 2023-01-02T12:00:00Z | ❌ |\n"
    table2 = "| file1 | 2023-01-01T12:00:00Z | ✅ | ✅ |\n| file2 | 2023-01-02T12:00:00Z | ❌ | ✅ |\n"
    current_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")

    markdown_content = f"""# Info
    ## Initial environment
    **Environment:** [{STAGING_BUCKET_TYPE}]

    **Bucket:** {staging_bucket}

    **Preprocessing timestamp(s):**
    {''.join(f'* {ts}\n' for ts in staging_preprocess_timestamps)}

    **Preprocessing version(s):**
    {''.join(f'* {ver}\n' for ver in staging_preprocess_workflow_versions)}

    **Cohort analysis timestamp:** {staging_cohort_analysis_timestamps}

    **Cohort analysis version:** {staging_cohort_analysis_workflow_versions}

    **Harmonized PMDBS workflow version:** ({staging_cohort_analysis_workflow_versions})

    **Sample set:** {staging_sample_list_loc}

    **Tests passed:** {all_tests_result_status}

    ## Target environment
    **Environment:** [curated]

    **Bucket:** gs://asap-curated-data-{team}

    **Preprocessing timestamp(s):**
    {''.join(f'* {ts}\n' for ts in prod_preprocess_timestamps)}

    **Preprocessing version(s):**
    {''.join(f'* {ver}\n' for ver in prod_preprocess_workflow_versions)}

    **Cohort analysis timestamp:** {prod_cohort_analysis_timestamps}

    **Cohort analysis version:** {prod_cohort_analysis_workflow_versions}

    **Harmonized PMDBS workflow version:** ({prod_cohort_analysis_workflow_versions})

    **Sample set:** {prod_sample_list_loc}

    **Tests passed:** N/A


    # Definitions
    ### Table 1: Definitions
    | Term | Definition |
    |---------|---------|
    | New files | Set of new files (i.e. they didn’t exist in previous runs/workflow versions). |
    | Modified files | Set of files that have different checksums. |
    | Deleted files | Set of files that no longer exist in this version of the pipeline (expected, not an error in the pipeline). |
    | Not empty test | A test that checks if all files in buckets are empty or less than or equal to 10 bytes in size. |
    | Metadata present test | A test that checks if all files in buckets have an associated metadata. The metadata file (MANIFEST.tsv) is generated in the workflow. |


    # Files changed
    ## New (i.e. only in staging)
    {new_files_table}

    ## Modified
    {mod_files_table}

    ## Deleted (i.e. only in prod)
    {deleted_files_table}


    # File tests
    ### Table 2: Summary of data integrity tests results
    Summarizes the results of all data integrity tests on all files and when the tests were run. If all tests pass for all files, the data will be promoted and the "all tests passed" column will show a ✅. If any test fails for any file, the data will not be promoted and the "all tests passed" column will show a ❌.
    | timestamp | all tests passed |\n|---------|---------|\n
    {table1}

    ### Table 3: Data integrity tests results for each file
    Individual data integrity test results for each file (a comprehensive variation of [Table 2](#table-2-summary-of-data-integrity-tests-results)) and when the tests were run. Tests involve checking if files are not empty and have an associated metadata (more details in [Table 1](#table-1-definitions)). All tests for all files must pass in order for data to be promoted.
    | filename | timestamp | not empty test | metadata present test |\n|---------|---------|---------|-------------|\n
    {table2}


    # Manifest file locations
    **New manifest:** {staging_bucket}/metadata/{current_timestamp}/MANIFEST.tsv

    **Previous manifest:** /path/to/previous_manifest_combined_loc
    """

    with open(f"{team}_data_promotion_report.md", "w") as file:
        file.write(markdown_content)
