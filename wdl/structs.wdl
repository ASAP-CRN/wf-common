version 1.0

struct Sample {
	String sample_id
	String? batch

	Array[File]+ fastq_R1s
	Array[File]+ fastq_R2s
	Array[File] fastq_I1s
	Array[File] fastq_I2s
}

struct Project {
	String project_id
	Array[Sample] samples

	File? project_sample_metadata_csv

	Boolean run_project_cohort_analysis

	String raw_data_bucket
	Array[String] staging_data_buckets
}
