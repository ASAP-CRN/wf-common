version 1.0

struct Sample {
	String sample_id
	String? batch

	Array[File]+ fastq_R1s
	Array[File]+ fastq_R2s
	Array[File] fastq_I1s
	Array[File] fastq_I2s

	File? visium_brightfield_image
	String? visium_slide_serial_number
	String? visium_capture_area
}

struct Project {
	String team_id
	String dataset_id
	Array[Sample] samples

	File? project_sample_metadata_csv
	File? project_condition_metadata_csv

	File? geomx_config_ini
	File? geomx_lab_annotation_xlsx

	Boolean run_project_cohort_analysis

	String raw_data_bucket
	Array[String] staging_data_buckets
}
