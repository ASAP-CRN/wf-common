version 1.0

task fastqc {
	input {
		String project_id
		Array[File] fastq_R1s
		Array[File] fastq_R2s

		String raw_data_path
		Array[Array[String]] workflow_info
		String billing_project
		String container_registry
		String zones
	}

	Int threads = 4
	Int mem_gb = ceil(threads * 2)
	Int disk_size = ceil((size(fastq_R1s, "GB") + size(fastq_R2s, "GB") + 20))

	Array[File] paired_fastqs = flatten([fastq_R1s, fastq_R2s])
	String first_fastq_basename = basename(paired_fastqs[0])

	command <<<
		set -euo pipefail

		mkdir -p ~{project_id}_fastqc_reports

		fastqc \
			--extract \
			--outdir ~{project_id}_fastqc_reports \
			--threads ~{threads} \
			~{sep=' ' paired_fastqs}

		# Includes ZIP and HTML files
		trimmed_fastqs=$(echo ~{first_fastq_basename} | grep "trimmed" || [[ $? == 1 ]])
		if [[ -z "$trimmed_fastqs" ]]; then
			tar -czvf "~{project_id}_fastqc_reports.tar.gz" "~{project_id}_fastqc_reports"
			upload_outputs \
				-b ~{billing_project} \
				-d ~{raw_data_path} \
				-i ~{write_tsv(workflow_info)} \
				-o "~{project_id}_fastqc_reports.tar.gz"
		else
			tar -czvf "~{project_id}_trimmed_fastqc_reports.tar.gz" "~{project_id}_fastqc_reports"
			upload_outputs \
				-b ~{billing_project} \
				-d ~{raw_data_path} \
				-i ~{write_tsv(workflow_info)} \
				-o "~{project_id}_trimmed_fastqc_reports.tar.gz"
		fi
	>>>

	output {
		String fastqc_reports_tar_gz =  "~{raw_data_path}/~{project_id}_fastqc_reports.tar.gz"
		String trimmed_fastqc_reports_tar_gz =  "~{raw_data_path}/~{project_id}_trimmed_fastqc_reports.tar.gz"
	}

	runtime {
		docker: "~{container_registry}/fastqc:0.12.0"
		cpu: threads
		memory: "~{mem_gb} GB"
		disks: "local-disk ~{disk_size} HDD"
		preemptible: 3
		zones: zones
	}
}
