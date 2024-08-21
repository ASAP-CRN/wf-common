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

	Array[String] fastq_R1_basenames = [sub(basename(fastq_R1, ".gz"), "(\.fastq|\.fq)$", "") for fastq_R1 in fastq_R1s]
	Array[String] fastq_R2_basenames = [sub(basename(fastq_R2, ".gz"), "(\.fastq|\.fq)$", "") for fastq_R2 in fastq_R2s]
	Array[String] fastq_basenames = fastq_R1_basenames + fastq_R2_basenames

	command <<<
		set -euo pipefail

		mkdir -p ~{project_id}_fastqc_reports

		fastqc \
			--extract \
			--outdir ~{project_id}_fastqc_reports \
			--threads ~{threads} \
			~{sep=' ' fastq_basenames}

		# Includes ZIP and HTML files
		tar -czvf "~{project_id}_fastqc_reports.tar.gz" "~{project_id}_fastqc_reports"

		upload_outputs \
			-b ~{billing_project} \
			-d ~{raw_data_path} \
			-i ~{write_tsv(workflow_info)} \
			-o "~{project_id}_fastqc_reports.tar.gz"
	>>>

	output {
		String fastqc_reports_tar_gz =  "~{raw_data_path}/~{project_id}_fastqc_reports.tar.gz"
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
