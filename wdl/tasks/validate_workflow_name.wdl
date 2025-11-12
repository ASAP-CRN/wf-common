version 1.0

task validate_workflow_name {
	input {
		String workflow_name
		String zones
	}

	command <<<
		set -euo pipefail

		# Sc/sn RNAseq pipeline
		if [[ ~{workflow_name} == "pmdbs_sc_rnaseq" ]]; then
			echo "Running: [~{workflow_name}]"
		elif [[ ~{workflow_name} == "pmdbs_multimodal_sc_rnaseq" ]]; then
			echo "Running: [~{workflow_name}]"
		elif [[ ~{workflow_name} == "mouse_sc_rnaseq" ]]; then
			echo "Running: [~{workflow_name}]"
		else
			echo "[ERROR] Invalid workflow name for sc/sn RNAseq: [~{workflow_name}]"
			printf "Please select a valid workflow name for sc/sn RNAseq:\n  pmdbs_sc_rnaseq\n  pmdbs_multimodal_sc_rnaseq\n  mouse_sc_rnaseq"
			exit 1
		fi
	>>>

	output {
	}

	runtime {
		docker: "gcr.io/google.com/cloudsdktool/google-cloud-cli:524.0.0-slim"
		cpu: 2
		memory: "4 GB"
		disks: "local-disk 10 HDD"
		preemptible: 3
		zones: zones
	}
}
