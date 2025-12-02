version 1.0

task get_workflow_name {
	input {
		String organism
		String zones
	}

	command <<<
		set -euo pipefail

		# Sc/sn RNAseq pipeline
		if [[ ~{organism} == "human" ]]; then
			echo "Detected: [~{organism}]"
			workflow_name="pmdbs_sc_rnaseq"
			echo "${workflow_name}" > workflow_name.txt
			echo "Running: [${workflow_name}]"
		elif [[ ~{organism} == "mouse" ]]; then
			echo "Detected: [~{organism}]"
			workflow_name="mouse_sc_rnaseq"
			echo "${workflow_name}" > workflow_name.txt
			echo "Running: [${workflow_name}]"
		else
			echo "[ERROR] Invalid organism for sc/sn RNAseq: [~{organism}]"
			printf "Please select a valid organism for sc/sn RNAseq:\n  human\n  mouse"
			exit 1
		fi
	>>>

	output {
		String workflow_name = read_string("workflow_name.txt")
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
