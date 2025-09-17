version 1.0

task upload_final_outputs {
	input {
		Array[String] output_file_paths

		Array[String] staging_data_buckets
		String staging_data_path
		String billing_project
		String zones
	}

	command <<<
		set -euo pipefail

		# Write the file manifest
		sed 's~$~.meta.tsv~' ~{write_lines(output_file_paths)} > metadata_paths.txt

		echo -e "filename\tmd5_hash\ttimestamp\tworkflow\tworkflow_version\tworkflow_release" > MANIFEST.tsv
		mkdir metadata
		gcloud storage cp --billing-project=~{billing_project} -I ./metadata/ \
		< metadata_paths.txt

		find metadata -type f -exec cat {} \; \
		>> MANIFEST.tsv

		while read -r staging_data_bucket || [[ -n "${staging_data_bucket}" ]]; do
			# Remove files currently existing at the target path, if they exist
			if gcloud storage ls --billing-project=~{billing_project} "${staging_data_bucket}/~{staging_data_path}/**"; then
				gcloud storage rm --billing-project=~{billing_project} \
					"${staging_data_bucket}/~{staging_data_path}/**"
			fi

			# Copy files to the staging data path
			gcloud storage cp --billing-project=~{billing_project} \
				-I \
				"${staging_data_bucket}/~{staging_data_path}/" \
			< ~{write_lines(output_file_paths)}

			# Upload the manifest to the staging data path
			gcloud storage cp --billing-project=~{billing_project} \
				MANIFEST.tsv \
				"${staging_data_bucket}/~{staging_data_path}/"

			echo "${staging_data_bucket}/~{staging_data_path}/MANIFEST.tsv" \
			>> manifest_locs.txt
		done < ~{write_lines(staging_data_buckets)}
	>>>

	output {
		Array[String] manifests = read_lines("manifest_locs.txt")
	}

	runtime {
		docker: "gcr.io/google.com/cloudsdktool/google-cloud-cli:524.0.0-slim"
		cpu: 2
		memory: "4 GB"
		disks: "local-disk 20 HDD"
		preemptible: 3
		zones: zones
	}
}
