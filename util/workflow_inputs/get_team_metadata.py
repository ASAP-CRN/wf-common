#!/usr/bin/env python3

from argparse import ArgumentParser
import pandas as pd
import os
from pathlib import Path
import re
from common import parse_dataset_id


released_team_dataset_ids = [
    # Sn/sc RNAseq datasets
    "team-hafler-pmdbs-sn-rnaseq-pfc",
    "team-lee-pmdbs-sn-rnaseq",
    "team-jakobsson-pmdbs-sn-rnaseq",
    "team-scherzer-pmdbs-sn-rnaseq-mtg",
    "team-hardy-pmdbs-sn-rnaseq",
    "team-sulzer-pmdbs-sn-rnaseq",
    "team-biederer-mouse-sc-rnaseq",
    "team-cragg-mouse-sn-rnaseq-striatum",
    # Multimodal sn/sc RNAseq datasets
    "team-wood-pmdbs-multimodal-seq",
    # Bulk RNAseq datasets
    "team-hardy-pmdbs-bulk-rnaseq",
    "team-lee-pmdbs-bulk-rnaseq-mfg",
    "team-wood-pmdbs-bulk-rnaseq",
    "team-jakobsson-pmdbs-bulk-rnaseq",
    "team-jakobsson-invitro-bulk-rnaseq-dopaminergic",
    "team-jakobsson-invitro-bulk-rnaseq-microglia",
    # Spatial GeoMx datasets
    "team-edwards-pmdbs-spatial-geomx-th",
    "team-vila-pmdbs-spatial-geomx-thlc",
    "team-vila-pmdbs-spatial-geomx-unmasked",
    # Spatial Visium datasets
    "team-cragg-mouse-spatial-visium-striatum",
    "team-scherzer-pmdbs-spatial-visium-mtg",
]


def load_fastq_locations(dataset_id, team, source, assay, output_dir):
    fastq_loc_file = f"{output_dir}/{team}/{source}-{assay}/{dataset_id}.fastq_locs.txt"
    fastq_locations = {}
    with open(fastq_loc_file, "r") as f:
        for line in f:
            path = line.strip()
            file = path.split("/")[-1]
            if file not in fastq_locations:
                fastq_locations[file] = path
            else:
                if re.search(r"Undetermined", file):
                    continue
                elif "team-voet-pmdbs-sn-atacseq-10x" in dataset_id:
                    continue
                else:
                    raise SystemExit(
                        f"Duplicate fastq file name found for dataset {dataset_id}: [{file}]"
                    )
    return fastq_locations


# Get the location of fastqs corresponding to samples
def add_fastq_info(data, dataset_id, team, source, assay, metadata_dir, output_dir):
    fastq_locations = load_fastq_locations(dataset_id, team, source, assay, output_dir)

    sample_fastq_r1s = []
    sample_fastq_r2s = []
    sample_fastq_r3s = []
    sample_fastq_i1s = []
    sample_fastq_i2s = []

    data_file = f"{metadata_dir}/{team}/{source}-{assay}/DATA.csv"
    if not os.path.exists(data_file):
        raise SystemExit(f"Failed to find data file [{data_file}] for dataset [{dataset_id}]")
    else:
        fastq_data = pd.read_csv(data_file, index_col=0)
        is_atac = any(assay in dataset_id for assay in ("sn-atacseq", "sc-atacseq"))

        # pool_id is only populated for multiplexed datasets
        def has_pool_id(df):
            if "pool_id" not in df.columns:
                return False
            pool_ids = df["pool_id"].dropna().astype(str).str.strip()
            return (~pool_ids.str.lower().isin(["", "na", "nan"])).any()

        is_multiplexed = has_pool_id(data) and has_pool_id(fastq_data)

        # For multiplexed data, match on (ASAP_sample_id, pool_id) instead of
        # sample_id, since the same sample can appear in multiple pools/batches.
        if is_multiplexed:
            merge_keys = ["ASAP_sample_id", "pool_id"]
        else:
            merge_keys = ["sample_id"]

        # Check if merge_keys and replicate match in SAMPLE.csv and DATA.csv
        check_cols = merge_keys + ["replicate"]
        data_lower = data.copy()
        data_lower[check_cols] = data_lower[check_cols].astype(str).apply(lambda x: x.str.lower())
        fastq_data_lower = fastq_data.copy()
        fastq_data_lower[check_cols] = fastq_data_lower[check_cols].astype(str).apply(lambda x: x.str.lower())
        merged = data_lower.merge(fastq_data_lower, on=check_cols, how="left", indicator=True)
        if "left_only" in merged["_merge"].unique() or "right_only" in merged["_merge"].unique():
            # Temporary fix for unmatched replicate columns produced by DTi
            missing_samples = merged.loc[merged["_merge"].isin(["left_only", "right_only"]), merge_keys[0]]
            if not missing_samples.empty:
                print(f"Can't find {missing_samples.tolist()} from SAMPLE.csv in DATA.csv; using SAMPLE.csv for replicates")
            fastq_data = fastq_data.drop(columns=["replicate"], errors="ignore").merge(
                data[merge_keys + ["replicate"]], on=merge_keys, how="left"
            )

        for _, row_data in data.iterrows():
            filter_mask = pd.Series(True, index=fastq_data.index)
            for key in merge_keys:
                filter_mask &= (fastq_data[key] == row_data[key])
            filter_mask &= (fastq_data["replicate"] == row_data["replicate"])
            sample_fastqs = sorted(fastq_data[filter_mask]["file_name"].tolist())

            FASTQ_SUFFIX = r"(_[0-9][0-9][0-9]|)\.(fastq|fq)(\.gzgz|\.gz|)$"
            R1_pattern = re.compile(r".*R1" + FASTQ_SUFFIX)
            R2_pattern = re.compile(r".*R2" + FASTQ_SUFFIX)
            R3_pattern = re.compile(r".*R3" + FASTQ_SUFFIX)
            I1_pattern = re.compile(r".*I1" + FASTQ_SUFFIX)
            I2_pattern = re.compile(r".*I2" + FASTQ_SUFFIX)

            # 10x Multiome data
            ATAC_pattern = re.compile(r"atac", re.IGNORECASE)
            RNA_pattern = re.compile(r"rna", re.IGNORECASE)

            # Once the full gs uri path is added, remove duplicates
            def locations_for(pattern, fastqs):
                return list(
                    dict.fromkeys(
                        fastq_locations[fastq_loc.replace(".gzgz", ".gz")]
                        for fastq_loc in fastqs
                        if pattern.search(fastq_loc)
                    )
                )

            # Team Biederer has R3 fastqs and Team Voet ATAC has R3 fastqs, which means that R3=R2 and R2=I2
            if dataset_id == "team-voet-pmdbs-sn-multimodal":
                atac_fastqs = [
                    fastq_loc
                    for fastq_loc in sample_fastqs
                    if ATAC_pattern.search(os.path.basename(fastq_loc))
                ]
                rna_fastqs = [
                    fastq_loc
                    for fastq_loc in sample_fastqs
                    if not ATAC_pattern.search(os.path.basename(fastq_loc))
                ]

                if atac_fastqs and not any(R3_pattern.search(f) for f in atac_fastqs):
                    raise SystemExit(
                        f"Found ATAC fastqs with no R3 for sample {row_data["ASAP_sample_id"]}:\n"
                        f"{sorted(os.path.basename(f) for f in atac_fastqs)}"
                    )

                fastq_R1s = locations_for(R1_pattern, atac_fastqs) + locations_for(R1_pattern, rna_fastqs)
                fastq_R2s = locations_for(R3_pattern, atac_fastqs) + locations_for(R2_pattern, rna_fastqs)
                fastq_I1s = locations_for(I1_pattern, atac_fastqs) + locations_for(I1_pattern, rna_fastqs)
                fastq_I2s = locations_for(R2_pattern, atac_fastqs) + locations_for(I2_pattern, rna_fastqs)

            elif dataset_id == "team-biederer-mouse-sc-rnaseq":
                shifted = any(R3_pattern.search(fastq_loc) for fastq_loc in sample_fastqs)
                
                fastq_R1s = locations_for(R1_pattern, sample_fastqs)
                fastq_I1s = locations_for(I1_pattern, sample_fastqs)
                if shifted:
                    fastq_R2s = locations_for(R3_pattern, sample_fastqs)
                    fastq_I2s = locations_for(R2_pattern, sample_fastqs)
                else:
                    fastq_R2s = locations_for(R2_pattern, sample_fastqs)
                    fastq_I2s = locations_for(I2_pattern, sample_fastqs)

            else:
                fastq_R1s = locations_for(R1_pattern, sample_fastqs)
                fastq_R2s = locations_for(R2_pattern, sample_fastqs)
                fastq_I1s = locations_for(I1_pattern, sample_fastqs)
                fastq_I2s = locations_for(I2_pattern, sample_fastqs)

            fastq_R1s = list(dict.fromkeys(fastq_R1s))
            fastq_R2s = list(dict.fromkeys(fastq_R2s))
            fastq_I1s = list(dict.fromkeys(fastq_I1s))
            fastq_I2s = list(dict.fromkeys(fastq_I2s))

            if is_atac:
                fastq_R3s = locations_for(R3_pattern, sample_fastqs)
            else:
                fastq_R3s = []

            if len(fastq_R1s) == 0 or len(fastq_R2s) == 0:
                raise SystemExit(
                    f"Expected to find 'R1' in read 1 and 'R2' in read 2, but didn't for sample {row_data['ASAP_sample_id']}\nR1s: {fastq_R1s}\nR2s: {fastq_R2s}\n{sample_fastqs}"
                )
            else:
                sample_fastq_r1s.append(fastq_R1s)
                sample_fastq_r2s.append(fastq_R2s)
                sample_fastq_r3s.append(fastq_R3s)
                sample_fastq_i1s.append(fastq_I1s)
                sample_fastq_i2s.append(fastq_I2s)

    data["fastq_R1s"] = sample_fastq_r1s
    data["fastq_R2s"] = sample_fastq_r2s
    data["fastq_R3s"] = sample_fastq_r3s
    data["fastq_I1s"] = sample_fastq_i1s
    data["fastq_I2s"] = sample_fastq_i2s

    return data


def parse_metadata_file(dataset_id, metadata_dir, output_dir):
    team, source, assay = parse_dataset_id(dataset_id)

    sample_metadata_file = f"{metadata_dir}/{team}/{source}-{assay}/SAMPLE.csv"
    subject_metadata_file = f"{metadata_dir}/{team}/{source}-{assay}/SUBJECT.csv"
    study_metadata_file = f"{metadata_dir}/{team}/{source}-{assay}/STUDY.csv"
    spatial_metadata_file = f"{metadata_dir}/{team}/{source}-{assay}/SPATIAL.csv"

    if not os.path.exists(sample_metadata_file):
        raise SystemExit(
            f"Failed to find metadata file [{sample_metadata_file}] for dataset [{dataset_id}]"
        )
    elif not os.path.exists(study_metadata_file):
        raise SystemExit(
            f"Failed to find metadata file [{study_metadata_file}] for dataset [{dataset_id}]"
        )
    else:
        data = pd.read_csv(sample_metadata_file)
        data["replicate"] = data["replicate"].fillna("Rep1")
        data["team_id"] = f"team-{team}"

        data = add_fastq_info(data, dataset_id, team, source, assay, metadata_dir, output_dir)

        data["embargoed"] = dataset_id not in released_team_dataset_ids
        data["dataset_id"] = dataset_id
        data["source"] = source
        data["assay"] = assay

        subject_metadata = pd.read_csv(subject_metadata_file)
        data = data.merge(
            subject_metadata[["ASAP_subject_id", "sex"]],
            on="ASAP_subject_id",
            how="left",
        )

        study_metadata = pd.read_csv(study_metadata_file)
        doi_url = study_metadata["dataset_doi_url"].iloc[0]
        data["dataset_doi_url"] = doi_url

        desired_columns = [
            "team_id",
            "ASAP_dataset_id",
            "ASAP_sample_id",
            "subject_id",
            "ASAP_subject_id",
            "batch",
            "sex",
            "region_level_1",
            "region_level_2",
            "region_level_3",
            "fastq_R1s",
            "fastq_R2s",
            "fastq_R3s",
            "fastq_I1s",
            "fastq_I2s",
            "embargoed",
            "dataset_id",
            "source",
            "assay",
            "dataset_doi_url",
            "pool_id"
        ]

        if os.path.exists(spatial_metadata_file):
            spatial_metadata = pd.read_csv(spatial_metadata_file)
            asap_dataset_id = spatial_metadata["ASAP_dataset_id"][0]
            spatial_metadata["geomx_slide"] = spatial_metadata["sample_id"].apply(lambda x: "-".join(x.split("-")[:2]))
            unique_map_slides = {val: f"ASAP_{asap_dataset_id}_SLIDE_{i+1:04d}" for i, val in enumerate(spatial_metadata["geomx_slide"].unique())}
            spatial_metadata["ASAP_geomx_slide_id"] = spatial_metadata["geomx_slide"].map(unique_map_slides)
            spatial_columns = [
                "geomx_config",
                "geomx_dsp_config",
                "geomx_annotation_file",
                "geomx_slide",
                "ASAP_geomx_slide_id",
                "visium_cytassist",
                "visium_probe_set",
                "visium_slide_ref",
                "visium_capture_area",
            ]
            spatial_metadata_filtered = spatial_metadata[
                ["ASAP_sample_id"] + spatial_columns
            ]
            data = pd.merge(data, spatial_metadata_filtered, on="ASAP_sample_id", how="outer")
            desired_columns.extend(spatial_columns)
            data["ASAP_geomx_slide_id"] = (
                data["ASAP_geomx_slide_id"] + "_BATCH_" + data["batch"].astype(str)
            )
            slide_counts = data["ASAP_geomx_slide_id"].value_counts()
            single_occurrence_slide_ids = slide_counts[slide_counts == 1].index
            if "geomx" in assay and len(single_occurrence_slide_ids) > 0:
                print(f"[WARNING] The following ASAP_geomx_slide_id(s) occur only once: {list(single_occurrence_slide_ids)}.\n"
                    "The GeoMx pipeline will fail during preprocessing")

        data["ASAP_sample_id"] = (
            data["ASAP_sample_id"] + "_" + data["replicate"].astype(str)
        )
        data = data[desired_columns]

        # Initialize blacklisted vars
        blacklisted_samples = []
        blacklisted_pools = []
        blacklisted_donors = []
        blacklisted_pool_donor_pairs = []

        # Blacklisted from Sulzer
        # gs://cromwell-output-640f238f9db54e34/harmonized_pmdbs_analysis/cb8fde91-f207-4eb1-af64-5360b47ca4c7/call-project_cohort_analysis/shard-3/cohort_analysis/591babd2-8c90-4b42-ad7b-cc6b69e0430e/call-filter_and_normalize/shard-32/filter_and_normalize-32.log
        # gs://cromwell-output-640f238f9db54e34/harmonized_pmdbs_analysis/cb8fde91-f207-4eb1-af64-5360b47ca4c7/call-project_cohort_analysis/shard-3/cohort_analysis/591babd2-8c90-4b42-ad7b-cc6b69e0430e/call-filter_and_normalize/shard-33/filter_and_normalize-33.log
        if dataset_id == "team-sulzer-pmdbs-sn-rnaseq":
            blacklisted_samples = [
                "ASAP_PMDBS_000238_s001_BiolRep15",
                "ASAP_PMDBS_000245_s001_BiolRep16",
            ]
        # gs://cromwell-output-640f238f9db54e34/harmonized_pmdbs_analysis/cb8fde91-f207-4eb1-af64-5360b47ca4c7/call-project_cohort_analysis/shard-3/cohort_analysis/591babd2-8c90-4b42-ad7b-cc6b69e0430e/call-filter_and_normalize/shard-38/filter_and_normalize-38.log
        # Error in h(simpleError(msg, call)) :
        #   error in evaluating the argument 'x' in selecting a method for function 'as.matrix': invalid character indexing
        # Calls: %>% ... get_model_pars -> as.matrix -> [ -> [ -> subCsp_ij -> intI
        # Execution halted
        # Command exited with non-zero status 1
            blacklisted_samples.append("ASAP_PMDBS_000228_s001_BiolRep4")
        # Blacklisted from Jakobsson - issue with index files
        # https://dnastack.slack.com/archives/C05MRCXT65U/p1702408239701329
        # blacklisted_samples = ["ASAP_PMBDS_000103_s002_1", "ASAP_PMBDS_000118_s002_1"]
        # Duplicate row.names - these samples have the same ASAP sample ID and replicate
        # blacklisted_samples = ["ASAP_PMBDS_000113_s001_1", "ASAP_PMBDS_000113_s002_1"]
        # blacklisted_samples = blacklisted_samples.extend(
        #     ["ASAP_PMBDS_000113_s001_1", "ASAP_PMBDS_000113_s002_1"]
        # )

        # Team Lee pmdbs-bulk-rnaseq-mfg
        # Different R1 and R2 with ERROR: input files don't contain identical amount of reads
        # Sample ID: 2062HC_MFG_bulk_L000_R2_001.fastq.gz
        # gs://cromwell-output-640f238f9db54e34/pmdbs_bulk_rnaseq_analysis/8e5fa101-fd6c-456e-aec2-c5832216aeff/call-upstream/shard-1/upstream/3bdb4665-317e-4980-9301-c69bfcf3af35/call-trim_and_qc/shard-5/trim_and_qc-5.log
        if dataset_id == "team-lee-pmdbs-bulk-rnaseq-mfg":
            blacklisted_samples.append("ASAP_PMBDS_000014_s002_Rep1") # TODO update once metadata is fixed

        # Team Scherzer pmdbs-spatial-visium-mtg
        # One sample: [error] Image must have at least one dimension >= 2000 for standard slides
        if dataset_id == "team-scherzer-pmdbs-spatial-visium-mtg":
            blacklisted_samples.append("ASAP_PMBDS_000016_s007_Rep1") # TODO update once metadata is fixed

        # Team Cragg cragg-mouse-sn-rnaseq-striatum
        # The R1 FASTQs are corrupt - exclude for now
        if dataset_id == "team-cragg-mouse-sn-rnaseq-striatum":
            blacklisted_samples.extend(["ASAP_MOUSE_000010_s001_BiolRep4", "ASAP_MOUSE_000013_s001_BiolRep1"])

        # Team Voet voet-pmdbs-sn-atacseq-10x
        # Donors that did not pass their QC/not assigned in Vireo
        # The reason why they were not integrating is probably because of some obstruction that may have happened on these two lanes during the wet lab protocol
        if dataset_id == "team-voet-pmdbs-sn-atacseq-10x":
            blacklisted_pools = [
                "AT025e",
                "AT026a"
            ]
        # ASA_137 and ASA_047 are two donors for which we did not manage to get whole genome sequencing data due to DNA extraction failure
        # ASA_038 did not have much grey matter left during tissue sampling, so it is likely that we added white matter instead of grey matter to the pools, which resulted in no nuclei being present for that donor
        # ASA_105 was also not integrating with the rest of the dataset, so we decided to exclude it form the analysis (for cingulate cortex, not for substantia nigra)
            blacklisted_donors = [
                "ASA_137",
                "ASA_038",
                "ASA_105"
            ]
        # AT014 pools, recovered very little nuclei from this ASA_077 donor
            blacklisted_pool_donor_pairs = {
                ("AT014a", "ASA_077"),
                ("AT014e", "ASA_077")
            }

        # Team Voet voet-pmdbs-sn-multimodal
        # No chromatin accessibility files because they contribute only very few cells
        if dataset_id == "team-voet-pmdbs-sn-multimodal":
            blacklisted_pools = [
                "MO005a",
                "MO005b",
                "MO005c",
                "MO006a",
                "MO009a",
                "MO009b",
                "MO009c",
                "MO009d"
            ]

            blacklisted_donors = [
                "ASA_047"
            ]

            blacklisted_pool_donor_pairs = {
                ("MO021d", "ASA_044"),
                ("MO021f", "ASA_044"),
                ("MO021h", "ASA_044"),
                ("MO018e", "ASA_038"),
                ("MO020b", "ASA_044")
            }

        if blacklisted_samples:
            data = data[~data["ASAP_sample_id"].isin(blacklisted_samples)]
        if blacklisted_pools:
            data = data[~data["pool_id"].isin(blacklisted_pools)]
        if blacklisted_donors:
            data = data[~data["subject_id"].isin(blacklisted_donors)]
        pair_index = pd.MultiIndex.from_frame(data[["pool_id", "subject_id"]])
        if blacklisted_pool_donor_pairs:
            data = data[~pair_index.isin(blacklisted_pool_donor_pairs)]

        output_file = f"{output_dir}/{team}/{source}-{assay}/{dataset_id}.metadata.tsv"
        Path(f"{output_dir}/{team}/{source}-{assay}").mkdir(parents=True, exist_ok=True)
        data.to_csv(output_file, sep="\t", index=False)
        print(f"Wrote {output_file}")


def main(args):
    for dataset_id in args.dataset_id:
        print(f"Processing dataset_id={dataset_id}")
        parse_metadata_file(dataset_id, args.metadata_dir, args.output_dir)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Retrieve the required information from team metadata"
    )
    
    parser.add_argument(
        "-m",
        "--metadata-dir",
        type=str,
        required=True,
        help="Directory in which raw team metadata lives",
    )
    parser.add_argument(
        "-d",
        "--dataset-id",
        type=str,
        nargs='+',
        required=True,
        help="Space-delimited dataset ID(s) (e.g. team-jakobsson-pmdbs-sn-rnaseq).",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        required=False,
        default="team_metadata",
        help="Directory to output filtered metadata to",
    )

    args = parser.parse_args()

    main(args)
