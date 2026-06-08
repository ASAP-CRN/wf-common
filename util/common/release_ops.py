#!/usr/bin/env python3
"""Releases-Sheet loading, release constants, and dataset slug classifiers.

Pulls the live Releases Google Sheet as the single source of truth and exposes
the derived bucket lists and ordered category constants, plus the slug-based
classifiers used when Sheet metadata isn't available (e.g. internal QC datasets).
"""

import os
import logging
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def get_releases_df(
	sheet_id: str = "1Qx4W3EsGQwRHXKtDd6jBnEyPGsuhxB8YCVdgJ-Mn6Hs",
	tab_name: str = "Releases_src",
	credentials_path: str = os.path.expanduser("~/.config/gspread/credentials.json")
) -> pd.DataFrame:
	if not os.path.exists(credentials_path):
		raise FileNotFoundError(f"Credentials file not found: {credentials_path}; look at README")
	creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
	gc = gspread.authorize(creds)
	ws = gc.open_by_key(sheet_id).worksheet(tab_name)
	return pd.DataFrame(ws.get_all_records())

releases_df = get_releases_df()
releases_df["raw_buckets"] = "gs://asap-raw-" + releases_df["dataset_id"]
releases_df["dev_buckets"] = "gs://asap-dev-" + releases_df["dataset_id"]

ALL_TEAMS = releases_df["team_id"].unique().tolist()

## Minor and Major Release that includes pipeline/curated outputs
### The latest_workflow_version column is being used to infer datasets with pipeline outputs
unembargoed_dev_buckets_and_workflow_version_outputs = (
	releases_df[
		releases_df["latest_workflow_version"].str.startswith("v", na=False)
	]
	.sort_values("latest_workflow_version")
	.drop_duplicates(subset="dev_buckets", keep="last")
	.set_index("dev_buckets")["latest_workflow_version"]
	.to_dict()
)
## Urgent and Minor Release or platforming exercise during a Major Release
completed_platforming_raw_buckets = (
	releases_df[
		~releases_df["latest_workflow_version"].str.startswith("v", na=False)
	]["raw_buckets"]
	.drop_duplicates()
	.tolist()
)


######################################################################
##### SLUG CLASSIFIERS AND ORDERED CATEGORY LISTS ####################
######################################################################
# Used by generate_dataset_summary_table and generate_brain_bank_summary
# to categorize datasets by assay type, organism, and sample source when
# the Releases Sheet metadata isn't available (e.g., internal QC datasets).

# Ordered list of assay/data-type categories. Matches the column order in
# the dataset summary pivot tables. Add new categories here as they appear.
ASSAY_ORDER = [
	"sc/snRNA-seq",
	"sc/snATAC-seq",
	"sc/sn Multiome",
	"Bulk RNA-seq",
	"Spatial Transcriptomics",
	"Proteomics",
	"Metabolomics",
	"Lipidomics",
	"Genetics",
	"WGS",
	"Metagenomics",
]

HUMAN_SOURCES_ORDER = [
	"Brain tissue",
	"Cell lines",
	"Gastrointestinal",
	"Fecal",
]

MOUSE_SOURCES_ORDER = [
	"Brain tissue",
	"Liver tissue",
	"Lung tissue",
	"Kidney tissue",
	"Plasma",
	"Embryonic fibroblast",
	"Fecal",
]


def team_from_slug(slug):
	"""prod-team-hafler-pmdbs-... → hafler. Returns 'unknown' if the slug
	does not match the expected prod-team-<name>-... shape."""
	parts = str(slug).split("-")
	if len(parts) >= 3 and parts[0] == "prod" and parts[1] == "team":
		return parts[2]
	return "unknown"


def classify_assay(value):
	"""Map an assay value (free-text Releases-Sheet `assay`, or a dataset slug)
	to an entry in ASSAY_ORDER. Returns None if no pattern matches — callers
	decide their own fallback (e.g. 'Unclassified').

	The patterns cover both Releases-Sheet conventions (snake_case, free-text
	like 'Bulk_RNA_Seq', 'mass spec') and slug conventions (kebab-case like
	'sc-rnaseq', 'ms-mb-plasma'). Order matters: more specific patterns are
	checked before more general ones."""
	s = str(value).lower()

	# ATAC-seq first — broader sc/sn would otherwise capture sc_atac/sn_atac
	if any(x in s for x in [
		"sn-atacseq", "sc-atacseq", "snatacseq", "scatacseq",
		"sc_atac", "sn_atac", "scatac", "snatac",
		"sc-atac", "sn-atac", "atacseq", "atac_seq", "atac-seq",
	]):
		return "sc/snATAC-seq"

	# Multiome (paired RNA+ATAC, e.g. 10x Multiome) — checked before sc/sn RNA-seq
	# so the multimodal/multiome keyword takes precedence over a generic sc-/sn- match
	if any(x in s for x in ["multimodal", "multiome", "multiomic"]):
		return "sc/sn Multiome"

	# sc/sn RNA-seq — covers both kebab-case slugs and snake_case Sheet values
	if any(x in s for x in [
		"sn-rnaseq", "sc-rnaseq", "scrnaseq", "snrnaseq",
		"single-cell", "single-nucleus", "single_cell", "single_nucleus",
		"scrna", "snrna", "sc-rna", "sn-rna",
		"sc_", "sn_",
	]):
		return "sc/snRNA-seq"

	if "bulk" in s:
		return "Bulk RNA-seq"

	if any(x in s for x in ["spatial", "visium", "geomx", "cosmx", "xenium", "nanostring"]):
		return "Spatial Transcriptomics"

	# Mass-spec assays — tighten ms-p / ms-mb / ms-l patterns so they don't collide.
	# Bare `ms-p` is allowed when it's the whole value (e.g. Sheet input "ms-p")
	# or at the end of a slug; the slug-tightened `ms-p-` / `ms-p_` handles
	# embedded positions.
	if (any(x in s for x in ["ms-p-", "ms-p_", "proteom", "mass-spec", "mass_spec", "mass spec"])
			or s.endswith("ms-p") or s == "ms-p"):
		return "Proteomics"
	if any(x in s for x in ["ms-mb-", "ms-mb_", "metabolom"]) or s.endswith("ms-mb") or s == "ms-mb":
		return "Metabolomics"
	if any(x in s for x in ["ms-l-", "ms-l_", "lipidom"]) or s.endswith("ms-l") or s == "ms-l":
		return "Lipidomics"

	# WGS before Genetics — Genetics is the catch-all for genotyping/SNP/array data
	if any(x in s for x in ["wgs", "whole-genome", "whole_genome"]):
		return "WGS"
	if any(x in s for x in ["genetic", "genotyp", "gwas", "snp", "variant"]):
		return "Genetics"
	if any(x in s for x in ["metagenom", "shotgun", "microbiome", "16s"]):
		return "Metagenomics"

	return None


def classify_organism(value):
	"""Map an organism value (free-text Releases-Sheet `organism`, or a dataset
	slug) to 'Human' or 'Mouse'. Returns None if neither matches.

	Cell-line / in-vitro values default to Human unless the input also says
	'mouse' (so a slug like prod-team-alessi-mefs-... resolves to Mouse via
	the MEF check below)."""
	s = str(value).lower()
	# Human keywords (slug-side: pmdbs; Sheet-side: human, homo)
	if any(x in s for x in ["human", "homo", "pmdbs"]):
		return "Human"
	# MEF first — must come before generic mouse check so it doesn't get
	# swallowed by a slug that happens to contain 'mef' but not 'mouse'
	if "mef" in s or "mefs" in s:
		return "Mouse"
	if any(x in s for x in ["mouse", "mus", "sulzer-fecal-metagenome-fp-spf"]):
		return "Mouse"
	# Cell-line / in-vitro datasets — assume Human unless the value also
	# says mouse (caller decides further disambiguation if needed)
	if any(x in s for x in ["invitro", "ipsc", "hek"]):
		return "Human" if "mouse" not in s else "Mouse"
	return None


def classify_source(value, organism=""):
	"""Map a sample-source value (slug or Releases-Sheet `sample_source`) to an
	entry in HUMAN_SOURCES_ORDER / MOUSE_SOURCES_ORDER. Returns None if no
	pattern matches.

	`organism` is optional and is used only to disambiguate Fecal samples
	when relevant. Pass '' (the default) when classifying from a slug — Fecal
	doesn't need disambiguation at the slug level since both Human Fecal and
	Mouse Fecal collapse to the same 'Fecal' bucket.

	When called from the Releases-Sheet path, pass the organism so Fecal can
	be split by Human/Mouse appropriately (currently both still map to 'Fecal',
	but the organism arg is preserved for future disambiguation needs)."""
	s = str(value).lower()
	o = str(organism).lower()

	# MEF = mouse embryonic fibroblast → must come before generic cell-line check
	if "mef" in s or "fibroblast" in s or "embryon" in s:
		return "Embryonic fibroblast"

	if any(x in s for x in [
		"brain", "pmdbs", "postmortem", "midbrain", "striatum", "cortex",
		"substantia-nigra", "substantia nigra",
		"hippocampus", "cerebellum", "neural",
	]):
		return "Brain tissue"

	if any(x in s for x in ["colon", "gastro", "intestin", "gi-tract", "gi tract", "gut"]):
		return "Gastrointestinal"

	# Fecal: handles both Human Fecal and Mouse Fecal. The organism arg lets
	# callers route to bucket-specific behavior later; for now both return Fecal.
	if any(x in s for x in ["fecal", "stool", "feces", "microbiome", "metagenom"]):
		return "Fecal"

	if "liver" in s:
		return "Liver tissue"
	if "lung" in s:
		return "Lung tissue"
	if "kidney" in s or "renal" in s:
		return "Kidney tissue"
	if any(x in s for x in ["plasma", "serum"]) or "-blood-" in s or s.endswith("-blood") or s == "blood":
		return "Plasma"

	if any(x in s for x in [
		"cell line", "cell-line", "invitro", "in vitro", "ipsc", "hek",
		"neuronal cell", "hesc", "hpsc",
	]):
		return "Cell lines"

	return None


embargoed_dev_buckets = [
	# Human PMDBS Multimodal Seq
	"gs://asap-raw-team-wood-pmdbs-multimodal-seq",
	# Human PMDBS Spatial Transcriptomics Nanostring GeoMx
	"gs://asap-dev-team-vila-pmdbs-spatial-geomx-thlc",
	"gs://asap-dev-team-vila-pmdbs-spatial-geomx-unmasked",
]


def list_teams():
	logging.info("Available teams:")
	for team in ALL_TEAMS:
		logging.info(team)


__all__ = [
    "SCOPES", "get_releases_df", "releases_df", "ALL_TEAMS",
    "unembargoed_dev_buckets_and_workflow_version_outputs",
    "completed_platforming_raw_buckets",
    "ASSAY_ORDER", "HUMAN_SOURCES_ORDER", "MOUSE_SOURCES_ORDER",
    "team_from_slug", "classify_assay", "classify_organism", "classify_source",
    "embargoed_dev_buckets", "list_teams",
]
