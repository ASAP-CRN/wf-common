#!/usr/bin/env python3

def parse_dataset_id(dataset_id):
    """Parse dataset_id of format 'team-{team}-{source}-{assay}[-{context}]'.
    e.g. 'team-jakobsson-pmdbs-sn-rnaseq' ->
        team='jakobsson', source='pmdbs', dataset_name='jakobsson-pmdbs-sn-rnaseq'
    dataset_name is dataset_id without the 'team-' prefix.
    """
    parts = dataset_id.split("-")
    if len(parts) < 4 or parts[0] != "team":
        raise SystemExit(
            f"Invalid dataset_id format: '{dataset_id}'. "
            "Expected 'team-{team}-{source}-{assay}[-{context}]'."
        )
    team = parts[1]
    source = parts[2]
    assay = "-".join(parts[3:])
    return team, source, assay
