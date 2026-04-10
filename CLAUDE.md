# CLAUDE.md — wf-common

This file provides context for AI-assisted development in this repository.
Claude operates in **read-only mode**: no files will be written to this repo directly.
All outputs (scripts, suggested edits, documents) are returned to the user for review
and manual integration.

---

## Project Overview

This repo provides shared WDL workflows, tasks, utility scripts, and Docker images that
are reused across harmonised ASAP CRN bioinformatics workflows. Its purpose is to
centralise common components so that individual workflow repos can import them rather
than duplicating logic.

---

## Repo Structure

```
.
├── wdl/        # Reusable WDL tasks and workflows
├── util/       # Reusable utility scripts shared by other ASAP repositories (Python or shell)
└── docker/     # Dockerfiles for common workflow images
```

- **`wdl/`** — WDL task and workflow definitions intended to be imported by other
  workflow repos via `import` statements.
- **`util/`** — Helper scripts (e.g., data wrangling, file handling) that may be
  called from within WDL tasks or used standalone. Some of these are used by other ASAP repos.
- **`docker/`** — Dockerfiles defining the execution environments for workflow tasks.
  Images are built from these definitions and referenced in WDL `runtime` blocks.

---

## Language and Runtime

- Workflows are written in [Workflow Description Language (WDL)](https://openwdl.org/).
- Docker images define the runtime environment for each task. Image versions should
  be pinned explicitly in WDL `runtime` blocks to ensure reproducibility.
- Utility scripts are typically Python or shell.

---

## Primary Tasks for AI Assistance

Claude is used in this repo primarily for:

1. **WDL task and workflow development** — drafting new reusable tasks or workflows,
   or reviewing existing ones for correctness, style, and portability.

2. **Docker image maintenance** — suggesting updates to Dockerfiles (e.g., dependency
   version bumps, base image changes), to be reviewed and rebuilt by the developer.

3. **Utility script development** — drafting or extending scripts in `util/` that
   support workflow tasks.

4. **Cross-repo consistency** — ensuring that shared WDL tasks and utility scripts
   remain compatible with the downstream workflow repos that import them.

---

## Important Constraints and Pitfalls

- **This repo is a shared dependency.** WDL tasks, utility scripts, and Docker images
  here are imported or referenced by multiple downstream workflow repos. Changes can
  have broad impact — always assess downstream effects before suggesting modifications.
- **Docker image versions must be pinned.** WDL `runtime` blocks should reference
  specific image tags (not `latest`) to guarantee reproducibility across runs.
- **WDL imports are path- or URI-based.** When suggesting changes to task or workflow
  file names or locations, account for the fact that downstream repos may import these
  by path or URI and will need to be updated accordingly.
- **Do not suggest changes that break existing task interfaces** (input/output names
  and types) without explicitly flagging the breaking change and listing affected callers.

---

## Pull Requests

When suggesting changes, follow the standard fork-and-PR model and ensure that any
proposed modifications are accompanied by a description of downstream impact.
