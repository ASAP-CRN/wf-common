# wf-common
Workflows, tasks, utility scripts, and docker images reused across harmonized ASAP workflows. Workflows are written in [Workflow Description Language (WDL)](https://openwdl.org/).

- Common tasks that may be reused within or between workflows are defined in [the wdl directory](wdl).
- Common utility scripts that may be reused are defined in [the util directory](util).
- Common docker images used in workflows are defined in [the docker directory](docker).

## Python requirements

- `pyproject.toml` — declares the minimum Python version (`>=3.10`). Required by the union type syntax (`X | Y`) used in utility scripts.
- `requirements.txt` — lists third-party pip dependencies for the utility scripts.

Install dependencies with:
```bash
pip install -r requirements.txt
```
