# PolShiarb Agent Journal

Use this file to keep a running record of what we learn about the repo and exchange behavior.

## Format

- `YYYY-MM-DD HH:MM TZ`
- **Goal:** What you are trying to do
- **What changed:** Files, commands, observations
- **Why:** Decision rationale
- **Open question / risk:** Anything uncertain or unresolved

## Entries

- 2026-03-03 00:00:00 (repo setup)
  - Journal initialized for ongoing repo understanding and build decisions.
  - This file should be appended to as work progresses.
- 2026-03-03 18:02:08 EST (workflow documentation)
  - Added `README.md` to capture current repo state, PMXT setup, and safety constraints.
  - Captured known working PMXT local startup path via bundled server script and journal workflow.
  - Open risk: unify runtime entrypoint (`pmxt-server` binary vs bundled script) and avoid global install pitfalls for Polymarket CJS/ESM.
- 2026-03-03 18:14 EST (repo path correction)
  - Moved the Python API reference integration to the intended project directory: `code_repos/sampatt/polshiarb`.
  - Added `PMXT_PYTHON_API_REFERENCE.md` there, copied from upstream PMXT Python docs.
  - Updated `code_repos/sampatt/polshiarb/README.md` to point to that reference file.
  - Open risk: keep both copies of repo docs in sync if work later continues in `/home/nondescript/sampatt/polshiarb` path (non-existent now).
