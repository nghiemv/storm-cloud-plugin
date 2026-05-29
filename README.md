# StormHub Cloud Plugin

A [USACE Cloud Compute](https://github.com/USACE-Cloud-Compute/cloudcompute) plugin that creates storm catalogs from NOAA AORC precipitation data and converts them to HEC-DSS files.

```
S3 payload  -->  download-inputs  -->  process-storms  -->  convert-to-dss  -->  create-grid-file  -->  upload-outputs
```

## Quick Start

Requires **Python 3** and **Docker**. Works on Linux, macOS, and Windows.

```bash
./run.py        # Linux/macOS — builds the image, starts MinIO, runs the plugin
python run.py   # Windows / portable invocation
```

All non-stdlib work (S3 ops, plugin execution, AORC mirror) runs inside the
Docker image. The host needs nothing beyond Python 3 + Docker for the core
workflows. Dev verbs (`lint`/`format`/`test`) additionally need `ruff` /
`pytest` on the host — `pip install ruff pytest` to enable them.

Progress streams to the terminal (`[progress]` lines per action). MinIO console
is at http://localhost:9001 (`ccuser`/`ccpassword`); output files land in
`compute/outputs/quick-test/` on the host.

> Local runs serialize storm-search by default (1 worker) because no container
> memory limit is enforced. For a faster loop, set `CC_NUM_WORKERS=4` in
> `compute/local/dev.env` or pass `num_workers` in the payload `attributes`.

## Repo Layout

```
run.py                    # ONE entry point — dispatches every workflow below
plugin/                   # the CC compute plugin (python -m plugin)
  __main__.py             #   dispatches actions from the payload
  actions/                #   one module per pipeline step (5 steps)
  lib.py                  #   shared plumbing: context, S3 I/O, DSS naming, validation
  progress.py             #   [progress] log lines + progress.json snapshot
  workers.py              #   cgroup-aware worker count (OOM guard)
  tests/                  #   pytest (excluded from the image by .dockerignore)
compute/                  # run.py's resources, scoped per target
  Dockerfile              #   image build (shared)
  requirements.txt        #   shared
  constraints.txt         #   shared
  local/                  #   MinIO dev stack — `./run.py`
    compose.yaml
    dev.env               #     fake creds (committed)
    sample/               #     canonical local-run inputs
      payload.json
      watershed-boundary.geojson
      transposition-domain.geojson
  hec/                    #   HEC S3 — `./run.py hec` / `./run.py batch`
    env.example           #     cp to compute/hec/env, fill in (gitignored)
  outputs/                #   gitignored runtime — DSS files, progress.json, logs
stormhub/                 # forked upstream library (git submodule)
```

## `run.py`

Single Python file at the repo root, stdlib only. Docker carries the rest:
S3 ops for `hec`/`batch` run inside the plugin image (via `plugin/cli.py`),
the AORC mirror likewise. Run `./run.py help` for the full verb list.
Categories: local/HEC runs, dev maint (`lint`/`format`/`test`/`freeze`),
housekeeping (`down`/`clean`), and the browser UI (`web`).

Dev verbs need host installs: `pip install ruff` for `lint`/`format`,
`pip install pytest` for `test`. Core workflow (`local`/`hec`/`batch`/`web`)
needs no host pip installs.

Cross-platform: `./run.py <cmd>` on Linux/macOS, `python run.py <cmd>` on
Windows or anywhere portable.

## Custom Payloads

Edit `compute/local/sample/payload.json` or copy it and pass the path:

```bash
cp compute/local/sample/payload.json compute/local/sample/mine.json
./run.py compute/local/sample/mine.json
```

Storm parameters are in `attributes`. All values are strings (CC SDK convention).

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `catalog_id` | yes | | Unique ID for this storm catalog |
| `catalog_description` | yes | | Human-readable description for STAC metadata |
| `start_date` | yes | | Start of analysis period (`YYYY-MM-DD`) |
| `end_date` | no | `start_date` | End of analysis period (`YYYY-MM-DD`) |
| `storm_duration` | no | `"72"` | Storm event duration in hours |
| `top_n_events` | no | `"10"` | Number of top storms to keep |
| `min_precip_threshold` | no | `"0.0"` | Minimum mean precipitation (mm) |
| `check_every_n_hours` | no | `"24"` | How often to sample storm start times |
| `specific_dates` | no | | JSON array of dates to force-include |
| `num_workers` | no | auto | Parallel workers for storm search. Auto-sized from container memory (cgroup). Use `CC_NUM_WORKERS` env for a fleet default. Falls back to 1 worker when no memory limit is set. |
| `input_path` | yes | | S3 path to watershed/transposition geometries |
| `output_path` | yes | | S3 path for results |

## Environment Variables

The plugin reads its config from the payload; these env vars tune runtime
behavior and are usually set in the manifest / compose env (Twelve-Factor).

| Variable | Default | Effect |
|----------|---------|--------|
| `LOG_LEVEL` | `INFO` | Root log level (`DEBUG`, `INFO`, `WARNING`, …). |
| `LOG_FORMAT` | unset | Set to `json` for one-line JSON log records. |
| `CC_NUM_WORKERS` | unset | Fleet default for `process-storms` worker count. Overridden by payload `num_workers` attr. The cumsum scan auto-caps further by bbox+snapshot memory estimate — set this if you want fewer workers than the auto-cap chose, not more. |
| `CC_CUMSUM_SCAN` | `1` (on) | Year-parallel cumsum scan in place of stormhub's per-date loop. Bit-identical results; ~3× wall-clock vs `num_workers=4` baseline. Set to `0` to opt out. |
| `CC_CUMSUM_CHUNK_HOURS` | `720` (30 days) | Time-chunk size for cumsum's streaming reads. Larger amortizes per-chunk overhead but quadratically increases the worker's chunk-load transient memory peak. |
| `CC_MEMORY_GB` | unset → 80% of host MemTotal | Per-container memory budget (`docker run --memory`). Set explicitly when running multiple containers concurrently so they don't oversubscribe host RAM. |
| `DSS_WORKERS` | `0` (= cpu_count) | Worker count for `convert-to-dss` process pool. |
| `DSS_MAX_FAILURE_RATIO` | `0.5` | `convert-to-dss` hard-fails above this fraction. |
| `GRID_MAX_FAILURE_RATIO` | `0.5` | `create-grid-file` hard-fails above this fraction. |

### Automatic safeguards

These run on every `./run.py hec` invocation, no config needed:

- **AORC pre-flight cache check.** Before `process-storms` does any real work,
  HEAD every `<year>.zarr/.zmetadata` in the AORC cache. Missing years raise
  immediately with `./run.py mirror --year-start X --year-end Y` in the message.
- **Per-worker memory cap.** `cumsum_scan` projects per-worker peak RSS from the
  transposition bbox cell count, the largest year's snapshot count, and the
  chunk-load transient (the 4 numpy arrays alive simultaneously during
  `chunk_filled = np.where(...).astype(float64)`). Workers above the host's
  safe budget are capped automatically with a warning naming the projected
  per-worker MiB and the safe max.
- **Upload-side stale-key reconcile.** After `upload-outputs` finishes, list the
  catalog's S3 prefix and delete any key not in the just-uploaded set —
  catches the rank-mismatch case where re-running a catalog with a different
  top-N selection orphans the prior run's DSS filenames. Safety guard refuses
  to delete >50% of the prefix's keys.
- **Container memory budget.** `docker run --memory` is set to 80% of host
  `MemTotal` (override via `CC_MEMORY_GB`) so a runaway worker can't SIGKILL
  other host processes via OOM.

## Running Against HEC S3

Same plugin code, different backend. One-time setup:

```bash
cp compute/hec/env.example compute/hec/env   # gitignored
$EDITOR compute/hec/env                      # fill in real creds
```

`run.py` auto-loads `compute/hec/env` for `hec`, `batch`, and `mirror` — no
manual `source`. Shell env still wins, so you can override one var ad-hoc.

**Single job:**

```bash
./run.py hec                  # list payloads in S3, pick interactively
./run.py hec list             # machine-readable: UUID<TAB>timestamp
./run.py hec <UUID> [NAME]    # run a specific payload (NAME = output subdir)
```

`./run.py hec` (no args) calls `s3api list-objects-v2` against
`s3://$CC_AWS_S3_BUCKET/$CC_ROOT/`, sorts newest-first, prompts to pick. It
then `docker run`s the image directly — no compose, no local MinIO.

**Many jobs (batch):**

Drop one subdir per job under any path, each containing a `compute-manifest.json`:

```
jobs/
  indian-creek-72hr/compute-manifest.json   # has "uuid": "1a63..."
  kanawha-1000-120hr/compute-manifest.json
```

Then `./run.py batch jobs/` stages each manifest to
`s3://$CC_AWS_S3_BUCKET/manifests/<uuid>/payload` and runs the plugin once per
job, sequentially.

## Web UI

```bash
./run.py web              # browser UI at http://localhost:8744/
```

Lists payloads in HEC S3 (if `compute/hec/env` is configured), launches
local or HEC runs with one click, and shows each run's progress + elapsed
time. Reads from `compute/outputs/<name>/progress.json` (auto-refreshes every
2 s). Launches detach to the background, so closing the browser — or the web
process — doesn't kill the run; logs land in
`compute/outputs/<name>/launch.log`.

**Progress is duration-weighted.** Each pipeline step contributes to the bar
in proportion to its real cost (learned from past runs' measured durations,
falling back to an analytic estimate), not 1/N. Since `process-storms`
typically dominates (~98% of wall time), the bar reflects that instead of
jumping a flat 20% per step. The `process-storms` sub-progress is read live
from `launch.log`'s cumsum-scan year counts, so the bar advances smoothly
through the longest step instead of freezing.

**Auditing is built in.** Each completed run has a **Details** view
(`/run/<name>` — per-step weighted breakdown + log tail) and an **Audit**
view (`/audit/<name>` — DSS/grid/STAC integrity checks, maps, charts).
Click *Download audit* to pull a catalog's artifacts from HEC S3 in the
background, then *Audit* to view the report inline.

`app.py` is a single stdlib-only JSON API + static server — launching,
monitoring, and audit (download + QA + report rendering) all live in it.
All markup lives in `static/` (`index.html`, `style.css`, `app.js`, and the
audit report template `report.html`). Binds to `127.0.0.1`. No auth.

## Publishing to Cloud Compute

When registering this plugin in CC's orchestrator catalog, submit a manifest
shaped like:

```json
{
  "name": "storm-cloud-plugin",
  "image_and_tag": "ghcr.io/usace/storm-cloud-plugin:latest",
  "description": "Creates storm catalogs from NOAA AORC precipitation data and converts to HEC-DSS files.",
  "command": ["python3.12", "-u", "-m", "plugin"],
  "compute_environment": [
    { "resource_type": "vcpu", "value": "4" },
    { "resource_type": "memory", "value": "8192" }
  ],
  "environment": {},
  "credentials": {
    "FFRD": {
      "aws_access_key_id": "",
      "aws_secret_access_key": "",
      "aws_default_region": "us-east-1",
      "aws_s3_bucket": ""
    }
  }
}
```

## Reproducing the OOM Failure Mode

The vendored stormhub library would spawn `os.cpu_count() - 2` workers, which
inside a container reads the *host* CPU count and can exceed the container's
memory ceiling. To reproduce under a 3 GB cap:

```bash
./run.py build
docker compose -f compute/local/compose.yaml run --rm seed
docker compose -f compute/local/compose.yaml run --rm --memory=3g --memory-swap=3g storm-cloud-plugin
```

With the fix, the resolver reads the cgroup limit and picks a safe worker
count; without it, the library would pick 6 and `BrokenProcessPool`.

**Re-run this repro after bumping the `stormhub` submodule** — it's the
regression test for both the worker-count heuristic and the thread-cap env
vars in the Dockerfile.

## Known Limitations

- **stormhub thread fan-out**: `num_workers` only caps the *process* pool. Each worker still appears to fan out internally (likely via dask's threaded scheduler in the AORC loader and/or BLAS threads), so peak RSS scales with the container's visible vCPU count even at `num_workers=1`. **Workaround:** in addition to setting `num_workers=1` (payload attribute or `CC_NUM_WORKERS=1`), cap the container's CPU allocation so intra-worker threads can't fan out past what the memory budget tolerates. For a 15 GB cap, `cpus: "4"` (Docker Compose `deploy.resources.limits` or `--cpus 4` on `docker run`) has held under the limit in our runs. Tighten further if OOMs reappear.
