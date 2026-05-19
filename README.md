# StormHub Cloud Plugin

A [USACE Cloud Compute](https://github.com/USACE-Cloud-Compute/cloudcompute) plugin that creates storm catalogs from NOAA AORC precipitation data and converts them to HEC-DSS files.

```
S3 payload  -->  download-inputs  -->  process-storms  -->  convert-to-dss  -->  create-grid-file  -->  upload-outputs
```

## Quick Start

Requires **Python 3** and **Docker**.

```bash
./run.py        # Linux/macOS — builds the image, starts MinIO, runs the plugin
python run.py   # Windows / portable invocation
```

Progress streams to the terminal (`[progress]` lines per action). MinIO console
is at http://localhost:9001 (`ccuser`/`ccpassword`); output files land in
`compute/outputs/quick-test/` on the host.

> Local runs serialize storm-search by default (1 worker) because no container
> memory limit is enforced. For a faster loop, set `CC_NUM_WORKERS=4` in
> `compute/dev.env` or pass `num_workers` in the payload `attributes`.

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
compute/                  # everything run.py needs — flat by intent
  Dockerfile              #   image build (local + prod)
  requirements.txt
  constraints.txt
  compose.yaml            #   local MinIO dev stack
  dev.env                 #   fake MinIO creds (committed)
  hec.env.example         #   prod creds template — cp to hec.env (gitignored)
  cc-manifest.example.json#   CC orchestrator deployment spec (reference)
  sample/                 #   canonical local-run inputs
    payload.json
    watershed-boundary.geojson
    transposition-domain.geojson
  outputs/                #   gitignored runtime — DSS files, progress.json, logs
stormhub/                 # forked upstream library (git submodule)
```

## `run.py` Commands

```bash
./run.py                  # Local run with compute/sample/payload.json
./run.py PAYLOAD          # Local run with a custom payload
./run.py hec              # List HEC S3 payloads and pick one interactively
./run.py hec UUID         # Run a specific HEC S3 payload by UUID
./run.py batch [DIR]      # Multi-job HEC S3 driver (one subdir per job)
./run.py build            # docker build the plugin image
./run.py mirror [args]    # One-shot AORC zarr mirror (NOAA -> private S3)
./run.py web [--port N]   # Browser UI: browse payloads, launch runs, watch progress
./run.py lint             # ruff check + format check
./run.py format           # ruff format
./run.py test [args...]   # pytest plugin/tests/ (extra args forward to pytest)
./run.py freeze           # Regenerate compute/constraints.txt
./run.py down             # docker compose down
./run.py clean            # Stop containers, drop volumes, clear compute/outputs/
```

`run.py` is a single Python file at the repo root. Only required dependency
is Python 3 (Docker for build/run subcommands; `s3fs` + `xarray` for `./run.py
mirror`, lazy-imported only when invoked).

**Cross-platform invocation:**
- Linux / macOS: `./run.py <cmd>` (shebang + execute bit)
- Windows cmd / PowerShell: `python run.py <cmd>`
- Everywhere portable: `python run.py <cmd>`

## Custom Payloads

Edit `compute/sample/payload.json` or copy it and pass the path:

```bash
cp compute/sample/payload.json compute/sample/mine.json
./run.py compute/sample/mine.json
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
| `CC_NUM_WORKERS` | unset | Fleet default for `process-storms` worker count. Overridden by payload `num_workers` attr. |
| `DSS_WORKERS` | `0` (= cpu_count) | Worker count for `convert-to-dss` process pool. |
| `DSS_MAX_FAILURE_RATIO` | `0.5` | `convert-to-dss` hard-fails above this fraction. |
| `GRID_MAX_FAILURE_RATIO` | `0.5` | `create-grid-file` hard-fails above this fraction. |

## Running Against HEC S3

Same plugin code, different backend. One-time setup:

```bash
cp compute/hec.env.example compute/hec.env   # gitignored
$EDITOR compute/hec.env                      # fill in real creds
```

`run.py` auto-loads `compute/hec.env` for `hec`, `batch`, and `mirror` — no
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

Lists payloads in HEC S3 (if `compute/hec.env` is configured), launches
local or HEC runs with one click, and shows each run's current step + elapsed
time. Reads from `compute/outputs/<name>/progress.json` (auto-refreshes every
2 s). Launches detach to the background, so closing the browser — or the web
process — doesn't kill the run; logs land in
`compute/outputs/<name>/launch.log`.

Single file (`web.py`), stdlib only, binds to `127.0.0.1`. No auth.

## Reproducing the OOM Failure Mode

The vendored stormhub library would spawn `os.cpu_count() - 2` workers, which
inside a container reads the *host* CPU count and can exceed the container's
memory ceiling. To reproduce under a 3 GB cap:

```bash
./run.py build
docker compose -f compute/compose.yaml run --rm seed
docker compose -f compute/compose.yaml run --rm --memory=3g --memory-swap=3g storm-cloud-plugin
```

With the fix, the resolver reads the cgroup limit and picks a safe worker
count; without it, the library would pick 6 and `BrokenProcessPool`.

**Re-run this repro after bumping the `stormhub` submodule** — it's the
regression test for both the worker-count heuristic and the thread-cap env
vars in the Dockerfile.

## Known Limitations

- **stormhub thread fan-out**: `num_workers` only caps the *process* pool. Each worker still appears to fan out internally (likely via dask's threaded scheduler in the AORC loader and/or BLAS threads), so peak RSS scales with the container's visible vCPU count even at `num_workers=1`. **Workaround:** in addition to setting `num_workers=1` (payload attribute or `CC_NUM_WORKERS=1`), cap the container's CPU allocation so intra-worker threads can't fan out past what the memory budget tolerates. For a 15 GB cap, `cpus: "4"` (Docker Compose `deploy.resources.limits` or `--cpus 4` on `docker run`) has held under the limit in our runs. Tighten further if OOMs reappear.
