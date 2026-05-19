# `compute/hec-s3/` — Running against HEC S3

The plugin runs against any S3-compatible backend; HEC S3 is the production
target. This directory holds the auth scaffolding; the runtime invocation
itself is via `./run.py` at the repo root.

## One-time setup

```bash
cp compute/hec-s3/.env.example compute/hec-s3/.env   # .env is gitignored
$EDITOR compute/hec-s3/.env                          # fill in real creds
```

`./run.py` auto-loads `compute/hec-s3/.env` whenever you invoke `hec`, `batch`, or
`mirror` — no need to `source` it yourself. Any value already set in your
shell environment wins, so you can override individual keys without editing
the file.

## Run a single job

```bash
./run.py hec                 # list available payloads, pick interactively
./run.py hec list            # just list — UUID<TAB>timestamp, machine-readable
./run.py hec <PAYLOAD_UUID>  # run a specific one
```

`./run.py hec` (no args) calls `s3api list-objects-v2` against
`s3://$CC_AWS_S3_BUCKET/$CC_ROOT/`, sorts payloads by most recent first, and
prompts you to pick. It then skips compose (no MinIO) and `docker run`s the
image directly with the env vars from your `.env`. Progress JSON for each run
lands in `compute/outputs/<UUID>/progress.json` (or `<NAME>/` if you pass one).

## Run many jobs (batch)

Drop one subdir per job under any path you like, each containing a
`compute-manifest.json`:

```
jobs/
  indian-creek-72hr/
    compute-manifest.json    # has "uuid": "1a63..."
  kanawha-1000-120hr/
    compute-manifest.json
```

Then:

```bash
./run.py batch jobs/
```

`./run.py batch` stages each manifest to `s3://$CC_AWS_S3_BUCKET/manifests/<uuid>/payload`
and runs the plugin once per job, sequentially.
