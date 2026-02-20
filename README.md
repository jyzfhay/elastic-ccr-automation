# Elasticsearch Cross-Cluster Replication (CCR) Automation

Python scripts to automate CCR setup and cutover for Elasticsearch:

| Script | Purpose |
|--------|--------|
| **ccr_bootstrap** | Create follow tasks on the follower cluster for leader indices that are not yet followed. |
| **ccr-cutover** | Promote follower indices to standalone leaders (pause follow → unfollow → open → allow writes). |

## Features

- **Unified config**: Single JSON file per script for cluster URLs and API keys.
- **Safety**: `--dry-run` and optional confirmations; cutover supports `--yes` for automation.
- **Robustness**: Retries and backoff for API calls; concurrent bootstrap requests.
- **Security**: Keep credentials in `config.json` and add it to `.gitignore` (use `config.json.example` as a template).

## Requirements

- Python 3.6+
- **ccr_bootstrap**: `requests`
- **ccr-cutover**: `elasticsearch` (official Elasticsearch Python client)

## Installation

Each script has its own directory and requirements:

```bash
# Bootstrap (create follow tasks)
cd ccr_bootstrap
pip install -r requirements.txt

# Cutover (promote followers to leaders)
cd ccr-cutover
pip install -r requirements.txt
```

## Usage

### 1. CCR Bootstrap

Creates CCR follow tasks for every open leader index that is not yet followed.

```bash
cd ccr_bootstrap
cp config.json.example config.json
# Edit config.json: leader/follower URLs and API keys, rc_name (remote cluster name on follower)

python ccr_bootstrap.py config.json
# Or with defaults (config.json in current dir):
python ccr_bootstrap.py

# Dry run (no API writes):
python ccr_bootstrap.py --dry-run
```

**Config:** `config.json` must include `leader`, `follower`, and `rc_name`. See `ccr_bootstrap/readme.md` and `config.json.example`.

### 2. CCR Cutover

Promotes follower indices to leaders (pause follow, close, unfollow, open, re-apply aliases, allow writes). Only promotes indices that are **caught up** with the leader.

```bash
cd ccr-cutover
cp config.json.example config.json
# Edit config.json: es_src_url (follower cluster URL), api_key

python ccr-cutover.py config.json
# Or:
python ccr-cutover.py

# Dry run:
python ccr-cutover.py --dry-run

# Non-interactive (e.g. CI):
python ccr-cutover.py --yes
```

**Config:** `config.json` must include `es_src_url` (follower cluster) and `api_key`. See `ccr-cutover/readme.md` and `config.json.example`.

## License

See [LICENSE](LICENSE).
