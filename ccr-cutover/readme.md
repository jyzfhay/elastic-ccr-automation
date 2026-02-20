# CCR Cutover — Promote Follower Indices to Leaders

This script promotes CCR follower indices to standalone leader indices: it pauses follow, closes the index, unfollows the leader, reopens the index, re-applies aliases, and allows writes.

## Prerequisites

- Follower cluster with indices that are following a leader via CCR.
- API key with permissions for: CCR (pause, unfollow), index management (close, open, aliases, settings).

## Config

Copy the example and set your follower cluster URL and API key:

```bash
cp config.json.example config.json
```

`config.json`:

```json
{
  "es_src_url": "https://your-follower-cluster.es.us-central1.gcp.cloud.es.io:9243",
  "api_key": "your-api-key",
  "dry_run": false
}
```

- **es_src_url**: Follower cluster URL (the cluster whose follower indices you are promoting).
- **api_key**: API key with CCR and index permissions.
- **dry_run**: If `true`, no changes are made; the script only logs what it would do.

## Usage

```bash
pip install -r requirements.txt
python ccr-cutover.py [config.json]
```

Options:

- **config path**: Optional; default is `config.json` in the current directory.
- **--dry-run**: Simulate only; no API calls that change state.
- **--yes / -y**: Skip confirmation prompts (for automation).

The script:

1. Lists follower indices from CCR stats.
2. Checks which are caught up (leader vs follower global checkpoint).
3. Asks for confirmation (unless `--yes`).
4. For each **caught-up** index: pause follow → close → unfollow → open → re-apply aliases → allow writes.

Indices that are **not** caught up are listed but never promoted.
