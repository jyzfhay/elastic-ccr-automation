# CCR Bootstrap — Create Follow Tasks from Leader to Follower

This script creates CCR follow tasks on the follower cluster for every open leader index that is not already being followed.

## Prerequisites

- Leader and follower clusters with a **remote cluster** already configured on the follower (pointing at the leader).
- API keys for both clusters. Follower key needs CCR follow and index management.

## Config

Copy the example and fill in URLs and API keys:

```bash
cp config.json.example config.json
```

`config.json`:

```json
{
  "leader": {
    "dep_name": "leader-cluster",
    "elastic_url": "https://leader.example.com",
    "elastic_api_key": "leader-api-key"
  },
  "follower": {
    "dep_name": "follower-cluster",
    "elastic_url": "https://follower.example.com",
    "elastic_api_key": "follower-api-key"
  },
  "dry_run": false,
  "rc_name": "my-remote-cluster"
}
```

- **leader.elastic_url** / **follower.elastic_url**: Cluster URLs.
- **leader.elastic_api_key** / **follower.elastic_api_key**: API keys for each cluster.
- **rc_name**: Name of the remote cluster as configured on the **follower** (used in the follow API).
- **dry_run**: If `true`, no follow requests are sent.

## Usage

```bash
pip install -r requirements.txt
python ccr_bootstrap.py [config.json]
```

Options:

- **config path**: Optional; default is `config.json`.
- **--dry-run**: Log which indices would be followed without sending requests.

Workflow:

1. Load config and resolve `rc_name`.
2. Get open leader indices from the leader cluster (`_cat/indices`).
3. Get existing follower indices from the follower cluster (`*/_ccr/info`).
4. For each leader index not already followed, send a PUT follow request to the follower (with retries and concurrency).

Exit code 1 if any follow request failed.
