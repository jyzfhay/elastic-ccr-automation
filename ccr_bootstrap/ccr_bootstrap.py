#!/usr/bin/env python3
"""
Bootstrap CCR (cross-cluster replication) from leader to follower.
Run: python ccr_bootstrap.py <config.json>
"""
import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("ccr_bootstrap")

DEFAULT_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 1
DEFAULT_MAX_WORKERS = 10


def load_config(file_path: str) -> dict:
    """Load JSON config. Exits on error."""
    try:
        with open(file_path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.error("Failed to load config %s: %s", file_path, e)
        sys.exit(1)


def get_followers_list(follower_config: dict) -> list:
    """Return list of follower index names from the follower cluster."""
    url = f"{follower_config['elastic_url'].rstrip('/')}/*/_ccr/info"
    headers = {
        "Authorization": f"ApiKey {follower_config['elastic_api_key']}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        logger.error("Request to follower cluster failed: %s", e)
        return []
    if resp.status_code != 200:
        logger.error("Failed to get follower CCR info: %s", resp.text)
        return []
    data = resp.json()
    return [x["follower_index"] for x in data.get("follower_indices", [])]


def get_leaders_list(leader_config: dict, retries: int = DEFAULT_RETRIES) -> list:
    """Return list of open leader index names. Retries with backoff."""
    url = f"{leader_config['elastic_url'].rstrip('/')}/_cat/indices/*,-.*?v&s=index&format=json"
    headers = {
        "Authorization": f"ApiKey {leader_config['elastic_api_key']}",
        "Content-Type": "application/json",
    }
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            logger.warning("Attempt %d: request failed: %s", attempt + 1, e)
            time.sleep(DEFAULT_BACKOFF_FACTOR * (2 ** attempt))
            continue
        if resp.status_code == 200:
            return [x["index"] for x in resp.json() if x.get("status") == "open"]
        logger.warning("Attempt %d: status %s - %s", attempt + 1, resp.status_code, resp.text)
        time.sleep(DEFAULT_BACKOFF_FACTOR * (2 ** attempt))
    logger.error("Failed to get leader index list after %d attempts", retries)
    return []


def put_follow_request(
    follower_config: dict,
    rc_name: str,
    index: str,
    retries: int = DEFAULT_RETRIES,
) -> tuple:
    """
    Send PUT request to start following a leader index.
    Returns (index_name, success).
    """
    url = f"{follower_config['elastic_url'].rstrip('/')}/{index}/_ccr/follow"
    headers = {
        "Authorization": f"ApiKey {follower_config['elastic_api_key']}",
        "Content-Type": "application/json",
    }
    body = {"remote_cluster": rc_name, "leader_index": index}
    for attempt in range(retries):
        try:
            resp = requests.put(
                url,
                data=json.dumps(body),
                headers=headers,
                timeout=60,
            )
        except requests.RequestException as e:
            logger.warning("Attempt %d for %s: %s", attempt + 1, index, e)
            time.sleep(DEFAULT_BACKOFF_FACTOR * (2 ** attempt))
            continue
        if resp.status_code == 200:
            return index, True
        logger.warning("Attempt %d for %s: %s - %s", attempt + 1, index, resp.status_code, resp.text)
        time.sleep(DEFAULT_BACKOFF_FACTOR * (2 ** attempt))
    return index, False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bootstrap CCR: create follow tasks for leader indices not yet followed.",
    )
    parser.add_argument(
        "config",
        nargs="?",
        default="config.json",
        help="Path to config JSON (default: config.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be done without sending follow requests",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    leader_config = config["leader"]
    follower_config = config["follower"]
    dry_run = args.dry_run or config.get("dry_run", False)
    rc_name = config.get("rc_name")
    if not rc_name:
        logger.error("Config must include 'rc_name' (remote cluster name on follower).")
        sys.exit(1)

    if dry_run:
        logger.info("Running in DRY_RUN mode")

    leaders_list = get_leaders_list(leader_config)
    existing_followers = get_followers_list(follower_config)
    to_bootstrap = [x for x in leaders_list if x not in existing_followers]

    logger.info("Leader indices: %d | Existing followers: %d | To bootstrap: %d",
                len(leaders_list), len(existing_followers), len(to_bootstrap))

    if not to_bootstrap:
        logger.info("Nothing to bootstrap. Exiting.")
        return

    if dry_run:
        logger.info("[DRY RUN] Would create follow for: %s", to_bootstrap)
        return

    success_list = []
    failed_list = []

    def task(idx: str):
        return put_follow_request(follower_config, rc_name, idx)

    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        results = list(executor.map(lambda idx: task(idx), to_bootstrap))

    for index, ok in results:
        if ok:
            success_list.append(index)
        else:
            failed_list.append(index)

    logger.info("Success (%d): %s", len(success_list), success_list)
    logger.info("Failed (%d): %s", len(failed_list), failed_list)
    if failed_list:
        sys.exit(1)


if __name__ == "__main__":
    main()
