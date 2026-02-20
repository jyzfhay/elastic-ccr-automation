#!/usr/bin/env python3
"""
Promote CCR follower indices to standalone leaders (pause follow, unfollow, open, allow writes).
Run: python ccr-cutover.py [config.json]
"""
import argparse
import json
import logging
import sys
import time
from elasticsearch import Elasticsearch, exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("ccr_cutover")

PROMOTE_RETRIES = 3
PROMOTE_DELAY = 5


def load_config(path: str) -> dict:
    """Load JSON config. Exits on error."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.error("Failed to load config %s: %s", path, e)
        sys.exit(1)


def get_follower_indices(es: Elasticsearch) -> list:
    """Return list of follower index names from CCR stats."""
    out = []
    try:
        resp = es.ccr.stats()
        body = resp.body if hasattr(resp, "body") else resp
        for follow_stats in body.get("follow_stats", {}).get("indices", []):
            for shard in follow_stats.get("shards", []):
                idx = shard.get("follower_index")
                if idx and idx not in out:
                    out.append(idx)
    except exceptions.ConnectionError as e:
        logger.error("Connection error: %s", e)
    except exceptions.NotFoundError as e:
        logger.error("Not found: %s", e)
    except (KeyError, Exception) as e:
        logger.error("Error getting follower indices: %s", e)
    return out


def validate_follower_indices(es: Elasticsearch, follower_indices: list) -> tuple:
    """
    Check which followers are caught up with their leader.
    Returns (caught_up_list, not_caught_up_list).
    """
    caught_up = []
    not_caught_up = []
    try:
        for index in follower_indices:
            resp = es.ccr.follow_info(index=index)
            body = resp.body if hasattr(resp, "body") else resp
            for follower in body.get("follower_indices", []):
                fidx = follower.get("follower_index")
                if not fidx:
                    continue
                all_ok = True
                for shard in follower.get("shards", []):
                    if shard.get("leader_global_checkpoint") != shard.get("follower_global_checkpoint"):
                        all_ok = False
                        break
                if all_ok:
                    caught_up.append(fidx)
                else:
                    not_caught_up.append(fidx)
    except (exceptions.ConnectionError, exceptions.NotFoundError, KeyError, Exception) as e:
        logger.error("Error validating followers: %s", e)
    return caught_up, not_caught_up


def promote_follower(es: Elasticsearch, index: str, dry_run: bool) -> bool:
    """Pause follow, close, unfollow, open, re-apply aliases, allow writes. Returns True on success."""
    if dry_run:
        logger.info("[DRY RUN] Would promote index: %s", index)
        return True

    for attempt in range(PROMOTE_RETRIES):
        try:
            es.ccr.pause_follow(index=index)
            logger.info("Paused CCR for %s", index)

            alias_resp = es.indices.get_alias(index=index)
            alias_body = alias_resp.body if hasattr(alias_resp, "body") else alias_resp
            alias_data = alias_body.get(index, {}).get("aliases", {})

            es.indices.close(index=index)
            logger.info("Closed index %s", index)

            es.ccr.unfollow(index=index)
            logger.info("Unfollowed %s", index)

            es.indices.open(index=index)
            logger.info("Opened index %s", index)

            for alias, meta in alias_data.items():
                es.indices.put_alias(index=index, name=alias, body=meta or {})
                logger.info("Reapplied alias %s for %s", alias, index)

            es.indices.put_settings(index=index, body={"index.blocks.write": False})
            logger.info("Allowed writes for %s", index)
            return True

        except (exceptions.ConnectionError, exceptions.NotFoundError, exceptions.RequestError) as e:
            logger.error("Attempt %d for %s: %s", attempt + 1, index, e)
        except Exception as e:
            logger.error("Attempt %d for %s: %s", attempt + 1, index, e)
        if attempt < PROMOTE_RETRIES - 1:
            time.sleep(PROMOTE_DELAY)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Promote CCR follower indices to leaders (cutover).",
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
        help="Only log what would be done",
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip confirmation prompts (use in automation)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    dry_run = args.dry_run or config.get("dry_run", False)
    es_url = config.get("es_src_url")
    api_key = config.get("api_key")
    if not es_url or not api_key:
        logger.error("Config must include es_src_url and api_key.")
        sys.exit(1)

    es = Elasticsearch([es_url], api_key=api_key)

    if dry_run:
        logger.info("Running in DRY_RUN mode")

    follower_indices = get_follower_indices(es)
    if not follower_indices:
        logger.info("No follower indices found. Exiting.")
        return

    logger.info("Follower indices: %s", follower_indices)

    caught_up, not_caught_up = validate_follower_indices(es, follower_indices)

    if not caught_up and not not_caught_up:
        logger.info("No follower index info from follow_info. Exiting.")
        return

    if not_caught_up:
        logger.warning("Not caught up (will not be promoted): %s", not_caught_up)
    if caught_up:
        logger.info("Caught up (will be promoted): %s", caught_up)

    if not caught_up:
        logger.info("No indices are caught up. Nothing to promote. Exiting.")
        return

    if not args.yes:
        if not_caught_up:
            msg = (
                f"Only {len(caught_up)} index/indices are caught up; {len(not_caught_up)} are not. "
                "Promote only the caught-up ones? (yes/no): "
            )
        else:
            msg = "Proceed with promotion? (yes/no): "
        try:
            reply = input(msg).strip().lower()
        except EOFError:
            reply = "no"
        if reply != "yes":
            logger.info("Promotion cancelled.")
            return

        if not dry_run:
            reply2 = input("Final confirmation — proceed with promotion? (yes/no): ").strip().lower()
            if reply2 != "yes":
                logger.info("Promotion cancelled.")
                return

    failed = []
    for index in caught_up:
        if not promote_follower(es, index, dry_run):
            failed.append(index)
            logger.error("Failed to promote %s after %d attempts", index, PROMOTE_RETRIES)

    logger.info("Promoted %d index/indices.", len(caught_up) - len(failed))
    if failed:
        logger.error("Failed: %s", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
