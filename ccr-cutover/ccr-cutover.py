import json
import logging
import time
from elasticsearch import Elasticsearch, exceptions

# Load configuration from config.json
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

es_src_url = config['es_src_url']
api_key = config['api_key']
dry_run = config.get('dry_run', False)

es = Elasticsearch([es_src_url], api_key=api_key)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to identify follower indices
def get_follower_indices():
    follower_indices = []
    try:
        ccr_stats = es.ccr.stats()
        ccr_stats_dict = ccr_stats.body 
        logging.info(f"CCR Stats: {json.dumps(ccr_stats_dict, indent=2)}") 

        # Extract follower indices from the CCR stats
        for follow_stats in ccr_stats_dict.get('follow_stats', {}).get('indices', []):
            for shard in follow_stats.get('shards', []):
                follower_index = shard.get('follower_index')
                if follower_index and follower_index not in follower_indices:
                    follower_indices.append(follower_index)
    except exceptions.ConnectionError as e:
        logging.error(f"Connection error: {e}")
    except exceptions.NotFoundError as e:
        logging.error(f"Not found error: {e}")
    except KeyError as e:
        logging.error(f"Key error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    return follower_indices

# Function to check if follower indices are caught up with leader indices
def validate_follower_indices(follower_indices):
    caught_up_indices = []
    not_caught_up_indices = []
    try:
        for index in follower_indices:
            stats = es.ccr.follow_info(index=index)
            stats_dict = stats.body  # Use .body to get the JSON response as a dictionary
            logging.info(f"Follow info for {index}: {json.dumps(stats_dict, indent=2)}") 
            follower_indices_info = stats_dict.get('follower_indices', [])
            for follower in follower_indices_info:
                follower_index = follower.get('follower_index')
                if not follower_index:
                    logging.error(f"No 'follower_index' found in {follower}")
                    continue
                all_shards_caught_up = True
                for shard in follower.get('shards', []):
                    if shard.get('leader_global_checkpoint') != shard.get('follower_global_checkpoint'):
                        all_shards_caught_up = False
                        not_caught_up_indices.append(follower_index)
                        break
                if all_shards_caught_up:
                    caught_up_indices.append(follower_index)
    except exceptions.ConnectionError as e:
        logging.error(f"Connection error: {e}")
    except exceptions.NotFoundError as e:
        logging.error(f"Not found error: {e}")
    except KeyError as e:
        logging.error(f"Key error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    return caught_up_indices, not_caught_up_indices

# Function to promote a follower index to a leader index
def promote_follower(index, retries=3, delay=5):
    if dry_run:
        logging.info(f"[DRY RUN] Would promote index {index} to leader")
        return True  
    else:
        for attempt in range(retries):
            try:
                # Pause CCR
                es.ccr.pause_follow(index=index)
                logging.info(f"Paused CCR for index {index}")

                # Get alias information
                alias_info = es.indices.get_alias(index=index)
                alias_info_dict = alias_info.body  # Use .body to get the JSON response as a dictionary
                logging.info(f"Alias info for index {index}: {json.dumps(alias_info_dict, indent=2)}")

                # Close the index
                es.indices.close(index=index)
                logging.info(f"Closed index {index}")

                # Unfollow the leader
                es.ccr.unfollow(index=index)
                logging.info(f"Unfollowed index {index}")

                # Open the index
                es.indices.open(index=index)
                logging.info(f"Opened index {index}")

                # Reapply alias information
                for alias, alias_data in alias_info_dict.get(index, {}).get('aliases', {}).items():
                    es.indices.put_alias(index=index, name=alias, body=alias_data)
                    logging.info(f"Reapplied alias {alias} for index {index} with data {alias_data}")

                # Allow writes
                es.indices.put_settings(index=index, body={"index.blocks.write": False})
                logging.info(f"Allowed writes for index {index}")

                return True
            except exceptions.ConnectionError as e:
                logging.error(f"Connection error: {e}")
            except exceptions.NotFoundError as e:
                logging.error(f"Not found error: {e}")
            except exceptions.RequestError as e:
                logging.error(f"Request error: {e}")
            except Exception as e:
                logging.error(f"Attempt {attempt + 1}/{retries}: Error promoting index {index}: {e}")
                if attempt < retries - 1: 
                    time.sleep(delay)
        return False

if __name__ == "__main__":
    follower_indices = get_follower_indices()
    
    if not follower_indices:
        logging.info("No follower indices found. Exiting.")
        exit()

    logging.info(f"Follower indices found: {follower_indices}")

    caught_up_indices, not_caught_up_indices = validate_follower_indices(follower_indices)

    if not caught_up_indices and not not_caught_up_indices:
        logging.info("No follower indices found. Exiting.")
        exit()

    if not_caught_up_indices:
        logging.info(f"Indices not caught up: {not_caught_up_indices}")
        if dry_run:
            logging.info("[DRY RUN] Would prompt for proceeding despite some indices not being caught up.")
        else:
            proceed = input("Follower indices are not caught up yet. Do you still wish to proceed? (yes/no): ").lower()
            if proceed != "yes":
                logging.info("Promotion cancelled due to indices not being caught up.")
                exit()
    else:
        logging.info("All follower indices are caught up.")
        if dry_run:
            logging.info("[DRY RUN] Would prompt for proceeding with promotion.")
        else:
            proceed = input("Do you want to proceed with promotion? (yes/no): ").lower()
            if proceed != "yes":
                logging.info("Promotion cancelled.")
                exit()

    logging.info(f"Total number of follower indices found: {len(follower_indices)}")
    logging.info(f"Follower indices to be promoted:\n{json.dumps(follower_indices, indent=2)}")
    
    if not dry_run:
        confirmation = input("Proceed with promotion (yes/no)? ").lower()
        if confirmation != "yes":
            logging.info("Promotion cancelled.")
            exit()

    # Promote each follower index (only if not dry run)
    for index in caught_up_indices:
        if not promote_follower(index):
            logging.error(f"Failed to promote index {index} after multiple attempts.")
    
    logging.info(f"Total number of follower indices promoted: {len(caught_up_indices)}")
