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
        ccr_stats_dict = ccr_stats.raw
        logging.info(f"CCR Stats: {json.dumps(ccr_stats_dict, indent=2)}")  # Print the full CCR stats response

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

# Function to promote a follower index to a leader index
def promote_follower(index, retries=3, delay=5):
    if dry_run:
        logging.info(f"[DRY RUN] Would promote index {index} to leader")
        return True  # Simulate success in dry run
    else:
        for attempt in range(retries):
            try:
                # Pause CCR
                es.ccr.pause_follow(index=index)
                logging.info(f"Paused CCR for index {index}")

                # Close the index
                es.indices.close(index=index)
                logging.info(f"Closed index {index}")

                # Unfollow the leader
                es.ccr.unfollow(index=index)
                logging.info(f"Unfollowed index {index}")

                # Open the index
                es.indices.open(index=index)
                logging.info(f"Opened index {index}")

                # Allow writes
                es.indices.put_settings(index=index, body={"index.blocks.write": False})
                logging.info(f"Allowed writes for index {index}")

                return True
            except exceptions.ConnectionError as e:
                logging.error(f"Connection error: {e}")
            except exceptions.NotFoundError as e:
                logging.error(f"Not found error: {e}")
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

    # Always log the follower indices
    logging.info(f"Follower indices found: {follower_indices}")

    if not dry_run:
        confirmation = input("Proceed with promotion (yes/no)? ").lower()
        if confirmation != "yes":
            logging.info("Promotion cancelled.")
            exit()

    # Promote each follower index (only if not dry run)
    for index in follower_indices:
        if not promote_follower(index):
            logging.error(f"Failed to promote index {index} after multiple attempts.")

    # Log the total number of follower indices at the end
    total_follower_indices = len(follower_indices)
    logging.info(f"Total number of follower indices found: {total_follower_indices}")
    