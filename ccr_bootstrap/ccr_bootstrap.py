import json
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from sync_logger import get_logger

logger = get_logger("ccr_setup")

# Load the configuration from JSON
def load_config(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Assuming the JSON file path is passed as the first command line argument
config = load_config(sys.argv[1])

# Assign leader and follower from the loaded configuration
leader_config = config['leader']
follower_config = config['follower']
dry_run = config.get("dry_run", False)
rc_name = config.get("rc_name")

if dry_run:
    logger.info("Running in DRY_RUN mode")

def get_followers_list():
    followers_list = []
    response = requests.get(
        f"{follower_config['elastic_url']}/*/_ccr/info",
        headers={"Authorization": f"ApiKey {follower_config['elastic_api_key']}", "Content-Type": "application/json"}
    )
    if response.status_code == 200:
        followers_info = response.json()
        followers_list = [x["follower_index"] for x in followers_info["follower_indices"]]
    else:
        logger.error("Failed at getting follower's ccr info")
        logger.error(response.json())
    return followers_list

def get_leaders_list():
    leaders_list = []
    retries = 3
    backoff_factor = 1
    for attempt in range(retries):
        response = requests.get(
            f"{leader_config['elastic_url']}/_cat/indices/*,-.*?v&s=index&format=json",
            headers={"Authorization": f"ApiKey {leader_config['elastic_api_key']}", "Content-Type": "application/json"}
        )
        if response.status_code == 200:
            leaders_list = [x["index"] for x in response.json() if x["status"] == "open"]
            break
        else:
            logger.error(f"Attempt {attempt + 1}: Failed at getting leader's index list")
            logger.error(response.text)
            time.sleep(backoff_factor * (2 ** attempt))
    return leaders_list

leaders_list = get_leaders_list()
existing_followers = get_followers_list()
to_bootstrap_indices = [x for x in leaders_list if x not in existing_followers]

logger.info(f"Existing followers count: {len(existing_followers)}")
logger.info(f"Leaders count: {len(leaders_list)}")
logger.info(f"Indices to initiate CCR count: {len(to_bootstrap_indices)}")

def put_req(reqs):
    return requests.put(
        reqs["url"],
        data=json.dumps(reqs["body"]),
        headers={"Authorization": f"ApiKey {follower_config['elastic_api_key']}", "Content-Type": "application/json"}
    )

success_list = []
failed_list = []

if not dry_run:
    reqs = [{"url": f"{follower_config['elastic_url']}/{x}/_ccr/follow",
             "body": {"remote_cluster": rc_name, "leader_index": x}} for x in to_bootstrap_indices]
    with ThreadPoolExecutor(max_workers=10) as executor:
        response_list = list(executor.map(put_req, reqs))
    for response, idx in zip(response_list, to_bootstrap_indices):
        if response.status_code == 200:
            success_list.append(idx)
        else:
            failed_list.append(idx)

logger.info(f"Success list - ({len(success_list)}): {success_list}")
logger.info(f"Failed list - ({len(failed_list)}): {failed_list}")
