# Elasticsearch Cross-Cluster Replication Setup

This Python script automates the setup of cross-cluster replication (CCR) for Elasticsearch, handling the configuration of leader and follower clusters through a unified JSON file. It includes robust error handling, logging, and concurrency management to optimize the replication setup process.

## Features

- **Unified Configuration**: Utilizes a single JSON file to manage configurations for both leader and follower clusters.
- **Error Handling**: Enhanced error logging and retries for robustness.
- **Concurrency**: Uses `ThreadPoolExecutor` for concurrent API requests to speed up the replication setup.
- **Security**: Encourages secure handling of sensitive information like API keys.

## Requirements

- Python 3.6 or higher
- `requests` library

## Installation

To get started, clone this repository and install the required Python packages:


git clone "url"
cd your-repository-directory
pip install -r requirements.txt

## Variables
### leader:
- dep_name: Deployment name of the leader cluster.
- elastic_url: URL of the leader cluster.
- melastic_api_key: API key for authenticating with the leader cluster.

### follower:
- dep_name: Deployment name of the follower cluster.
- elastic_url: URL of the follower cluster.
- elastic_api_key: API key for authenticating with the follower cluster.
- dry_run: Boolean flag to indicate whether the script should run in dry run mode. Set to true to simulate the process without making - any changes, or false to perform the actual replication setup.
- rc_name: Name of the remote cluster as configured in the follower cluster. This is used to reference the leader cluster in the CCR setup.

## Running the Script
To run the script, use the following command, replacing config.json with the path to your configuration file:


## Script Explanation
### Functions
- load_config(file_path): Loads the configuration from the specified JSON file.
- get_followers_list(): Retrieves the list of follower indices from the follower cluster.
- get_leaders_list(): Retrieves the list of open indices from the leader cluster.
- put_req(reqs): Sends a PUT request to initiate CCR for the specified index.

### Workflow
- Load the configuration from the specified JSON file.
- Retrieve the list of indices from the leader cluster.
- Retrieve the list of follower indices from the follower cluster.
- Identify indices that need to be bootstrapped for CCR.
- If dry_run is False, initiate CCR for each identified index using a thread pool for concurrent execution.
