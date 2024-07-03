# Elastic Cloud Follower Index Promotion Script

This script automates the process of promoting follower indices to leader indices in your Elastic Cloud deployment.

## Prerequisites

1.  **Elastic Cloud Cluster:** You need an active Elastic Cloud cluster with follower indices that you want to promote.
2.  **API Key:**  Create an API key in your Elastic Cloud console with appropriate permissions to manage indices and CCR.
3.  **config.json:** Use the `config.json` file in the same directory as this script with the following structure:

    ```json
    {
        "cloud_id": "your_cloud_id",
        "api_key": "your_api_key"
    }
    ```


## Usage

1.  Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

2.  Update `config.json` with your Elastic Cloud credentials.
3.  Run the script from your terminal:

    ```bash
    python promote_followers.py
    ```
    

The script will list the follower indices it finds and ask for your confirmation before proceeding. If you confirm, it will promote each follower index to a leader, pausing replication, closing the index, unfollowing the leader, opening it back up, and enabling writes.
