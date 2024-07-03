# Elasticsearch Cross-Cluster Replication Setup

This repository contains Python scripts to automate the setup and promotion of Elasticsearch cross-cluster replication (CCR). The repository includes two primary scripts:

1. **CCR Bootstrap Script**: Automates the initial setup of CCR between leader and follower clusters.
2. **CCR Cutover Script**: Promotes follower indices to leader indices once they have caught up.

## Features

- **Unified Configuration**: Manage configurations for both leader and follower clusters with a single JSON file.
- **Error Handling**: Enhanced error logging and retries for robustness.
- **Concurrency**: Uses `ThreadPoolExecutor` for concurrent API requests to speed up the replication setup.
- **Security**: Secure handling of sensitive information like API keys.

## Requirements

- Python 3.6 or higher
- `requests` library for the bootstrap script
- `elasticsearch` library for the cutover script

## Installation

To get started, clone this repository and install the required Python packages:

```bash
git clone 
cd your-repository-directory
pip install -r requirements.txt
