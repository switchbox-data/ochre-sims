import subprocess
import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SOURCE_BUCKET = "oedi-data-lake"
DEST_BUCKET = "dummy-123-target"
BASE_SOURCE_PATH = "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2022/resstock_amy2018_release_1.1/timeseries_individual_buildings/by_state"
BASE_DEST_PATH = "resstock_lifeboat/resstock/2022/resstock_amy2018_release_1.1/timeseries_individual_buildings/by_state"
AWS_PROFILE = "switchbox"  # Replace with your AWS profile name

def construct_paths(state: str, upgrade: str) -> tuple:
    """Construct source and destination paths for a given state and upgrade."""
    source_path = f"s3://{SOURCE_BUCKET}/{BASE_SOURCE_PATH}/upgrade={upgrade}/state={state}/*"
    dest_path = f"s3://{DEST_BUCKET}/{BASE_DEST_PATH}/upgrade={upgrade}/state={state}/"
    return source_path, dest_path

def copy_files(state: str, upgrade: str) -> bool:
    """Copy files for a specific state and upgrade combination."""
    source_path, dest_path = construct_paths(state, upgrade)
    
    command = [
        "s5cmd",
        "-v",
        "--profile", AWS_PROFILE,
        "cp",
        source_path,
        dest_path
    ]
    
    try:
        logger.info(f"Starting copy for state={state}, upgrade={upgrade}")
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"Successfully copied files for state={state}, upgrade={upgrade}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error copying files for state={state}, upgrade={upgrade}: {e}")
        logger.error(f"Command output: {e.output}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error for state={state}, upgrade={upgrade}: {e}")
        return False

def batch_copy(states: List[str], upgrades: List[str], max_workers: int = 3) -> None:
    """
    Execute copy operations for multiple states and upgrades in parallel.
    
    Args:
        states: List of state codes (e.g., ['MA', 'CA'])
        upgrades: List of upgrade values (e.g., ['0', '1'])
        max_workers: Maximum number of concurrent copy operations
    """
    successful_copies = 0
    failed_copies = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for state in states:
            for upgrade in upgrades:
                futures.append(
                    executor.submit(copy_files, state, upgrade)
                )
        
        for future in futures:
            if future.result():
                successful_copies += 1
            else:
                failed_copies += 1
    
    logger.info(f"Copy operations completed:")
    logger.info(f"Successful copies: {successful_copies}")
    logger.info(f"Failed copies: {failed_copies}")

if __name__ == "__main__":
    # Example usage
    states_to_copy = ["IL", "NY"]  # Add your desired states
    upgrades_to_copy = ["0", "3"]        # Add your desired upgrades
    
    # Ensure s5cmd is installed
    try:
        subprocess.run(["s5cmd", "--version"], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        logger.error("s5cmd is not installed. Please install it first.")
        exit(1)
    
    # Execute the batch copy
    batch_copy(states_to_copy, upgrades_to_copy)