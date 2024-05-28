import logging
import os
from config import LOG_FILE

def setup_logging():
    log_dir = os.path.dirname(LOG_FILE)  # Extract directory path from LOG_FILE
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)  # Create log directory if it does not exist

    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.DEBUG,  # Change to DEBUG to capture all log messages
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'  # Include logger name in the format
    )
