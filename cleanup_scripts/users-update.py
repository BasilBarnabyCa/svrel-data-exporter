import pandas as pd
import shutil  # For file moving
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory containing chunks
chunk_dir = "exports/csvs/users-config"
root_output_dir = "non_executable"  # Destination root folder for Sires chunks

# Ensure the root output folder exists
os.makedirs(root_output_dir, exist_ok=True)

try:
    # Step 1: Identify Horse and Sire chunks
    user_chunks = [file for file in os.listdir(chunk_dir) if file.startswith("Users_chunk_") and file.endswith(".csv")]

    for i, user_chunk in enumerate(user_chunks):
        user_chunk_path = os.path.join(chunk_dir, user_chunk)

        # Move the original Sires chunk to the root output directory
        destination_path = os.path.join(root_output_dir, user_chunk)
        shutil.move(user_chunk_path, destination_path)
        general_logger.info(f"Moved original Sire chunk {user_chunk} to {destination_path}")

    general_logger.info("All user chunks moved!")

except Exception as e:
    error_logger.error(f"Error processing chunks: {e}", exc_info=True)