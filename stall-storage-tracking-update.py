import pandas as pd
import os
import glob
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Paths to the files and directories
tracking_directory_path = "exports/csvs/stable-config"
stables_file_path = "exports/csvs/stable-config/Stables_chunk_0.csv"
stalls_file_path = "exports/csvs/stable-config/Stalls_chunk_0.csv"

try:
    # Load the Stables and Stalls data
    stables_df = pd.read_csv(stables_file_path)
    stalls_df = pd.read_csv(stalls_file_path)

    # Create mappings from IDs to Names
    stable_id_to_name = stables_df.set_index('Id')['Name'].to_dict()
    stall_id_to_name = stalls_df.set_index('Id')['Name'].to_dict()

    # Get all StallStorageTracking chunk files
    tracking_chunk_files = glob.glob(os.path.join(tracking_directory_path, "StallStorageTracking_chunk_*.csv"))

    # Iterate over each chunk file
    for file_path in tracking_chunk_files:
        # Load the current chunk
        tracking_df = pd.read_csv(file_path)

        # Map StableId to Stable names and StallId to Stall names, and ensure they are strings
        tracking_df['StableName'] = tracking_df['StableId'].map(stable_id_to_name).astype(str)
        tracking_df['StallName'] = tracking_df['StallId'].map(stall_id_to_name).astype(str)

        # Concatenate StableName and StallName with a '-' and handle NaN values
        tracking_df['Name'] = tracking_df['StableName'].fillna('') + '-' + tracking_df['StallName'].fillna('')

        # Drop the StableId and StallId columns
        tracking_df.drop(['StableId', 'StallId'], axis=1, inplace=True)

        # Save the updated DataFrame back to the CSV file
        tracking_df.to_csv(file_path, index=False)

        # Log the update
        general_logger.info(f"Updated 'Name' column in {file_path} based on Stables and Stalls data")

except Exception as e:
    error_logger.error("Error processing chunk files", exc_info=True)
