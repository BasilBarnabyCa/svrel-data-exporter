import pandas as pd
import os
import glob
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory where the chunks are located
tracking_directory_path = "exports/csvs/stable-config"
stables_file_path = "exports/csvs/stable-config/Stables_chunk_0.csv"

try:
    # Load the stable data
    stables_df = pd.read_csv(stables_file_path)

    # Create a dictionary to map StableID to Stable names
    stable_id_to_name_mapping = stables_df.set_index('Id')['Name'].to_dict()

    # Get a list of all StableTracking chunk files in the directory
    tracking_chunk_files = glob.glob(os.path.join(tracking_directory_path, "StableTracking_chunk_*.csv"))

    # Iterate over all StableTracking chunk files
    for file_path in tracking_chunk_files:
        # Load the CSV file
        tracking_df = pd.read_csv(file_path)

        # Check if 'StableID' exists in the DataFrame
        if 'StableID' not in tracking_df.columns:
            general_logger.error(f"'StableID' column not found in {file_path}")
            continue

        # Map 'StableID' to 'StableName' using the mapping dictionary
        tracking_df['StableName'] = tracking_df['StableID'].map(stable_id_to_name_mapping)

        # Handle possible NaN values after mapping
        tracking_df['StableName'].fillna('Unknown', inplace=True)

        # Save the updated DataFrame back to a CSV file
        tracking_df.to_csv(file_path, index=False)

        # Log the changes
        general_logger.info(f"Updated 'StableName' column in {file_path} based on Stables_chunk.csv")

except Exception as e:
    error_logger.error("Error processing chunk files", exc_info=True)
