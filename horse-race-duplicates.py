import pandas as pd
import os
import glob
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory where the chunks are located
directory_path = "exports/csvs/horse-config"

try:
    # Get a list of all chunk files in the directory
    chunk_files = glob.glob(os.path.join(directory_path, "HorseRace_chunk_*.csv"))
    print(chunk_files)

    # Iterate over all chunk files
    for file_path in chunk_files:
        # Load the CSV file
        df = pd.read_csv(file_path)

      	# Record the original number of rows for logging
        original_row_count = len(df)

        # Drop duplicate rows
        df = df.drop_duplicates()

        # Record the new number of rows after dropping duplicates
        new_row_count = len(df)

        # Save the updated DataFrame back to a CSV file
        df.to_csv(file_path, index=False)
        # Save the updated DataFrame back to a CSV file
        df.to_csv(file_path, index=False)

        general_logger.info(f"Cleaned up patterns from specified columns and saved to: {file_path}")

except Exception as e:
    error_logger.error("Error processing chunk files", exc_info=True)
