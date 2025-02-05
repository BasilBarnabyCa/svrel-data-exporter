import pandas as pd
import glob
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory where the chunk files are located
directory_path = "exports/csvs/personnel-config"

try:
    # Get a list of all chunk files in the directory
    chunk_files = glob.glob(os.path.join(directory_path, "Drivers_chunk_*.csv"))

    for file_path in chunk_files:
        try:
            # Load the CSV file
            df = pd.read_csv(file_path)

            # Ensure 'IsActive' and 'IsSuspended' columns exist after 'LastName'
            if 'LastName' in df.columns:
                last_name_index = df.columns.get_loc("LastName") + 1
                df.insert(last_name_index, "IsActive", 1)  # True
                df.insert(last_name_index + 1, "IsSuspended", 0)  # False

            # Save the updated DataFrame back to the CSV file
            df.to_csv(file_path, index=False)

            general_logger.info(f"Updated file: {file_path}")
        except Exception as e:
            error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)

except Exception as e:
    error_logger.error(f"Fatal error during cleanup: {e}", exc_info=True)