import pandas as pd
import os
import glob
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory where the chunks are located
directory_path = "exports/csvs/racing-config"
column_to_update = 'RaceNameId'  # Replace with the actual column name you want to decrement

try:
	# Get a list of all chunk files in the directory
	chunk_files = glob.glob(os.path.join(directory_path, "Races_chunk_*.csv"))

	# Iterate over all chunk files
	for file_path in chunk_files:
		# Load the CSV file
		df = pd.read_csv(file_path)

		df[column_to_update] = df[column_to_update].fillna(0)
		df[column_to_update] = df[column_to_update].astype(int)
		df[column_to_update] = df[column_to_update].replace(0, pd.NA)

		# Save the updated DataFrame back to a CSV file
		df.to_csv(file_path, index=False)

		# Log the changes
		general_logger.info(f"Updated '{column_to_update}' {file_path}")

except Exception as e:
    error_logger.error("Error processing chunk files", exc_info=True)
