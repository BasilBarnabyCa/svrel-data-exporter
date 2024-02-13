import pandas as pd
import os
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

file_path = "exports/csvs/personnel-config/Grooms_chunk_0.csv"  # Update this with the actual path to your CSV file

try:
	# Step 1: Load the CSV file
	df = pd.read_csv(file_path)
	# Step 2: Add the 'IsBanned' column and set its value to False for all rows
	df['IsBanned'] = 0  # Use 0 instead of False if you prefer a numeric representation

	# Step 3: Save the updated DataFrame back to a CSV file
	df.to_csv(file_path, index=False)

	general_logger.info(f"Updated Groom's IsBanned column and saved to: {file_path}")
except Exception as e:
	error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
