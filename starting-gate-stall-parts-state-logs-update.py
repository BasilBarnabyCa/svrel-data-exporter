import pandas as pd
import os
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

file_path = "exports/csvs/maintenance-config/StartingGateStallPartStateLogs_chunk_0.csv"
parts_state_csv_file_path = "exports/csvs/maintenance-config/StartingGateStallPartStates_chunk_0.csv"

try:
	# Step 1: Load the CSV file
	df = pd.read_csv(file_path)
	parts_state_df = pd.read_csv(parts_state_csv_file_path)

	df.rename(columns={'Id': 'Id_temp'}, inplace=True)

	# Step 2: Merge df with parts_state_df on PartId and StallId
	df = df.merge(parts_state_df[['Id', 'PartId', 'StallId']], on=['PartId', 'StallId'], how='left')

	# Step 3: Rename the 'Id' column from parts_state_df to 'PartStateId' in df
	df.rename(columns={'Id': 'PartStateId'}, inplace=True)

	# Step 4: Drop the original PartId and StallId columns, and any other unnecessary columns from df
	df.drop(columns=['PartId', 'StallId'], inplace=True)

	df.rename(columns={'Id_temp': 'Id'}, inplace=True)

	# Step 3: Save the updated DataFrame back to a CSV file
	df.to_csv(file_path, index=False)

	general_logger.info(f"Updated Part State Id and saved to: {file_path}")
except Exception as e:
	error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
