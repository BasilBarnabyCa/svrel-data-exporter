import pandas as pd
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Setup loggers
general_logger, error_logger = setup_loggers()

file_path = "exports/csvs/horse-config/Horses_chunk_0.csv"  # Update this with the actual path to your CSV file

try:
	# Step 1: Load the CSV file
	df = pd.read_csv(file_path)

	# Operation on 'Microchip' and 'ImportedMicrochip'
	if 'Microchip' in df and 'ImportedMicrochip' in df:
		microchip_index = df.columns.get_loc('Microchip') + 1
		df.insert(microchip_index, 'MicrochipType', df['ImportedMicrochip'].notnull().astype(int))
		df.drop(columns=['ImportedMicrochip'], inplace=True)

	# Update 'Type' column
	if 'Name' in df:
		name_index = df.columns.get_loc('Name') + 1
		df.insert(name_index, 'Type', 2)

	# Update Null Gender to Unknown enum value
	if 'Gender' in df.columns:
		df["Gender"] = df["Gender"].fillna(6).astype(int)

	# Step 3: Save the updated DataFrame
	df.to_csv(file_path, index=False)
	general_logger.info(f"Updated Horse's Horse Type, Microchip Types, deleted Imported Microchip column and saved to: {file_path}")
except Exception as e:
    error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)



