import pandas as pd
import os
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Specify the column name where you want to check for null values
null_check_column = 'DeceasedAt'  # Replace with your actual column name

file_path = "exports/csvs/horse-config/HorseDeaths_chunk_0.csv"  # Update this with the actual path to your CSV file

try:
    # Step 1: Load the CSV file
    df = pd.read_csv(file_path)
    
    # Step 2: Delete all rows where the specified column's value is null
    df = df.dropna(subset=[null_check_column])
    
    # Step 3: Save the updated DataFrame back to a CSV file
    df.to_csv(file_path, index=False)

    general_logger.info(f"Removed rows with null values in '{null_check_column}' and saved to: {file_path}")
except Exception as e:
    error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)