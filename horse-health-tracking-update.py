import pandas as pd
import os
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

file_path = "exports/csvs/horse-config/HorseHealthTracking_chunk_0.csv"  # Update this with the actual path to your CSV file

try:
    # Step 1: Load the CSV file
    df = pd.read_csv(file_path)

    # Step 2: Cleanup 'Comment' column by replacing '&amp;' and '&AMP;' with an empty string
    if 'Comment' in df.columns:
        df['Comment'] = df['Comment'].str.replace('&amp;', '', case=False, regex=False)
        df['Comment'] = df['Comment'].str.replace('&AMP;', '', case=False, regex=False)
        df['Comment'] = df['Comment'].str.replace(';', '', case=False, regex=False)

    # Step 3: Save the updated DataFrame back to a CSV file
    df.to_csv(file_path, index=False)

    general_logger.info(f"Cleaned up '&amp;' and '&AMP;' from 'Comment' column and saved to: {file_path}")
except Exception as e:
    error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
