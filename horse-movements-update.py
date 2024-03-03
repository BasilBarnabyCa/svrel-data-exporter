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

# Columns to clean up
columns_to_clean = ['ArrivalTransporterName']  # Add more column names as needed

# Patterns to replace in the columns
patterns_to_replace = [r'&amp;', r'&AMP;', r';', r'\'']  # Use raw strings for regex patterns

try:
    # Get a list of all chunk files in the directory
    chunk_files = glob.glob(os.path.join(directory_path, "HorseMovements_chunk_*.csv"))
    print(chunk_files)

    # Iterate over all chunk files
    for file_path in chunk_files:
        # Load the CSV file
        df = pd.read_csv(file_path)

        # Iterate over each column to clean
        for column_to_clean in columns_to_clean:
            # Check if the column exists in the DataFrame
            if column_to_clean in df.columns:
                # Cleanup the column by replacing patterns with an empty string
                for pattern in patterns_to_replace:
                    # Using regex=True to handle all occurrences of the pattern
                    df[column_to_clean] = df[column_to_clean].str.replace(pattern, '', regex=True)

        # Save the updated DataFrame back to a CSV file
        df.to_csv(file_path, index=False)

        general_logger.info(f"Cleaned up patterns from specified columns and saved to: {file_path}")

except Exception as e:
    error_logger.error("Error processing chunk files", exc_info=True)
