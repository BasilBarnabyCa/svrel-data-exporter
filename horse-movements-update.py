import pandas as pd
import os
import glob
import re
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory containing CSV chunk files
directory_path = "exports/csvs/horse-config"

# Patterns to clean up
patterns_to_replace = [r'&amp;', r'&AMP;', r';']  # Add other patterns if necessary

def clean_value(value):
    """Clean and sanitize cell values."""
    if pd.isna(value) or value == '':  # Handle NULL or empty values
        return None
    try:
        value = str(value)
        # Remove unwanted patterns
        value = re.sub('|'.join(patterns_to_replace), '', value)
        # Remove newlines and carriage returns
        value = re.sub(r'[\n\r]+', ' ', value)
        # Remove or escape backslashes
        value = value.replace('\\', '')
        # Escape single quotes
        value = value.replace("'", "''")
        # Strip leading/trailing spaces
        return value.strip()
    except Exception as e:
        error_logger.error(f"Error cleaning value: {value} - {e}")
        return None

try:
    # Find all CSV files matching the pattern
    chunk_files = glob.glob(os.path.join(directory_path, "HorseMovements_chunk_*.csv"))
    general_logger.info(f"Found chunk files: {chunk_files}")

    for file_path in chunk_files:
        general_logger.info(f"Processing file: {file_path}")
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)

            # Clean all object (string) columns
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].apply(clean_value)

            # Save the cleaned data back to the same file
            df.to_csv(file_path, index=False)
            general_logger.info(f"Cleaned file saved: {file_path}")

        except Exception as e:
            error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)

except Exception as e:
    error_logger.error(f"Fatal error during cleanup: {e}", exc_info=True)