import pandas as pd
import sys
import os
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Column names
id_column = "HorseId"            # Primary key column
date_column = "DeceasedAt"  # Column used to determine latest entry
order_column = "CreatedAt"  # Column used to restore original order

# File path
file_path = "exports/csvs/horse-config/HorseDeaths_chunk_0.csv"  # Update this with the actual path

try:
    # Load the CSV file
    df = pd.read_csv(file_path)

    # Drop rows where DeceasedAt is null
    df = df.dropna(subset=[date_column])

    # Ensure the date columns are in proper datetime format
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df[order_column] = pd.to_datetime(df[order_column], errors="coerce")

    # Keep only the latest entry for each unique Id based on DeceasedAt
    df = df.sort_values(by=[id_column, date_column], ascending=[True, False]).drop_duplicates(subset=[id_column], keep="first")

    # Restore original order by CreatedAt
    df = df.sort_values(by=order_column, ascending=True)

    # Save the cleaned DataFrame back to the CSV file
    df.to_csv(file_path, index=False)

    general_logger.info(f"Processed {file_path}: Removed null '{date_column}' entries, kept latest per Id, and resorted by '{order_column}'.")

except Exception as e:
    error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)