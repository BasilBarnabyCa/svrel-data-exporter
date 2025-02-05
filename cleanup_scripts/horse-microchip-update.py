import pandas as pd
import glob
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logger_setup import setup_loggers

# Setup loggers
general_logger, error_logger = setup_loggers()

# Directory containing horse chunk files
directory_path = "exports/csvs/horse-config"

try:
    # Get all chunk files
    chunk_files = sorted(glob.glob(os.path.join(directory_path, "Horses_chunk_*.csv")))
    
    general_logger.info(f"Found {len(chunk_files)} chunk files: {chunk_files}")

    for file_path in chunk_files:
        try:
            general_logger.info(f"Processing file: {file_path}")

            # Load the CSV file
            df = pd.read_csv(file_path)

            # Ensure 'HasMicrochip' column exists right after 'JrcId'
            if 'JrcId' in df.columns:
                jrcid_index = df.columns.get_loc('JrcId') + 1
                if 'HasMicrochip' not in df.columns:
                    df.insert(jrcid_index, 'HasMicrochip', False)  # Default to False
                    general_logger.info(f"Added 'HasMicrochip' after 'JrcId' in {file_path}")

            # Set 'HasMicrochip' based on whether 'Microchip' has a value
            if 'Microchip' in df.columns:
                df['HasMicrochip'] = df['Microchip'].notnull() & df['Microchip'].astype(str).str.strip().ne('')
                general_logger.info(f"Updated 'HasMicrochip' in {file_path}")

            # Save the updated DataFrame
            df.to_csv(file_path, index=False)
            general_logger.info(f"Updated Horse data saved to: {file_path}")

        except Exception as e:
            error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)

except Exception as e:
    error_logger.error(f"Error processing chunk files: {e}", exc_info=True)