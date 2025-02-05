import pandas as pd
import glob
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory where the chunk files are located
directory_path = "exports/csvs/stable-config"  # Ensure this is the correct path

# Mapping of storage type names to their corresponding IDs
storage_mapping = {
    "Horse": 1,
    "Unverified Horse": 2,
    "Equipment": 3,
    "Sawdust": 4,
    "Feed": 5,
    "Dormitory": 6
}

try:
    # Find all chunk files dynamically
    chunk_files = glob.glob(os.path.join(directory_path, "Stalls_chunk_*.csv"))

    if not chunk_files:
        general_logger.warning(f"No CSV chunk files found in {directory_path}")
    else:
        general_logger.info(f"Found {len(chunk_files)} Stalls chunk files in {directory_path}")

    # Process each chunk file dynamically
    for file_path in chunk_files:
        try:
            df = pd.read_csv(file_path)

            # Ensure 'StorageTypeId' column exists
            if "StorageTypeId" in df.columns:
                # Replace text values with corresponding IDs
                df["StorageTypeId"] = df["StorageTypeId"].apply(
                    lambda x: storage_mapping.get(str(x).strip(), None) if pd.notna(x) else None
                )

                # Ensure column is treated as an integer type
                df["StorageTypeId"] = pd.to_numeric(df["StorageTypeId"], errors="coerce").astype("Int64")

                # Save updated DataFrame back to the same file
                df.to_csv(file_path, index=False)

                # Log a sample of updates to confirm the fix
                general_logger.info(f"Updated 'StorageTypeId' in {file_path}: Sample Data\n{df[['Id', 'StorageTypeId']].head()}")

            else:
                general_logger.warning(f"Column 'StorageTypeId' not found in {file_path}, skipping.")

        except Exception as e:
            error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)

except Exception as e:
    error_logger.error("Fatal error while processing Stalls chunk files", exc_info=True)