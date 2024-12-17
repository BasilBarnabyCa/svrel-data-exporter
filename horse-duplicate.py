import os
import pandas as pd
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory containing horse chunks
chunk_dir = "exports/csvs/horse-config"

try:
    # Step 1: Identify all horse chunks
    horse_chunks = [file for file in os.listdir(chunk_dir) if file.startswith("Horses_chunk_") and file.endswith(".csv")]
    horse_chunks.sort(key=lambda x: int(x.split("_chunk_")[1].replace(".csv", "")))

    # Step 2: Collect all IDs across chunks
    all_ids = set()
    updated_chunks = []  # Store updated DataFrames

    for chunk_index, chunk_file in enumerate(horse_chunks):
        chunk_path = os.path.join(chunk_dir, chunk_file)
        chunk_df = pd.read_csv(chunk_path)

        # Step 3: Process IDs and handle duplicates
        for idx, row in chunk_df.iterrows():
            horse_id = str(row["Id"])
            if horse_id in all_ids:
                # Append suffix based on the chunk index
                if chunk_index == 1:  # Sire chunk
                    new_id = f"{horse_id}_sire"
                elif chunk_index == 2:  # Dam chunk
                    new_id = f"{horse_id}_dam"
                else:
                    new_id = f"{horse_id}_duplicate"
                
                general_logger.info(f"Duplicate ID found: {horse_id} -> Renamed to {new_id}")
                chunk_df.at[idx, "Id"] = new_id  # Update the ID
            else:
                all_ids.add(horse_id)

        updated_chunks.append((chunk_path, chunk_df))

    # Step 4: Save updated chunks back
    for chunk_path, chunk_df in updated_chunks:
        chunk_df.to_csv(chunk_path, index=False)
        general_logger.info(f"Updated and saved chunk: {chunk_path}")

    general_logger.info("Duplicate ID handling completed successfully.")

except Exception as e:
    error_logger.error(f"Error handling duplicate IDs: {e}", exc_info=True)