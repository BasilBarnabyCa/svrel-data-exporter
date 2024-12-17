import os
import pandas as pd
import shutil  # For file moving
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory containing chunks
chunk_dir = "exports/csvs/horse-config"
root_output_dir = "non_executable"  # Destination root folder for Dams chunks

# Ensure the root output folder exists
os.makedirs(root_output_dir, exist_ok=True)

try:
    # Step 1: Identify Horse and Dam chunks
    horse_chunks = [file for file in os.listdir(chunk_dir) if file.startswith("Horses_chunk_") and file.endswith(".csv")]
    dam_chunks = [file for file in os.listdir(chunk_dir) if file.startswith("Dams_chunk_") and file.endswith(".csv")]

    # Step 2: Sort chunks by numerical order
    horse_chunks.sort(key=lambda x: int(x.split("_chunk_")[1].replace(".csv", "")))
    dam_chunks.sort(key=lambda x: int(x.split("_chunk_")[1].replace(".csv", "")))

    # Step 3: Determine the starting index for Dams
    last_horse_chunk_number = int(horse_chunks[-1].split("_chunk_")[1].replace(".csv", "")) if horse_chunks else 0
    starting_dam_chunk_number = last_horse_chunk_number + 1

    # Step 4: Load a sample Horse chunk to match columns
    sample_horse_df = pd.read_csv(os.path.join(chunk_dir, horse_chunks[0])) if horse_chunks else pd.DataFrame()

    # Helper function to align, clean, and handle IsTattooed
    def align_and_clean_columns(df, sample_df):
        # Add missing columns and reorder to match sample
        for column in sample_df.columns:
            if column not in df.columns:
                if sample_df[column].dtype in ['int64', 'float64']:
                    df[column] = None  # Set to NULL for numeric columns
                elif sample_df[column].dtype == 'bool':
                    df[column] = False  # Default to False for boolean columns
                else:
                    df[column] = None  # Default to None for other types

        df = df.reindex(columns=sample_df.columns)  # Ensure correct column order

        # Fill missing values for IsTattooed with 0
        if 'IsTattooed' in df.columns:
            missing_tattooed_count = df['IsTattooed'].isna().sum()
            df['IsTattooed'] = df['IsTattooed'].fillna(0).astype(int)
            general_logger.info(f"Updated 'IsTattooed' for {missing_tattooed_count} rows where it was NULL.")
        else:
            # Create IsTattooed column with default value 0
            df['IsTattooed'] = 0
            general_logger.info("Created 'IsTattooed' column with default value 0.")
        
        # Fill missing values for IsImported with 0
        if 'IsImported' in df.columns:
            missing_tattooed_count = df['IsImported'].isna().sum()
            df['IsImported'] = df['IsImported'].fillna(0).astype(int)
            general_logger.info(f"Updated 'IsImported' for {missing_tattooed_count} rows where it was NULL.")
        else:
            # Create IsImported column with default value 0
            df['IsImported'] = 0
            general_logger.info("Created 'IsImported' column with default value 0.")
        
        # Fill missing values for IsTurnedOut with 0
        if 'IsTurnedOut' in df.columns:
            missing_tattooed_count = df['IsTurnedOut'].isna().sum()
            df['IsTurnedOut'] = df['IsTurnedOut'].fillna(0).astype(int)
            general_logger.info(f"Updated 'IsTurnedOut' for {missing_tattooed_count} rows where it was NULL.")
        else:
            # Create IsTurnedOut column with default value 0
            df['IsTurnedOut'] = 0
            general_logger.info("Created 'IsTurnedOut' column with default value 0.")
        
        # Fill missing values for IsRegistered with 0
        if 'IsRegistered' in df.columns:
            missing_tattooed_count = df['IsRegistered'].isna().sum()
            df['IsRegistered'] = df['IsRegistered'].fillna(0).astype(int)
            general_logger.info(f"Updated 'IsRegistered' for {missing_tattooed_count} rows where it was NULL.")
        else:
            # Create IsRegistered column with default value 0
            df['IsRegistered'] = 0
            general_logger.info("Created 'IsRegistered' column with default value 0.")

        return df

    # Step 5: Process Horse chunks
    for horse_chunk in horse_chunks:
        horse_chunk_path = os.path.join(chunk_dir, horse_chunk)
        horse_df = pd.read_csv(horse_chunk_path)

        # Align, clean, and handle IsTattooed
        aligned_horse_df = align_and_clean_columns(horse_df, sample_horse_df)

        # Validate and save the aligned Horse chunk
        aligned_horse_df.to_csv(horse_chunk_path, index=False)
        general_logger.info(f"Aligned and saved Horse chunk: {horse_chunk}")

    # Step 6: Process Dam chunks and rename them to Horses_chunk_n
    for i, dam_chunk in enumerate(dam_chunks):
        dam_chunk_path = os.path.join(chunk_dir, dam_chunk)
        dam_df = pd.read_csv(dam_chunk_path)

        # Align, clean, and handle IsTattooed
        aligned_dam_df = align_and_clean_columns(dam_df, sample_horse_df)

        # Set mandatory column data for Dams
        aligned_dam_df["Type"] = 0
        aligned_dam_df["MicrochipType"] = 0
        aligned_dam_df["Gender"] = 5

        # Generate a new chunk filename
        new_chunk_number = starting_dam_chunk_number + i
        new_chunk_name = f"Horses_chunk_{new_chunk_number}.csv"
        dam_output_path = os.path.join(chunk_dir, new_chunk_name)

        aligned_dam_df.to_csv(dam_output_path, index=False)
        general_logger.info(f"Processed and saved Dam chunk as: {new_chunk_name}")

        # Move the original Dams chunk to the root output directory
        destination_path = os.path.join(root_output_dir, dam_chunk)
        shutil.move(dam_chunk_path, destination_path)
        general_logger.info(f"Moved original Dam chunk {dam_chunk} to {destination_path}")

    general_logger.info("All Horse and Dam chunks successfully processed, aligned, and Dams chunks moved!")

except Exception as e:
    error_logger.error(f"Error processing chunks: {e}", exc_info=True)