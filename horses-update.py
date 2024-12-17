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
        if 'MicrochipType' not in df.columns:
            # Create the 'MicrochipType' column if it doesn't exist
            microchip_index = df.columns.get_loc('Microchip') + 1
            df.insert(microchip_index, 'MicrochipType', df['ImportedMicrochip'].notnull().astype(int))
            general_logger.info("Created 'MicrochipType' column based on 'ImportedMicrochip'.")
        df.drop(columns=['ImportedMicrochip'], inplace=True)  # Drop the 'ImportedMicrochip' column
        general_logger.info("'ImportedMicrochip' column removed.")

    # Update 'Type' column only for missing values
    if 'Name' in df and 'Type' in df:
        missing_type_count = df['Type'].isna().sum()
        df['Type'] = df['Type'].fillna(2)  # Default value for Type (Runner)
        general_logger.info(f"Updated 'Type' for {missing_type_count} rows where it was missing.")
    elif 'Name' in df:  # Handle case where 'Type' column is missing
        name_index = df.columns.get_loc('Name') + 1
        df.insert(name_index, 'Type', 2)  # Default value for new 'Type' column
        general_logger.info("Created 'Type' column with default value 2.")

    # Update 'Gender' column only for missing values
    if 'Gender' in df.columns:
        missing_gender_count = df['Gender'].isna().sum()
        df["Gender"] = df["Gender"].fillna(6).astype(int)  # Default value for Gender (Unknown)
        general_logger.info(f"Updated 'Gender' for {missing_gender_count} rows where it was missing.")

    # NEW: Ensure 'IsTattooed' column is not NULL
    if 'IsTattooed' in df.columns:
        missing_tattooed_count = df['IsTattooed'].isna().sum()
        df['IsTattooed'] = df['IsTattooed'].fillna(0).astype(int)  # Default value is 0
        general_logger.info(f"Updated 'IsTattooed' for {missing_tattooed_count} rows where it was NULL.")
    else:
        # Create 'IsTattooed' column if it doesn't exist
        df['IsTattooed'] = 0  # Default value
        general_logger.info("Created 'IsTattooed' column with default value 0.")

    # Step 3: Save the updated DataFrame
    df.to_csv(file_path, index=False)
    general_logger.info(f"Updated Horse data and saved to: {file_path}")

except Exception as e:
    error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)