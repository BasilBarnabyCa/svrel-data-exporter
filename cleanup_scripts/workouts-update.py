import pandas as pd
import glob
import sys
import os
from datetime import datetime
import re
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Directory where the chunks are located
directory_path = "exports/csvs/workouts-config"  # Update this with the actual directory path
users_file_path = "exports/csvs/users-config/Users_chunk_0.csv"  # Path to the Users chunk file

def clean_html(text):
    """Remove HTML tags and escape single quotes."""
    if not isinstance(text, str):
        return text
    text = re.sub(r'<.*?>', '', text)  # Remove HTML tags
    text = text.replace("'", "''")  # Escape single quotes
    text = text.replace('\n', ' ')  # Replace line breaks with spaces
    return text

try:
    # Load the Users data
    users_df = pd.read_csv(users_file_path)

    # Debug: Log the column names to ensure they match expectations
    general_logger.info(f"Users file columns: {list(users_df.columns)}")

    # Ensure required columns exist
    if all(col in users_df.columns for col in ['FirstName', 'LastName', 'Id']):
        # Create a mapping of user IDs to "firstname lastname"
        users_df['FullName'] = users_df['FirstName'].fillna('') + ' ' + users_df['LastName'].fillna('')
        users_mapping = users_df.set_index('Id')['FullName'].to_dict()
    else:
        raise KeyError("Missing required columns 'FirstName', 'LastName', or 'Id' in Users file.")

    # Get a list of all chunk files in the directory
    chunk_files = glob.glob(os.path.join(directory_path, "Workouts_chunk_*.csv"))

    # Iterate over all chunk files
    for file_path in chunk_files:
        try:
            # Load the CSV file
            df = pd.read_csv(file_path)

            # Debug: Log the column names in the Workouts file
            general_logger.info(f"Processing file: {file_path}, columns: {list(df.columns)}")

            # Cleanup 'Note' column
            if 'Note' in df.columns:
                df['Note'] = df['Note'].apply(clean_html)

            # Fix 'WorkoutDate' column to ensure it includes time
            if 'WorkoutDate' in df.columns:
                df['WorkoutDate'] = pd.to_datetime(df['WorkoutDate'], errors='coerce')
                df['WorkoutDate'] = df['WorkoutDate'].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else None
                )

            # Ensure 'IsCompleted' column exists and defaults to False if missing or null
            if 'IsCompleted' not in df.columns:
                df['IsCompleted'] = 0  # Add column if missing
            else:
                df['IsCompleted'] = df['IsCompleted'].fillna(0).astype(int)  # Set missing values to 0 (False)

            # Ensure 'IsApproved' column exists and defaults to False if missing or null
            if 'IsApproved' not in df.columns:
                df['IsApproved'] = 0  # Add column if missing
            else:
                df['IsApproved'] = df['IsApproved'].fillna(0).astype(int)  # Set missing values to 0 (False)

            # Replace Creator column (user IDs) with corresponding full names
            if 'Creator' in df.columns:
                df['Creator'] = df['Creator'].map(users_mapping).fillna("Unknown")
            else:
                df['Creator'] = "Unknown"  # Add column with default value if missing

            # Save the updated DataFrame back to the CSV file
            df.to_csv(file_path, index=False)

            # Log the changes
            general_logger.info(f"Processed file: {file_path}")

        except Exception as e:
            error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)

except Exception as e:
    error_logger.error("Error processing chunk files", exc_info=True)