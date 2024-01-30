import pandas as pd
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Ensure export directories exist
export_dirs = ["exports", "exports/csvs", "exports/inserts"]
for dir_path in export_dirs:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

# Ensure the config directory exists
if not os.path.exists("config"):
    os.makedirs("config")

# Database connection parameters from environment variables
db_config = {
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "3306")  # Default MySQL port if not specified
}

# SQLAlchemy engine for MySQL
engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

# Specify the chunk size
chunk_size = 10000  # Adjust based on your memory capacity and table size

# Function to process each table
def process_table(table_name, config, export_subdir):
    if not config.get("enabled", False):
        return

    print(f"Processing {table_name}...")
    for i, chunk_df in enumerate(pd.read_sql(config["sql_query"], engine, chunksize=chunk_size)):
        # Replace NaN values with None (which becomes NULL in the CSV)
        chunk_df = chunk_df.where(pd.notnull(chunk_df), None)

        # Exclude columns
        if "exclude_columns" in config:
            chunk_df.drop(columns=config["exclude_columns"], inplace=True)

        # Convert column names to title case if enabled
        if config.get("title_case_column_names_enabled", True):  # Check if title casing for column names is enabled
            chunk_df.columns = [col.title() for col in chunk_df.columns]

        # Convert specified columns to title case
        for col in config.get("title_case_columns", []):
            if col in chunk_df.columns:  # Check if the column exists
                chunk_df[col] = chunk_df[col].astype(str).str.title()  # Apply title case
            else:
                print(f"Warning: Column '{col}' not found in '{table_name}'. Skipping title casing for this column.")

        # Rename columns
        if "rename_columns" in config:
            chunk_df.rename(columns=config["rename_columns"], inplace=True)

        # Export to CSV in the specific subdirectory for this config
        csv_export_path = os.path.join('exports/csvs', export_subdir, f'{table_name}_chunk_{i}.csv')
        chunk_df.to_csv(csv_export_path, index=False, header=i==0)
        print(f"Exported chunk {i} of {table_name} to {csv_export_path}")

# Loop through all config files in the 'config' directory
config_dir = "config"
for config_file in os.listdir(config_dir):
    if config_file.endswith(".json"):
        config_path = os.path.join(config_dir, config_file)
        with open(config_path) as file:
            tables_config = json.load(file)

        # Create a subdirectory for exports based on the config file name (without extension)
        export_subdir = os.path.splitext(config_file)[0]
        export_subdir_path = os.path.join('exports/csvs', export_subdir)
        if not os.path.exists(export_subdir_path):
            os.makedirs(export_subdir_path)

        # Process each table according to its configuration
        for table, config in tables_config.items():
            try:
                process_table(table, config, export_subdir)
            except SQLAlchemyError as e:
                print(f"Failed to process {table}: {e}")
