import pandas as pd
import os
import json
import subprocess
import sqlalchemy
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from logger_setup import setup_loggers

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

# Ensure the 'logs' directory exists
if not os.path.exists("logs"):
    os.makedirs("logs")

# Setup loggers
general_logger, error_logger = setup_loggers()

# Database connection parameters from environment variables
db_config_processing = {
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "3306")  # Default MySQL port if not specified
}

# SQLAlchemy engine for MySQL
engine_processing = sqlalchemy.create_engine(f"mysql+pymysql://{db_config_processing['user']}:{db_config_processing['password']}@{db_config_processing['host']}:{db_config_processing['port']}/{db_config_processing['database']}")

# Database connection parameters for SQL script execution (different DB)
db_config_sql_execution = {
    "database": os.getenv("SQL_EXECUTION_DB_DATABASE"),
    "user": os.getenv("SQL_EXECUTION_DB_USERNAME"),
   	"password": quote_plus(os.getenv("SQL_EXECUTION_DB_PASSWORD")),
    "host": os.getenv("SQL_EXECUTION_DB_HOST", "127.0.0.1"),
    "port": os.getenv("SQL_EXECUTION_DB_PORT", "3306")  # Default MySQL port if not specified
}

# SQLAlchemy engine for SQL script execution
engine_sql_execution = sqlalchemy.create_engine(f"mysql+pymysql://{db_config_sql_execution['user']}:{db_config_sql_execution['password']}@{db_config_sql_execution['host']}:{db_config_sql_execution['port']}/{db_config_sql_execution['database']}")

# Function to execute SQL scripts in a given folder
def execute_sql_scripts(folder_path, engine):
	try:
		with engine.connect() as conn:
			# Disable foreign key checks
			conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
			general_logger.info("Foreign key checks disabled.")

			# Recursively execute SQL scripts in all subfolders
			for root, dirs, files in os.walk(folder_path):
				for file in files:
					if file.endswith('.sql'):
						# Derive the table name from the file name (assuming a naming convention)
						table_name = file.replace('.sql', '').replace('_inserts', '')

						# Truncate the table associated with this SQL file
						general_logger.info(f"Truncating table: {table_name}")
						conn.execute(text(f"TRUNCATE TABLE {table_name};"))

						# Execute the SQL script for the truncated table
						file_path = os.path.join(root, file)
						with open(file_path, 'r') as sql_file:
							sql_script = sql_file.read()
							try:
								# Execute each statement in the SQL script
								for statement in sql_script.strip().split(';'):
									if statement:
										conn.execute(text(statement))
								general_logger.info(f"Executed script: {file_path}")
							except SQLAlchemyError as e:
								error_logger.error(f"Error executing {file_path}: {e}")

			# Re-enable foreign key checks
			conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
			general_logger.info("Foreign key checks enabled.")

	except SQLAlchemyError as e:
		error_logger.error(f"Database connection error: {e}")

# Specify the chunk size
chunk_size = 10000  # Adjust based on your memory capacity and table size

# Function to process each table
def process_table(table_name, config, export_subdir):
	try:
		if not config.get("enabled", False):
			return

		general_logger.info(f"Processing {table_name}...")
		for i, chunk_df in enumerate(pd.read_sql(config["sql_query"], engine_processing, chunksize=chunk_size)):
			# Replace NaN values with None (which becomes NULL in the CSV)
			chunk_df = chunk_df.where(pd.notnull(chunk_df), None)

			# Exclude columns
			if "exclude_columns" in config:
				chunk_df.drop(columns=config["exclude_columns"], inplace=True)

			# Convert column names to title case if enabled
			if config.get("title_case_column_names_enabled", True):  # Check if title casing for column names is enabled
				chunk_df.columns = [col.title() for col in chunk_df.columns]

			# Set default values for columns
			if "default_true_columns" in config:
				for col in config["default_true_columns"]:  # Iterate over the specified columns
					if col in chunk_df.columns:  # Check if the column exists
						chunk_df[col] = 1  # Set all values in the column to True
					else:
						general_logger.warning(f"Warning: Column '{col}' not found in '{table_name}'. Skipping setting default True for this column.")

			# Set default values for columns
			if "default_false_columns" in config:
				for col in config["default_false_columns"]:  # Iterate over the specified columns
					if col in chunk_df.columns:  # Check if the column exists
						chunk_df[col] = 0  # Set all values in the column to True
					else:
						general_logger.warning(f"Warning: Column '{col}' not found in '{table_name}'. Skipping setting default True for this column.")

			# Convert specified columns to title case
			for col in config.get("title_case_columns", []):
				if col in chunk_df.columns:  # Check if the column exists
					chunk_df[col] = chunk_df[col].astype(str).str.title()  # Apply title case
				else:
					general_logger.warning(f"Warning: Column '{col}' not found in '{table_name}'. Skipping title casing for this column.")

			# Update enum columns
			if "enum_columns" in config:
				for col in config["enum_columns"]:
					if col in chunk_df.columns:
						# Subtract one from the column values and convert to integer, replace NaN with None
						chunk_df[col] = chunk_df[col].apply(lambda x: x - 1 if pd.notnull(x) else None).astype(pd.Int64Dtype())

			# Rename columns
			if "rename_columns" in config:
				chunk_df.rename(columns=config["rename_columns"], inplace=True)

			# Export to CSV in the specific subdirectory for this config
			csv_export_path = os.path.join('exports/csvs', export_subdir, f'{table_name}_chunk_{i}.csv')
			chunk_df.to_csv(csv_export_path, index=False, header=i==0)
			general_logger.info(f"Exported chunk {i} of {table_name} to {csv_export_path}")
	except Exception as e:
		error_logger.error(f"Error processing {table_name}: {e}")

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
                error_logger.error(f"Failed to process {table}: {e}")

# List of scripts you want to execute
scripts = [
    'clean-up.py',
    'csv-sql-converter.py',
]

# Loop through the scripts and execute each one
for script in scripts:
    try:
        # Execute the script
        completed_process = subprocess.run(['python', script], check=True, text=True, capture_output=True)

        # Print the standard output of the script
        general_logger.info(completed_process.stdout)

    except subprocess.CalledProcessError as e:
        # If an error occurs during script execution, print the error
        error_logger.error(f"Error executing {script}: {e}")

        # Optionally, print the standard error output
        error_logger.error(e.stderr)

# Execute SQL scripts from the 'exports/inserts' folder after data processing
sql_scripts_folder = 'exports/inserts'
execute_sql_scripts(sql_scripts_folder, engine_sql_execution)
