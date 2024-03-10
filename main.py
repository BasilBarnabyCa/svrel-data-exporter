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

# Load environment variables
load_dotenv()

# DATABASE CONFIGURATIONS
# Source
db_config_processing = {
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "3306")
}

# Destination
db_config_sql_execution = {
    "database": os.getenv("SQL_EXECUTION_DB_DATABASE"),
    "user": os.getenv("SQL_EXECUTION_DB_USERNAME"),
   	"password": quote_plus(os.getenv("SQL_EXECUTION_DB_PASSWORD")),
    "host": os.getenv("SQL_EXECUTION_DB_HOST", "127.0.0.1"),
    "port": os.getenv("SQL_EXECUTION_DB_PORT", "3307")
}

# VARIABLES
chunk_size = 10000
config_dir = "config"
export_dirs = ["exports", "exports/csvs", "exports/inserts"]
sql_scripts_dir = 'exports/inserts'
engine_processing = sqlalchemy.create_engine(f"mysql+pymysql://{db_config_processing['user']}:{db_config_processing['password']}@{db_config_processing['host']}:{db_config_processing['port']}/{db_config_processing['database']}")
engine_sql_execution = sqlalchemy.create_engine(f"mysql+pymysql://{db_config_sql_execution['user']}:{db_config_sql_execution['password']}@{db_config_sql_execution['host']}:{db_config_sql_execution['port']}/{db_config_sql_execution['database']}")

# SCRIPTS TO EXECUTE
scripts = [
    'clean-up.py',
    'csv-sql-converter.py',
]

# DIRECTORIES SETUP
for dir_path in export_dirs:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

if not os.path.exists("config"):
    os.makedirs("config")

if not os.path.exists("logs"):
    os.makedirs("logs")

# LOGGERS SETUP
general_logger, error_logger = setup_loggers()

# EXECUTE SQL SCRIPTS
def execute_sql_scripts(folder_path, engine):
	try:
		with engine.connect() as conn:
			# Disable foreign key checks
			conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
			general_logger.info("Foreign key checks disabled.")
   
			# Initialize a set to track truncated tables
			truncated_tables = set()

			# Recursively execute SQL scripts in all subfolders
			for root, dirs, files in os.walk(folder_path):
				for file in files:
					if file.endswith('.sql'):
						# Derive the table name from the file name (assuming a naming convention)
						table_name = file.replace('.sql', '').split('_', 1)[0]

						# Check if the table has already been truncated
						if table_name not in truncated_tables:
							# Truncate the table if it's the first time processing it
							general_logger.info(f"Truncating table: {table_name}")
							conn.execute(text(f"TRUNCATE TABLE {table_name};"))
							truncated_tables.add(table_name)

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
								error_logger.error(f"Error executing {file_path}: {e.args[0]}")

			# Re-enable foreign key checks
			conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
			general_logger.info("Foreign key checks enabled.")

	except SQLAlchemyError as e:
		error_logger.error(f"Database connection error: {e.args[0]}")

# CONFIGURATION PROCESSOR
def get_table_data(table_name, config, export_subdir):
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
				for col in config["default_true_columns"]:
					if col in chunk_df.columns:
						chunk_df[col] = 1
					else:
						general_logger.warning(f"Warning: Column '{col}' not found in '{table_name}'. Skipping setting default True for this column.")

			# Set default values for columns
			if "default_false_columns" in config:
				for col in config["default_false_columns"]:
					if col in chunk_df.columns:
						chunk_df[col] = 0
					else:
						general_logger.warning(f"Warning: Column '{col}' not found in '{table_name}'. Skipping setting default True for this column.")

			# Convert specified columns to title case
			for col in config.get("title_case_columns", []):
				if col in chunk_df.columns:
					chunk_df[col] = chunk_df[col].astype(str).str.title()
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
			chunk_df.to_csv(csv_export_path, index=False, header=True)
			general_logger.info(f"Exported chunk {i} of {table_name} to {csv_export_path}")
	except Exception as e:
		error_logger.error(f"Error processing {table_name}: {e.args[0]}")

# JSON CONFIG PROCESSING LOOP
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
                get_table_data(table, config, export_subdir)
            except SQLAlchemyError as e:
                error_logger.error(f"Failed to process {table}: {e.args[0]}")

# SCRIPTS EXECUTION LOOP
for script in scripts:
	try:
		completed_process = subprocess.run(['python', script], check=True, text=True, capture_output=True)
		for line in completed_process.stdout.splitlines():
			general_logger.info(line)

	except subprocess.CalledProcessError as e:
		error_logger.error(f"Error executing {script}: {e}", exc_info=True)
		for line in e.stderr.splitlines():
			error_logger.error(line)

# CALL SQL SCRIPT EXECUTION
execute_sql_scripts(sql_scripts_dir, engine_sql_execution)
general_logger.info("Data transfer complete!")
