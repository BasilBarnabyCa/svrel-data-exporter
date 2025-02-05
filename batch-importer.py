import os
import glob
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from logger_setup import setup_loggers
from urllib.parse import quote_plus

# Load environment variables (for database credentials)
load_dotenv()

# Ensure the 'logs' folder exists
if not os.path.exists("logs"):
    os.makedirs("logs")
    print("Created 'logs' directory.")

# Initialize loggers
general_logger, error_logger = setup_loggers()

# Database Configuration
db_config = {
    "database": os.getenv("SQL_EXECUTION_DB_DATABASE"),
    "user": os.getenv("SQL_EXECUTION_DB_USERNAME"),
    "password": quote_plus(os.getenv("SQL_EXECUTION_DB_PASSWORD")),
    "host": os.getenv("SQL_EXECUTION_DB_HOST", "127.0.0.1"),
    "port": os.getenv("SQL_EXECUTION_DB_PORT", "3307")
}

# Create the database engine
engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

def import_sql_scripts(folder_path, table_name):
    """
    Imports all SQL files in the specified folder that match the table name.
    Foreign key checks are turned off at the start and re-enabled at the end.
    """
    try:
        with engine.connect() as conn:
            # Disable foreign key checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            general_logger.info(f"Foreign key checks disabled for table: {table_name}")

            # Truncate the table
            general_logger.info(f"Truncating table: {table_name}")
            try:
                conn.execute(text(f"TRUNCATE TABLE `{table_name}`;"))
            except SQLAlchemyError as e:
                error_logger.error(f"Failed to truncate table {table_name}: {str(e)}")
                return

            # Find all SQL files matching the table name's chunks in the folder
            pattern = os.path.join(folder_path, f"{table_name}_chunk_*.sql")
            sql_files = sorted(glob.glob(pattern))
            
            # Log number of files found
            if not sql_files:
                general_logger.warning(f"No SQL files found for table: {table_name} in folder: {folder_path}")
                return
            
            general_logger.info(f"Found {len(sql_files)} SQL files for table: {table_name} in folder: {folder_path}")

            for sql_file in sql_files:
                try:
                    general_logger.info(f"Processing file: {os.path.basename(sql_file)}")
                    with open(sql_file, "r") as file:
                        sql_script = file.read().strip()
                        if sql_script:  # Execute non-empty scripts
                            conn.execute(text(sql_script))
                        else:
                            general_logger.warning(f"Skipped empty file: {os.path.basename(sql_file)}")
                except SQLAlchemyError as e:
                    error_logger.error(f"Error executing {os.path.basename(sql_file)}: {str(e)}")
                    continue  # Skip to the next file

            # Re-enable foreign key checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
            general_logger.info(f"Foreign key checks re-enabled for table: {table_name}")

    except SQLAlchemyError as e:
        error_logger.error(f"Critical database connection error for table: {table_name}: {str(e)}")
    except Exception as e:
        error_logger.error(f"Unexpected error for table: {table_name}: {str(e)}")


# Variables
folder_name = "exports/inserts"  # Replace with your folder path
table_name = "WorkoutDetails"  # Replace with the specific table name

# Call the function
import_sql_scripts(folder_name, table_name)