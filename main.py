import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from environment variables
db_config = {
    "database": os.getenv("DB_DATABASE"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "3306")  # Default MySQL port if not specified
}

# Checking if Exports folder and sub folders exists and creating if it doesn't
if not os.path.exists("exports"):
    os.mkdir("exports")

if not os.path.exists("exports/csvs"):
    os.mkdir("exports/csvs")

if not os.path.exists("exports/inserts"):
    os.mkdir("exports/inserts")

# SQLAlchemy engine for MySQL
engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

# Connectivity Test
try:
    with engine.connect() as connection:
        result = connection.execute("SELECT 1")
        # If the above line executes without error, connection is successful
        print("Database connection successful.")
except SQLAlchemyError as e:
    print(f"Database connection failed: {e}")
    exit()  # Exit the script if connection fails

# SQL query to select data from the table
sql_query = "SELECT * FROM your_table"

# Specify the chunk size
chunk_size = 10000  # Adjust based on your memory capacity and table size

# Process in chunks
for i, chunk_df in enumerate(pd.read_sql(sql_query, engine, chunksize=chunk_size)):
    # Rename multiple columns as needed
    chunk_df.rename(columns={
        'old_column_name1': 'new_column_name1',
        'old_column_name2': 'new_column_name2',
        # Add more columns as needed
    }, inplace=True)

    # Exclude columns as needed
    chunk_df.drop(columns=['exclude_column1', 'exclude_column2'], inplace=True)

    # Title case a specific column if needed
    chunk_df['column_to_title_case'] = chunk_df['column_to_title_case'].str.title()

    # Export the chunk to a CSV file
    # If it's the first chunk, write header, otherwise append without header
    chunk_df.to_csv(f'exported_table_chunk_{i}.csv', index=False, header=i==0)

    print(f"Chunk {i} exported successfully.")
