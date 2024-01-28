import sqlalchemy
import os
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

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

# SQLAlchemy engine for MySQL
engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

# Connectivity Test
try:
    # Connect to the database
    with engine.connect() as connection:
        # Execute a simple query to test connectivity
        result = connection.execute(sqlalchemy.text("SELECT 1"))
        # Fetch one result to ensure the query executed successfully
        print(f"Database connection successful, test query result: {result.fetchone()}")
except SQLAlchemyError as e:
    print(f"Database connection failed: {e}")
    exit()  # Exit the script if connection fails