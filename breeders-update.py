import pandas as pd
import logging
from datetime import datetime
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# File Paths
file_path = "exports/csvs/personnel-config/Breeders_chunk_0.csv"
# output_path = "exports/csvs/personnel-config/Breeders_chunk_0_updated.csv"

# def determine_type(name):
# 	try:
# 		# Array of keywords indicating Type 0
# 		company_keywords = ["farms", "stables", "ltd", "company", "university"]

# 		# Convert name to lowercase for case-insensitive matching
# 		name_lower = name.lower()

# 		# Check for words indicating Type 0
# 		if any(word in name_lower for word in company_keywords):
# 			return 0
# 		# Check for 'syndicate' or '&' (without Type 0 keywords), which indicates Type 2
# 		elif "syndicate" in name_lower or (
# 			"&" in name_lower and not any(word in name_lower for word in company_keywords)
# 		):
# 			return 2
# 		# Everything else should be Type 1
# 		else:
# 			return 1
# 	except Exception as e:
# 		error_logger.error(e)
# 		return None

# try:
# 	df = pd.read_csv(file_path)
# 	# Function to determine the 'Type' based on the 'name'

# 	# Step 2: Apply the 'determine_type' function to the 'name' column and update the 'Type' column
# 	df["Type"] = df["Name"].apply(determine_type)

# 	# Step 3: Save the updated DataFrame to a new CSV file
# 	output_path = "exports/csvs/personnel-config/Breeders_chunk_0.csv"  # Update this with your desired output path
# 	df.to_csv(output_path, index=False)

# 	general_logger.info(f"Updated Breeder's type and saved to: {output_path}")
# except Exception as e:
# 	error_logger.error(e)

def determine_type(name):
    try:
        company_keywords = ["farms", "stables", "ltd", "company", "university"]
        name_lower = name.lower()
        if any(word in name_lower for word in company_keywords):
            return 0
        elif "syndicate" in name_lower or ("&" in name_lower and not any(word in name_lower for word in company_keywords)):
            return 2
        else:
            return 1
    except Exception as e:
        error_logger.error(f"Error processing 'determine_type' for name: {name}, error: {e}")
        return None  # Consider a default value or additional handling

file_path = "exports/csvs/personnel-config/Breeders_chunk_0.csv"

try:
	# Step 1: Load the CSV file
    df = pd.read_csv(file_path)

	# Step 2: Apply the 'determine_type' function to the 'name' column and update the 'Type' column
    df["Type"] = df["Name"].apply(determine_type)

	# Step 3: Save the updated DataFrame to a new CSV file
    df.to_csv(file_path, index=False)
    general_logger.info(f"Updated Breeder's type and saved to: {file_path}")
except Exception as e:
    error_logger.error(f"Error processing file {file_path}: {e}", exc_info=True)
