import pandas as pd
import os

# Step 1: Load the CSV file
file_path = "exports/csvs/personnel-config/Grooms_chunk_0.csv"  # Update this with the actual path to your CSV file
df = pd.read_csv(file_path)


# Step 2: Add the 'IsBanned' column and set its value to False for all rows
df['IsBanned'] = 0  # Use 0 instead of False if you prefer a numeric representation

# Step 3: Save the updated DataFrame back to a CSV file
output_path = "exports/csvs/personnel-config/Grooms_chunk_0.csv"  # Specify your desired output file path
df.to_csv(output_path, index=False)

print(f"Updated DataFrame saved to: {output_path}")
