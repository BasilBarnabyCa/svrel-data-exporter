import pandas as pd

# Step 1: Load the CSV file
file_path = "exports/csvs/personnel-config/Breeders_chunk_0.csv"  # Update this with the actual path to your CSV file
df = pd.read_csv(file_path)


# Function to determine the 'Type' based on the 'name'
def determine_type(name):
    # Array of keywords indicating Type 0
    company_keywords = ["farms", "stables", "ltd", "company", "university"]

    # Convert name to lowercase for case-insensitive matching
    name_lower = name.lower()

    # Check for words indicating Type 0
    if any(word in name_lower for word in company_keywords):
        return 0
    # Check for 'syndicate' or '&' (without Type 0 keywords), which indicates Type 2
    elif "syndicate" in name_lower or (
        "&" in name_lower and not any(word in name_lower for word in company_keywords)
    ):
        return 2
    # Everything else should be Type 1
    else:
        return 1


# Step 2: Apply the 'determine_type' function to the 'name' column and update the 'Type' column
df["Type"] = df["Name"].apply(determine_type)

# Step 3: Save the updated DataFrame to a new CSV file
output_path = "exports/csvs/personnel-config/Breeders_chunk_0.csv"  # Update this with your desired output path
df.to_csv(output_path, index=False)

print(f"Updated DataFrame has been saved to: {output_path}")
