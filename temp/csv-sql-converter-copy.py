import pandas as pd
import os

# Base directory for CSV and SQL files
base_csv_dir = "exports/csvs"
sql_dir = "exports/inserts"
if not os.path.exists(sql_dir):
    os.makedirs(sql_dir)

# Function to convert a DataFrame to a list of SQL INSERT statements
def dataframe_to_sql_inserts(df, table_name):
    # Start the INSERT statement with column names surrounded by backticks
    insert_prefix = f"INSERT INTO {table_name} (`{'`, `'.join(df.columns)}`) VALUES\n"
    sql_inserts = []

    # Accumulate values for each row in a list
    values_list = []
    for index, row in df.iterrows():
        values = []
        for val in row.values:
            # Convert NaN values or empty strings to SQL NULL
            if pd.isna(val) or val == '':
                values.append('NULL')
            else:
                # Properly escape and quote other values
                val_str = str(val).replace("'", "''")
                values.append(f"'{val_str}'")
        values_str = ", ".join(values)
        values_list.append(f"({values_str})")

    # Combine accumulated values into a single INSERT statement
    if values_list:
        insert_statement = insert_prefix + ",\n".join(values_list) + ";"
        sql_inserts.append(insert_statement)

    return sql_inserts

# Process each subdirectory in the base CSV directory
for subdir in os.listdir(base_csv_dir):
    subdir_path = os.path.join(base_csv_dir, subdir)
    if os.path.isdir(subdir_path):
        # Process each CSV file in the subdirectory
        for csv_file in os.listdir(subdir_path):
            if csv_file.endswith(".csv"):
                # Construct the full file path
                csv_path = os.path.join(subdir_path, csv_file)

                # Read the CSV file into a DataFrame
                df = pd.read_csv(csv_path)

                # Use the CSV file name as the base table name, removing the extension
                base_table_name = csv_file.replace(".csv", "")

                # Format the table name to remove everything after the first underscore
                table_name = base_table_name.split("_", 1)[0]  # Splits at the first underscore and takes the first part

                # Generate SQL INSERT statements from the DataFrame
                sql_inserts = dataframe_to_sql_inserts(df, table_name)

                # Write the SQL statements to a .sql file in the SQL directory, under a subdirectory named after the config
                sql_file_subdir = os.path.join(sql_dir, subdir)
                if not os.path.exists(sql_file_subdir):
                    os.makedirs(sql_file_subdir)

                sql_file_name = f"{table_name}.sql"
                sql_file_path = os.path.join(sql_file_subdir, sql_file_name)
                with open(sql_file_path, "w") as sql_file:
                    sql_file.write("\n".join(sql_inserts))

                print(f"Generated SQL file for {table_name} in {subdir}")
