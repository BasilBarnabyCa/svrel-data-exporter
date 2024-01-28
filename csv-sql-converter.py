import pandas as pd
import os

# Directory containing your CSV files
csv_dir = "exports/csvs"

# Directory where you want to save the SQL files
sql_dir = "exports/inserts"
if not os.path.exists(sql_dir):
    os.makedirs(sql_dir)


# Function to convert a DataFrame to a list of SQL INSERT statements
def dataframe_to_sql_inserts(df, table_name):
    # Start the INSERT statement
    insert_prefix = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES\n"
    sql_inserts = []

    # Accumulate values for each row in a list
    values_list = []
    for index, row in df.iterrows():
        values = ", ".join(
            ["'{}'".format(str(val).replace("'", "''")) for val in row.values]
        )
        values_list.append(f"({values})")

    # Combine accumulated values into a single INSERT statement
    if values_list:  # Check if there are any rows to insert
        insert_statement = insert_prefix + ",\n".join(values_list) + ";"
        sql_inserts.append(insert_statement)

    return sql_inserts


# Process each CSV file in the directory
for csv_file in os.listdir(csv_dir):
    if csv_file.endswith(".csv"):
        # Construct the full file path
        csv_path = os.path.join(csv_dir, csv_file)

        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_path)

        # Convert the DataFrame to SQL INSERT statements
        # Use the CSV file name as the base table name, removing the extension
        base_table_name = csv_file.replace(".csv", "")

        # Format the table name to remove everything after the first underscore
        table_name = base_table_name.split("_", 1)[
            0
        ]  # Splits at the first underscore and takes the first part

        sql_inserts = dataframe_to_sql_inserts(df, table_name)

        # Write the SQL statements to a .sql file
        sql_file_name = f"{table_name}.sql"
        sql_file_path = os.path.join(sql_dir, sql_file_name)
        with open(sql_file_path, "w") as sql_file:
            sql_file.write("\n".join(sql_inserts))

        print(f"Generated SQL file for {table_name}")
