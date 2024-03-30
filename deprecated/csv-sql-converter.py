import pandas as pd
import os
import logging
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# VARIABLES
base_csv_dir = "exports/csvs"
sql_dir = "exports/inserts"

# Ensure the SQL directory exists
if not os.path.exists(sql_dir):
    os.makedirs(sql_dir)

# DATAFRAME TO SQL INSERTS
def dataframe_to_sql_inserts(df, table_name):
    try:
        insert_prefix = f"INSERT INTO `{table_name}` (`{'`, `'.join(df.columns)}`) VALUES\n"
        sql_inserts = []

        values_list = []
        for index, row in df.iterrows():
            values = []
            for val in row.values:
                if pd.isna(val) or val == '':
                    values.append('NULL')
                else:
                    val_str = str(val).replace("'", "''")
                    values.append(f"'{val_str}'")
            values_str = ", ".join(values)
            values_list.append(f"({values_str})")

        if values_list:
            insert_statement = insert_prefix + ",\n".join(values_list) + ";"
            sql_inserts.append(insert_statement)

        return sql_inserts
    except Exception as e:
        error_logger.error(f"Error generating SQL inserts for table {table_name}: {e}", exc_info=True)
        return []

# Process each CSV file in the specified directories
for subdir in os.listdir(base_csv_dir):
    subdir_path = os.path.join(base_csv_dir, subdir)
    if os.path.isdir(subdir_path):
        for csv_file in os.listdir(subdir_path):
            if csv_file.endswith(".csv"):
                try:
                    csv_path = os.path.join(subdir_path, csv_file)
                    df = pd.read_csv(csv_path)

                    parts = csv_file.replace(".csv", "").split("_chunk_")
                    chunk_index = parts[1] if len(parts) > 1 else "0"
                    table_name = parts[0]

                    sql_inserts = dataframe_to_sql_inserts(df, table_name)

                    sql_file_subdir = os.path.join(sql_dir, subdir)
                    if not os.path.exists(sql_file_subdir):
                        os.makedirs(sql_file_subdir)

                    sql_file_name = f"{table_name}_chunk_{chunk_index}.sql"
                    sql_file_path = os.path.join(sql_file_subdir, sql_file_name)
                    with open(sql_file_path, "w") as sql_file:
                        sql_file.write("\n".join(sql_inserts))

                    general_logger.info(f"Generated SQL file for {table_name} chunk {chunk_index} in {subdir}")
                except Exception as e:
                    error_logger.error(f"Error processing file {csv_path}: {e}", exc_info=True)
