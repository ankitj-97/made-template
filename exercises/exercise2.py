import sqlite3
import pandas as pd

def read_csv(file_path, delimiter=';'):
    # Read CSV and handle missing values
    return pd.read_csv(file_path, delimiter=delimiter, na_values=['', 'NA', 'NaN'])

def convert_columns_to_numeric(df, columns):
    # Convert specified columns to numeric, handling non-numeric values
    for column in columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    return df

def drop_column(df, column):
    # Drop a specific column from the DataFrame
    return df.drop(column, axis=1, inplace=True)

def drop_rows_with_nan(df):
    # Drop rows with any NaN values
    return df.dropna()

def filter_data(df):
    # Filter DataFrame based on specified conditions
    return df[(df['Verkehr'].isin(['FV', 'RV', 'nur DPN'])) & 
              (df['Laenge'].between(-90, 90)) & 
              (df['Breite'].between(-90, 90)) & 
              (df['IFOPT'].str.match(r'^[a-zA-Z]{2}:\d+:\d+(\:\d+)?$'))]

def create_sqlite_table(df, table_name, conn, dtypes):
    # Create an SQLite table from the DataFrame
    df.to_sql(table_name, conn, index=False, if_exists='replace', dtype=dtypes)

def clean_and_write_data(file_path, table_name='trainstops', delimiter=';'):
    # Read CSV
    df = read_csv(file_path, delimiter)
    
    # Convert specific columns to numeric
    numeric_columns = ['Laenge', 'Breite', 'Betreiber_Nr']
    df = convert_columns_to_numeric(df, numeric_columns)
    
    # Connect to the SQLite database
    conn = sqlite3.connect('trainstops.sqlite')
    
    # Drop the "Status" column
    drop_column(df, 'Status')
    # Drop rows with invalid values
    #df = drop_rows_with_nan(df)
    print(df)
    # df = filter_data(df)

    # Define SQLite data types for the table
    dtypes = {
        'EVA_NR': 'INTEGER',
        'DS100': 'TEXT',
        'IFOPT': 'TEXT',
        'NAME': 'TEXT',
        'Verkehr': 'TEXT',
        'Laenge': 'FLOAT',
        'Breite': 'FLOAT',
        'Betreiber_Name': 'TEXT',
        'Betreiber_Nr': 'INTEGER'
    }

    # Create the SQLite table
    create_sqlite_table(df, table_name, conn, dtypes)

    # Commit changes and close connection
    conn.commit()
    conn.close()

# Example usage:
file_path = "C:/Users/DELL/Downloads/D_Bahnhof_2020_alle.csv"
clean_and_write_data(file_path)