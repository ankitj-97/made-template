import os
import zipfile
import urllib.request
import random
import string
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import *

# Download and unzip data
download_url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
zip_file_path = 'mowesta-dataset.zip'
extracted_folder_path = 'mowesta-dataset'

urllib.request.urlretrieve(download_url, zip_file_path)

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_folder_path)

# Need to add extra colums for successfull reading of csv
data_path = os.path.join(extracted_folder_path, 'data.csv')
first_line_df = pd.read_csv(data_path, nrows=1, header=None, sep=None)
existing_columns = list(first_line_df.values.flatten().tolist())
# Finding the maximum entries in a line in csv, so as to generate that many columns
# Initialize a variable to store the maximum number of entries
max_entries = 0

# Open the CSV file in read mode
with open(data_path, 'r') as file:
    # Iterate through each line in the file
    for line in file:
        # Split the line based on the colon (':') separator
        entries = line.strip().split(';')
        # Update the maximum number of entries if the current line has more entries
        max_entries = max(max_entries, len(entries))

# Generate random column names
new_columns = [''.join(random.choices(string.ascii_uppercase + string.digits, k=8)) for _ in range(max_entries)]

# Reshape data
df = pd.read_csv(data_path,header=None, names=new_columns, skiprows=1, sep=';', decimal=',')

# Only use the columns "Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"
final_column = ['Geraet', 'Hersteller', 'Model', 'Monat', 'Temperatur in °C (DWD)', 'Batterietemperatur in °C', 'Geraet aktiv']
columns_mapping = dict(zip(df.columns[:len(existing_columns)], existing_columns))
df=df.iloc[:, :11]
df.rename(columns=columns_mapping, inplace=True)
df = df[final_column]

# Renaming columns
df.rename(columns={'Temperatur in °C (DWD)': 'Temperatur', 'Batterietemperatur in °C': 'Batterietemperatur'}, inplace=True)

# Discard all columns to the right of “​​Geraet aktiv”
discard_index = df.columns.get_loc('Geraet aktiv')
df = df.iloc[:, :discard_index + 1]

# Transform data
# Transform temperatures in Celsius to Fahrenheit 
df['Temperatur'] = pd.to_numeric(df['Temperatur'], errors='coerce')
df['Temperatur'] = df['Temperatur'].fillna(0)
df['Temperatur'] = (df['Temperatur'] * (9/5)) + 32
df['Batterietemperatur'] = pd.to_numeric(df['Batterietemperatur'], errors='coerce')
df['Batterietemperatur'] = df['Batterietemperatur'].fillna(0)
df['Batterietemperatur'] = (df['Batterietemperatur'] * (9/5)) + 32

# Validating data
# “Geraet” to be an id over 0
df['Geraet'] = pd.to_numeric(df['Geraet'], errors='coerce')
df = df[df['Geraet'] > 0.0]


# Use fitting SQLite types and write data into SQLite database
engine = create_engine('sqlite:///temperatures.sqlite')
df.to_sql('temperatures', engine, index=False, if_exists='replace', dtype={
    'Geraet': Integer,
    'Hersteller': Text,
    'Model': Text,
    'Monat': Integer,
    'Temperatur': Float,
    'Batterietemperatur': Float,
    'Geraet aktiv': Text
})
