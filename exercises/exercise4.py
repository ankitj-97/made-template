import os
import zipfile
import urllib.request
import random
import string
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import *

# Step 1: Download and unzip data
download_url = 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip'
zip_file_path = 'mowesta-dataset.zip'
extracted_folder_path = 'mowesta-dataset'

urllib.request.urlretrieve(download_url, zip_file_path)

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_folder_path)

# Need to add extra colums for successfull reading of csv
data_path = os.path.join(extracted_folder_path, 'data.csv')
first_line_df = pd.read_csv(data_path, nrows=1, header=None, sep=';')

# Count the number of existing columns and removing duplicates
existing_columns = set(first_line_df.values.flatten().tolist())
additional_columns = 453 - len(existing_columns)

# Generate random column names
new_columns = [''.join(random.choices(string.ascii_uppercase + string.digits, k=5)) for _ in range(additional_columns)]

# Combine existing and random column names
all_columns = list(existing_columns) + new_columns

# Step 2: Reshape data
df = pd.read_csv(data_path,header=None, names=all_columns, skiprows=1, sep=';', decimal=',')
# Only use the columns "Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"
final_column = ['Geraet', 'Hersteller', 'Model', 'Monat', 'Temperatur in °C (DWD)', 'Batterietemperatur in °C', 'Geraet aktiv']
df = df[final_column]
# Renaming columns
df.rename(columns={'Temperatur in °C (DWD)': 'Temperatur', 'Batterietemperatur in °C': 'Batterietemperatur'}, inplace=True)
# Discard all columns to the right of “​​Geraet aktiv”
discard_index = df.columns.get_loc('Geraet aktiv')
df = df.iloc[:, :discard_index + 1]
df.dropna(inplace=True)

# Step 3: Transform data
df['Temperatur'] = df['Temperatur'].fillna(0)
df['Temperatur'] = (df['Temperatur'] * (9/5)) + 32
df['Batterietemperatur'] = df['Batterietemperatur'].fillna(0)
df['Batterietemperatur'] = (df['Batterietemperatur'] * (9/5)) + 32

# Step 4: Validate data
df['Geraet']=df['Geraet'].astype(int)
df = df[df['Geraet'] > 0]


# Step 5: Use fitting SQLite types and write data into SQLite database
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