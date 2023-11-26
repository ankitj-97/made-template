import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, Text, MetaData, Table

# Download and read the CSV data
url = "https://downloaddata.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = pd.read_csv(url, sep=';')

# Drop the "Status" column
df = df.drop(columns=['Status'])

# Convert 'Laenge' and 'Breite' to numeric types
df['Laenge'] = pd.to_numeric(df['Laenge'], errors='coerce')
df['Breite'] = pd.to_numeric(df['Breite'], errors='coerce')

# Define a function to check the validity of IFOPT values
def is_valid_ifopt(ifopt):
    if not ifopt:
        return False
    parts = ifopt.split(':')
    if len(parts) != 3:
        return False
    if len(parts[0]) != 2:
        return False
    for part in parts[1:]:
        if not part.isdigit():
            return False
    return True

# Filter rows with valid values
df = df[df['Verkehr'].isin(['FV', 'RV', 'nur DPN'])]
df = df[(df['Laenge'] >= -90) & (df['Laenge'] <= 90)]
df = df[(df['Breite'] >= -90) & (df['Breite'] <= 90)]
df = df[df['IFOPT'].apply(is_valid_ifopt)]

# Create SQLite database and write the cleaned data
engine = create_engine('sqlite:///trainstops.sqlite')

# Define a SQLAlchemy table
metadata = MetaData()
trainstops_table = Table('trainstops', metadata,
    Column('BFNr', Integer),
    Column('Station', Text),
    Column('Category', Text),
    Column('Verkehr', Text),
    Column('Laenge', Float),
    Column('Breite', Float),
    Column('Land', Text),
    Column('Betreiber', Text),
    Column('PLZ', Text),
    Column('Ort', Text),
    Column('IFOPT', Text)
)

# Create the table in the database
metadata.create_all(engine)

# Insert the cleaned data into the table
df.to_sql('trainstops', engine, index=False, if_exists='replace', dtype={
    'BFNr': Integer,
    'Station': Text,
    'Category': Text,
    'Verkehr': Text,
    'Laenge': Float,
    'Breite': Float,
    'Land': Text,
    'Betreiber': Text,
    'PLZ': Text,
    'Ort': Text,
    'IFOPT': Text
})