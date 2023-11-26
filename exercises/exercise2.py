import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import *

url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = pd.read_csv(url, sep=';')

df['Laenge'] = df['Laenge'].str.replace(',', '.').astype(float)
df['Breite'] = df['Breite'].str.replace(',', '.').astype(float)
df = df.drop(columns=['Status'])


df = df[ (df['Laenge'] <= 90) & (df['Laenge'] >= -90)]
df = df[ (df['Breite'] <= 90) & (df['Breite'] >= -90)]
df = df[df['Verkehr'].isin(['FV', 'nur DPN', 'RV'])]
df = df.dropna()

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

engine = create_engine('sqlite:///trainstops.sqlite')
df.to_sql('trainstops', engine, index=False, if_exists='replace', dtype={
    "EVA_NR": Integer,
    "DS100": Text,
    "IFOPT": Text,
    "NAME": Text,
    "Verkehr": Text,
    "Laenge": Float,
    "Breite": Float,
    "Betreiber_Name": Text,
    "Betreiber_Nr": Integer,
})