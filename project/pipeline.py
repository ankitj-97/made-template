import pandas as pd
import sqlite3

#Create a connection
connection = sqlite3.connect('data/Dataset.sqlite')

# Fetch and load dataset 1
url1 = 'https://offenedaten-koeln.de/sites/default/files/Unfallstatistik%20K%C3%B6ln%202018.csv'
url2 = 'https://offenedaten-koeln.de/sites/default/files/Unfallstatistik%20K%C3%B6ln%202017.csv'
road_accidents_2018 = pd.read_csv(url1, encoding="iso-8859-1",  delimiter=';')
road_accidents_2017 = pd.read_csv(url2, encoding="iso-8859-1",  delimiter=';')


# Fetch and load dataset 2 (3 different files for 2020, 2021 and 2022)
url3 = 'https://offenedaten-koeln.de/sites/default/files/Fahrrad_Zaehlstellen_Koeln_2020.csv'
url4 = 'https://offenedaten-koeln.de/sites/default/files/Radverkehr%20f%C3%BCr%20Offene%20Daten%20K%C3%B6ln%202021.csv'
url5 = 'https://offenedaten-koeln.de/sites/default/files/Radverkehr%20f%C3%BCr%20Offene%20Daten%20K%C3%B6ln%202022.csv'
df_2020 = pd.read_csv(url3, encoding="iso-8859-1",  delimiter=';')
df_2021 = pd.read_csv(url4, encoding="iso-8859-1",  delimiter=';')
df_2022 = pd.read_csv(url5, encoding="iso-8859-1",  delimiter=';')

#Data Transformation for Dataset 1

# Combining both the files oe. for year 2017 and 2018
road_accidents= pd.concat([road_accidents_2018,road_accidents_2017])


# Dropping irrelevant column
road_accidents.drop(columns=['ID','ULAND','UREGBEZ','UKREIS','UGEMEINDE','LINREFX', 'LINREFY', 'IstFuss', 'UTYP1', 'IstKrad', 'IstGkfz', 'IstSonstige'], inplace=True)


#Renaming Columns
road_accidents.rename(columns= {'UJAHR':'Year', 'UMONAT':'Month', 'USTUNDE':'Hour', 'UWOCHENTAG ':'Weekday', 'UKATEGORIE':'Accident_Category', 'UART':'Accident_Type', 'ULICHTVERH':'Lighting_Conditions', 'IstRad': 'Accident_with_bike', 'IstPKW':'Accident_with_car', 'USTRZUSTAND':'Road_Condition'}, inplace=True)


#Replacing values with actual meaning

mapping = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 5: 'Thursday', 6: 'Friday', 7: 'Saturday'}
road_accidents['Weekday'] = road_accidents['Weekday'].replace(mapping)

accident_mapping = { 1: 'Accident with fatalities', 2: 'Accident with seriously injured people', 3: 'Accident with slightly injured people'}
road_accidents['Accident_Category'] = road_accidents['Accident_Category'].replace(accident_mapping)

accident_type_mapping = { 1: 'Driving accident', 2: 'Turning accident', 3: 'Turning / crossing accident', 4: 'Crossing accident', 5: 'Accident caused by stationary traffic', 6: 'Accident in parallel traffic', 7: 'Other accident', 8: 'Other accident', 9: 'Other accident'}
road_accidents['Accident_Type'] = road_accidents['Accident_Type'].replace(accident_type_mapping)

lighting_conditions_mapping = { 0: 'daylight', 1: 'dusk', 2: 'darkness' }
road_accidents['Lighting_Conditions'] = road_accidents['Lighting_Conditions'].replace(lighting_conditions_mapping)

accident_with_bike = {0: 'Accident without bicycle involvement', 1: 'Accident with bicycle involvement'}
road_accidents['Accident_with_bike'] = road_accidents['Accident_with_bike'].replace(accident_with_bike)

accident_with_car = {0: 'Accident without car involvement', 1: 'Accident with car involvement'}
road_accidents['Accident_with_car'] = road_accidents['Accident_with_car'].replace(accident_with_car)

road_conditions = {0: 'Dry', 1: 'Wet/damp/slippery', 2: 'Slippery in winter'}
road_accidents['Road_Condition'] = road_accidents['Road_Condition'].replace(road_conditions)


#Data Transformation for Dataset 2


#Renaming column names
df_2020.rename(columns= {'Jahr 2020':'Month'}, inplace=True)
df_2021.rename(columns= {'Unnamed: 0':'Month'}, inplace=True)
df_2022.rename(columns= {'Unnamed: 0':'Month'}, inplace=True)

#Repalcing column errors
df_2020.columns = df_2020.columns.str.replace('Ã¼', 'ü')
df_2020.columns = df_2020.columns.str.replace('Ã', 'ß')
df_2020.columns = df_2020.columns.str.replace('Ã¤', 'ä')

#Repalcing row errors
df_2020.replace({'Ã¼': 'ü', 'Ã ': 'ß', 'Ã¤': 'ä'}, regex=True, inplace=True)

#Fixing NaN values
df_2021.fillna(0, inplace=True)
df_2020.fillna(0, inplace=True)

#Adding year column
df_2021['Year']=2021
df_2020['Year']=2020
df_2022['Year']=2022

# Dropping extra columns
df_2021.drop(columns=['Rodenkirchener Brücke','Severinsbrücke', 'Neusser Straße'], inplace=True)
df_2022.drop(columns=['Rodenkirchener Brücke', 'Severinsbrücke', 'Hohe Pforte','Neusser Straße'], inplace=True)

#Combining both the datasets 
bicycle_traffic=pd.concat([df_2022, df_2021, df_2020])

#Load data into SQLite database
road_accidents.to_sql("road_accidents", connection, if_exists='replace', index=False)
bicycle_traffic.to_sql("bicycle_traffic", connection, if_exists='replace', index=False)


connection.commit()
connection.close()
