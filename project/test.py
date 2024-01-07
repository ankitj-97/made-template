import os
import pandas as pd
import pipeline

def test_dataframe_shape():
    #Test to check the shape of dataframe
    road_accidents_expected_shape = (9334, 10)  # expected shape of the dataframe 1
    bicycle_traffic_expected_shape = (36, 15)  # expected shape of the dataframe 2

    road_accidents_actual_shape = pipeline.road_accidents.shape  # actual shape of the dataframe 1
    bicycle_traffic_actual_shape = pipeline.bicycle_traffic.shape  # actual shape of the dataframe 2

    assert len(road_accidents_actual_shape) == 2
    assert len(bicycle_traffic_actual_shape) == 2
    assert road_accidents_expected_shape[0] == road_accidents_actual_shape[0]
    assert road_accidents_expected_shape[1] == road_accidents_actual_shape[1]
    assert bicycle_traffic_expected_shape[0] == bicycle_traffic_actual_shape[0]
    assert bicycle_traffic_expected_shape[1] == bicycle_traffic_actual_shape[1]

def test_data_load():
    #Test to check if the data load worked and the datasets are an object of class pandas.DataFrame
    assert isinstance(pipeline.road_accidents, pd.DataFrame)
    assert isinstance(pipeline.bicycle_traffic, pd.DataFrame)

def test_dataframe_columns():
    #Test to check if the columns are correct
    road_accidents_expected_columns = ['Year', 'Month', 'Hour', 'Weekday', 'Accident_Category',
       'Accident_Type', 'Lighting_Conditions', 'Accident_with_bike',
       'Accident_with_car', 'Road_Condition']  # expected columns of dataframe 1
    bicycle_traffic_expected_columns = ['Month', 'Deutzer Brücke', 'Hohenzollernbrücke', 'Neumarkt',
       'Zülpicher Straße', 'Bonner Straße', 'Venloer Straße', 'Vorgebirgswall',
       'Universitäts-straße', 'A.-Schütte-Allee', 'Vorgebirgspark',
       'A.-Silbermann-Weg', 'Stadtwald', 'Niederländer Ufer', 'Year']  # expected columns of dataframe 2
    

    road_accidents_actual_columns = pipeline.road_accidents.columns  # actual columns of dataframe 1
    bicycle_traffic_actual_columns = pipeline.bicycle_traffic.columns  # actual columns of dataframe 2

    assert len(road_accidents_actual_columns) == len(road_accidents_expected_columns)
    assert all([a == b for a, b in zip(road_accidents_actual_columns, road_accidents_expected_columns)])
    assert len(bicycle_traffic_actual_columns) == len(bicycle_traffic_expected_columns)
    assert all([a == b for a, b in zip(bicycle_traffic_actual_columns, bicycle_traffic_expected_columns)])
    


def test_output_exists():
    #Test if output file exists or not
    directory_path = os.getcwd()  # get directory path
    assert os.path.exists(os.path.join(directory_path, "Dataset.sqlite"))

def test_pipeline():
    #Declaration of all test functions
    test_output_exists()
    test_data_load()
    test_dataframe_shape()
    test_dataframe_columns()


if __name__ == "__main__":
    print("Initiating Pipeline test")
    test_pipeline()