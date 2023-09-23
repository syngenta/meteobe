"""Module to retrieve Meteoblue weather and soil data by using recommended best datasets"""
import pathlib
import sys
import pandas as pd
import json
import time
import os
import warnings
from datetime import timedelta, datetime

import meteoblue_dataset_sdk
from meteoblue_dataset_sdk.protobuf.dataset_pb2 import DatasetApiProtobuf

import constants
from config import ConfigUtil

# data domains
DOMAIN_NEMSGLOBAL = 'NEMSGLOBAL'
DOMAIN_ERA5 = 'ERA5'
DOMAIN_ERA5T = 'ERA5T'
DOMAIN_SOILGRIDS2 = 'SOILGRIDS2'

DEFAULT = 'default'

# Weather codes
TEMP = 11
PRECIPITATION = 61
HUMIDITY = 52
WIND_SPEED = 32
WIND_DIRECTION = 735
CLOUDS_TOTAL = 71
CLOUDS_HIGH = 75
CLOUDS_MEDIUM = 74
CLOUDS_LOW = 73
SUNSHINE_DURATION = 191
SHORTWAVE_RADIATION_TOTAL = 204
SHORTWAVE_RADIATION_DIRECT = 258
SHORTWAVE_RADIATION_DIFFUSE = 256
EVAPOTRANSPIRATION = 261
SOIL_TEMP = 85
SOIL_MOISTURE = 144
VAPPRESS_DEFICIT = 56
UV_MEAN = 721
# Weather level
LVL_2M_ELV_CORRECTED = '2 m elevation corrected'
LVL_2M_ABV_GND = '2 m above gnd'
LVL_SFC = 'sfc'
LVL_HIGH_CLD_LAY = 'high cld lay'
LVL_MID_CLD_LAY = 'mid cld lay'
LVL_LOW_CLD_LAY = 'low cld lay'
LVL_10CM_DOWN = '0-10 cm down'
LVL_10M_ABV_GND = '10 m above gnd'

# Soil codes
BULK_DENSITY = 808
CATION_EXCHANGE_CAPACITY = 809
CLAY_CONTENT_MASS_FRACTION = 803
COARSE_FRAGMENTS_VOLUMETRIC_FRACTION = 807
ORGANIC_CARBON_CONTENT = 811
ORGANIC_CARBON_DENSITY = 838
ORGANIC_CARBON_STOCKS = 837
SAND_CONTENT_MASS_FRACTION = 805
SILT_CONTENT_MASS_FRACTION = 804
TOTAL_NITROGEN_CONTENT = 817
PH_IN_H20 = 812
# Soil level
LVL_AGGREGATE = 'aggregated'
LVL_30 = '0-30 cm'
START_DEPTH_0 = 0
END_DEPTH_30 = 30
END_DEPTH_60 = 60

# Time resolution
TIME_RESOLUTION_DAILY = 'daily'
TIME_RESOLUTION_HOURLY = 'hourly'

# Weather REST Request JSON Keys
MAX = 'max'
MIN = 'min'
MEAN = 'mean'
SUM = 'sum'

# MeteoBlue REST Response JSON Keys
DOMAIN = 'domain'
TIME_INTERVALS = 'timeIntervals'
GEOMETRY = 'geometry'
COORDINATES = 'coordinates'
LOCATION_NAMES = 'locationNames'
CODES = 'codes'
VARIABLE = 'variable'
DATA_PER_TIME_INTERVAL = 'dataPerTimeInterval'
DATA = 'data'
AGGREGATION = 'aggregation'
START_DEPTH = 'startDepth'
END_DEPTH = 'endDepth'
UNIT = 'unit'
LEVEL = 'level'

# Crop column names and shared with Weather data
DATES = 'Dates'

# Temp columns
START_DATE_COLUMN = 'Start_Date'
END_DATE_COLUMN = 'End_Date'


class MeteoBlueConnector:
    """Connecting to Meteoblue via REST API and retrieve data by user input parameters"""

    def __init__(self, key: str, id_col: str, lat_col: str, lon_col: str,
                 country_code_col: str, codes_filename: str) -> None:
        """Instance of a MeteoBlueConnector with user API key"""
        self.key = key
        self.id_col = id_col
        self.lat_col = lat_col
        self.lon_col = lon_col
        self.country_code_col = country_code_col

        with open(codes_filename) as file:
            self.codes_lst = json.load(file)
            print(
                f'\nLoaded {len(self.codes_lst)} default unit, code and variable from Meteoblue JSON API '
                f'\n{self.codes_lst}')

    def lookup_variable_by_code(self, code: int) -> str:
        variable: str = ''
        for d in self.codes_lst:
            if d['code'] == code:
                variable = d['variable']
            else:
                continue
        return variable

    @staticmethod
    def validate_col_names(col_names: list, data: pd.DataFrame):
        for col_name in col_names:
            if col_name not in data.columns.tolist():
                warnings.warn(f'{col_name} does not exist in the input file, exit process')
                sys.exit(1)

    @staticmethod
    def convert_to_datetime(data: pd, col_names: list):
        """
        Converts date in any format into a standard format that can be consumed by Meteoblue REST API
        param data: The original data in data frame.
        param col_names: The original date column name.
        :return: None
        """
        for col_name in col_names:
            data[col_name] = pd.to_datetime(data[col_name], infer_datetime_format=True)

    @staticmethod
    def convert_timeinterval_to_list(start: int, end: int, stride: int) -> list:
        return [datetime.fromtimestamp(n).strftime("%Y-%m-%d %H:%M:%S") for n in range(start, end, stride)]

    @staticmethod
    def build_weather_data_query_best_dataset(country_code: str, precipitation_domains: dict, temperature_domains: dict,
                                              wind_domains: dict) -> list:
        """
        Builds weather data query for Meteoblue by using recommended best datasets.
        param country_code: ISO country code.
        param precipitation_domains: The best precipitation dataset for a specific country.
        param temperature_domains: The best temperature dataset for a specific country.
        param wind_domains: The best wind dataset for a specific country
        :return: A weather JSON query.
        """
        domain_precipitation = precipitation_domains.get(country_code, precipitation_domains.get(DEFAULT.upper()))
        domain_temp = temperature_domains.get(country_code, temperature_domains.get(DEFAULT.upper()))
        domain_wind = wind_domains.get(country_code, wind_domains.get(DEFAULT.upper()))
        print(
            f'country <{country_code}> use precipitation domain <{domain_precipitation}>, temperature domain '
            f'<domain_temp>, wind <{domain_wind}>')

        weather_query = [{
            "domain": DOMAIN_NEMSGLOBAL,
            "timeResolution": TIME_RESOLUTION_DAILY,
            "codes": [
                {"code": HUMIDITY, "level": LVL_2M_ABV_GND, "aggregation": MAX},  # Humidity_Max
                {"code": HUMIDITY, "level": LVL_2M_ABV_GND, "aggregation": MIN},  # Humidity_Min
                {"code": HUMIDITY, "level": LVL_2M_ABV_GND, "aggregation": MEAN},  # Humidity_Mean
                {"code": CLOUDS_TOTAL, "level": LVL_SFC, "aggregation": MEAN},  # Clouds_Total
                {"code": CLOUDS_HIGH, "level": LVL_HIGH_CLD_LAY, "aggregation": MEAN},  # Clouds_High
                {"code": CLOUDS_MEDIUM, "level": LVL_MID_CLD_LAY, "aggregation": MEAN},  # Clouds_Medium
                {"code": CLOUDS_LOW, "level": LVL_LOW_CLD_LAY, "aggregation": MEAN},  # Clouds_Low
                {"code": SUNSHINE_DURATION, "level": LVL_SFC, "aggregation": SUM},  # Sunshine_Duration
                {"code": SHORTWAVE_RADIATION_TOTAL, "level": LVL_SFC, "aggregation": MEAN},  # Shortwave_Radiation_Total
                {"code": SHORTWAVE_RADIATION_DIRECT, "level": LVL_SFC, "aggregation": MEAN},
                # Shortwave_Radiation_Direct
                {"code": SHORTWAVE_RADIATION_DIFFUSE, "level": LVL_SFC, "aggregation": MEAN},
                # Shortwave_Radiation_Diffuse
                {"code": EVAPOTRANSPIRATION, "level": LVL_SFC, "aggregation": SUM},  # Evapotranspiration
                {"code": SOIL_TEMP, "level": LVL_10CM_DOWN, "aggregation": MAX},  # Soil_Temp_Max
                {"code": SOIL_TEMP, "level": LVL_10CM_DOWN, "aggregation": MIN},  # Soil_Temp_Min
                {"code": SOIL_TEMP, "level": LVL_10CM_DOWN, "aggregation": MEAN},  # Soil_Temp_Mean
                {"code": SOIL_MOISTURE, "level": LVL_10CM_DOWN, "aggregation": MAX},  # Soil_Moisture_Max
                {"code": SOIL_MOISTURE, "level": LVL_10CM_DOWN, "aggregation": MIN},  # Soil_Moisture_Min
                {"code": SOIL_MOISTURE, "level": LVL_10CM_DOWN, "aggregation": MEAN},  # Soil_Moisture_Mean
                {"code": VAPPRESS_DEFICIT, "level": LVL_2M_ABV_GND, "aggregation": MAX},  # VapPress_Deficit_Max
                {"code": VAPPRESS_DEFICIT, "level": LVL_2M_ABV_GND, "aggregation": MIN},  # VapPress_Deficit_Min
                {"code": VAPPRESS_DEFICIT, "level": LVL_2M_ABV_GND, "aggregation": MEAN}  # VapPress_Deficit_Mean
            ]
        },
            {
                "domain": domain_temp,
                "timeResolution": TIME_RESOLUTION_DAILY,
                "codes": [
                    {"code": TEMP, "level": LVL_2M_ELV_CORRECTED, "aggregation": MAX},  # Temp_Max
                    {"code": TEMP, "level": LVL_2M_ELV_CORRECTED, "aggregation": MIN},  # Temp_Min
                    {"code": TEMP, "level": LVL_2M_ELV_CORRECTED, "aggregation": MEAN}  # Temp_Mean
                ]
            },
            {
                "domain": domain_precipitation,
                "timeResolution": TIME_RESOLUTION_DAILY,
                "codes": [
                    {"code": PRECIPITATION, "level": LVL_SFC, "aggregation": SUM}  # Precipitation
                ]
            },
            {
                "domain": domain_wind,
                "timeResolution": TIME_RESOLUTION_DAILY,
                "codes": [
                    {"code": WIND_SPEED, "level": LVL_10M_ABV_GND, "aggregation": MAX},  # Wind_Max
                    {"code": WIND_SPEED, "level": LVL_10M_ABV_GND, "aggregation": MIN},  # Wind_Min
                    {"code": WIND_SPEED, "level": LVL_10M_ABV_GND, "aggregation": MEAN},  # Wind_Mean
                    {"code": WIND_DIRECTION, "level": LVL_10M_ABV_GND}  # Wind_Direction
                ]
            },
            {
                "domain": DOMAIN_ERA5,
                "gapFillDomain": None,
                "timeResolution": TIME_RESOLUTION_HOURLY,
                "codes": [
                    {
                        "code": UV_MEAN,  # UV_Mean
                        "level": LVL_SFC
                    }
                ],
                "transformations": [
                    {
                        "type": "aggregateDaily",
                        "aggregation": MEAN
                    }
                ]
            }]

        return weather_query

    @staticmethod
    def build_soil_query(start_depth: int, end_depth: int) -> dict:
        """
        Builds soil data query for Meteoblue from only one domain.
        param start_depth: The start value of soil depth.
        param end_depth: The end value of soil depth.
        :return: Soil JSON query
        """
        soil_query = {
            "domain": DOMAIN_SOILGRIDS2,
            "codes": [
                {
                    "code": BULK_DENSITY,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth

                },
                {
                    "code": CATION_EXCHANGE_CAPACITY,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                },
                {
                    "code": CLAY_CONTENT_MASS_FRACTION,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                },
                {
                    "code": COARSE_FRAGMENTS_VOLUMETRIC_FRACTION,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                },
                {
                    "code": ORGANIC_CARBON_CONTENT,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                },
                {
                    "code": ORGANIC_CARBON_DENSITY,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                },
                {
                    "code": ORGANIC_CARBON_STOCKS,
                    "level": LVL_30  # only organic carbon stock only have one depth
                },
                {
                    "code": SAND_CONTENT_MASS_FRACTION,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                },
                {
                    "code": SILT_CONTENT_MASS_FRACTION,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                },
                {
                    "code": TOTAL_NITROGEN_CONTENT,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                },
                {
                    "code": PH_IN_H20,
                    "level": LVL_AGGREGATE,
                    "startDepth": start_depth,
                    "endDepth": end_depth
                }
            ]
        }
        return soil_query

    @staticmethod
    def load_json_from_file(json_file: str):
        with open(json_file) as f:
            return json.load(f)

    @staticmethod
    def build_json_payload(lat, lon, start_date, end_date, queries):
        """
        Builds Meteoblue REST JSON payload by using the queries built from query building function.
        param lat: The latitude of required weather data.
        param lon: The longitude of required weather data.
        param start_date: The start date of interested data range.
        param end_date: The end date of interested data range.
        param queries: The query that contains interested weather/soil attributes.
        :return: Fully constructed JSON request ready to submit to Meteoblue REST API.
        """
        params = {
            "units": {
                "temperature": "CELSIUS",
                "velocity": "KILOMETER_PER_HOUR",
                "length": "metric",
                "energy": "watts"
            },
            "geometry": {
                "type": "MultiPoint",
                "coordinates": [
                    [
                        lon, lat,
                    ]
                ],
                "locationNames": [
                    ""
                ],
                "mode": "preferLandWithMatchingElevation"
            },
            "format": "json",
            "timeIntervals": [
                f"{start_date}T+10:00\/{end_date}T+10:00"
            ],
            "timeIntervalsAlignment": "none",
            "queries": queries
        }

        return params

    def get_meteoblue_data(self, lat, lon, start_date, end_date, queries):
        """
        Sends Request to Meteoblue REST API.
        param lat: The latitude of required weather data.
        param lon: The longitude of required weather data.
        param start_date: The start date of interested data range.
        param end_date: The end date of interested data range.
        param queries:
        :return: The response from Meteoblue.
        """
        print(
            f'Getting data for geo location at latitude <{lat}> and longitude <{lon}> for date range from '
            f'<{start_date}> to <{end_date}>')

        try:
            client = meteoblue_dataset_sdk.Client(apikey=self.key)
            result = client.query_sync(self.build_json_payload(lat, lon, start_date, end_date, queries))

            return result

        except ConnectionError as ce:
            print(f'Got connection error with exception {ce}')
            time.sleep(10)
        except Exception as exception:
            print(
                f'No coordinates was found for geo location at latitude <{lat}> and longitude <{lon}>, '
                f'exception is {exception}')

    def convert_weather_json_to_dict(self, result: DatasetApiProtobuf, id_col: str, id_value: str) -> dict:
        """
        Converts weather data REST response DatasetApiProtobuf object to dictionary.

        param result: MeteoBlue response in DatasetApiProtobuf object.
        param id_col: Any unique ID from the field file
        param id_value: The value of the unique ID
        :return: A dictionary with required key value pair.
        """
        responses = {id_col: id_value}

        # geometry
        for i in range(len(result.geometries)):
            geometry = result.geometries[i]

            responses[self.lat_col] = geometry.lats[0]
            responses[self.lon_col] = geometry.lons[0]

            # dates
            responses[DATES] = self.convert_timeinterval_to_list(geometry.timeIntervals[0].start,
                                                                 geometry.timeIntervals[0].end,
                                                                 geometry.timeIntervals[0].stride)

            # codes
            codes = geometry.codes
            for j in range(len(codes)):
                code: int = codes[j].code
                var: str = self.lookup_variable_by_code(code)
                agg: str = codes[j].aggregation
                unit: str = codes[j].unit
                responses[var.replace(' ', '_') + '_(' + ''.join([agg[0].upper(), agg[1:]]) + ')_(' + unit + ')'] = \
                    codes[j].timeIntervals[0].data

        return responses

    def convert_soil_json_to_dict(self, result: DatasetApiProtobuf, id_col: str, id_value: str) -> dict:
        """
        Converts soil data REST response DatasetApiProtobuf object to a dictionary.
        param result: MeteoBlue response in DatasetApiProtobuf object.
        param id_col: Any unique ID from the field file
        param id_value: The value of the unique ID
        :return: A dictionary converted from response
        """

        responses = {id_col: id_value}
        for i in range(len(result.geometries)):
            # geometry
            geometry = result.geometries[i]
            responses[self.lat_col] = geometry.lats[0]
            responses[self.lon_col] = geometry.lons[0]

            # codes
            codes = geometry.codes
            for j in range(len(codes)):
                column_name: str
                code: int = codes[j].code
                var: str = self.lookup_variable_by_code(code)
                unit: str = codes[j].unit

                if codes[j].level == LVL_AGGREGATE:
                    start_depth: int = codes[j].startDepth
                    end_depth: int = codes[j].endDepth
                    column_name = var.replace(' ', '_') + '_(' + str(start_depth) + '-' + str(
                        end_depth) + ')_(' + unit + ')'
                else:
                    column_name = var.replace(' ', '_') + '_(' + codes[j].level + ')_(' + unit + ')'

                responses[column_name] = codes[j].timeIntervals[0].data

        return responses

    @staticmethod
    def load_data(input_file_dir: str, source_data_filename: str, sheet: str, add_on_cols: dict) -> pd.DataFrame:
        # Loads data into a dataframe, the crop type can be corn, grape etc.
        file_name_path = f'{input_file_dir}{os.path.sep}{source_data_filename}'
        print(f'Loading data from file: {file_name_path}... ')

        if pathlib.Path(file_name_path).suffix == '.csv':
            data = pd.read_csv(file_name_path)
        else:
            data = pd.read_excel(file_name_path, sheet_name=sheet)

        counter = 0
        for t in (add_on_cols.items()):
            data.insert(counter, t[0], t[1])
            counter += 1

        return data

    def time_data(self, df: pd, interested_dates_cols: list, start_date_offset, end_date_offset) -> pd:
        """
        Forming time data dataframe subset.
        param df: The dataframe of the input CSV file with geolocation information
        param interested_dates_cols: The date columns provided by the user.
        param start_date_offset: The start date offset provided by the user.
        param end_date_offset: The end date offset provided by the user.
        :return: A Pandas dataframe.
        """

        joined_on_cols: list = [self.id_col, self.lat_col, self.lon_col, self.country_code_col]
        start_end_cols: list = [START_DATE_COLUMN, END_DATE_COLUMN]

        # Converts the date columns to datetime for calculation
        self.convert_to_datetime(df, interested_dates_cols)

        # Calculates offset dates from the dates_of_interest columns, and add them back to the dataframe
        # This date will be used to extract the Meteoblue data.
        df[START_DATE_COLUMN] = df.apply(
            lambda x: min(x[interested_dates_cols]) + timedelta(days=start_date_offset), axis=1)
        df[END_DATE_COLUMN] = df.apply(
            lambda x: max(x[interested_dates_cols]) + timedelta(days=end_date_offset), axis=1)

        df[START_DATE_COLUMN] = pd.to_datetime(df[START_DATE_COLUMN]).dt.date
        df[END_DATE_COLUMN] = pd.to_datetime(df[END_DATE_COLUMN]).dt.date

        # Removes 'Unnamed' columns from the dataframe
        df.drop(df.columns[df.columns.str.contains('Unnamed')], axis=1, inplace=True)

        pd.set_option('display.max_rows', 100)
        pd.set_option('display.max_columns', None)

        df.info()

        df_with_time = df[joined_on_cols + start_end_cols]
        df_with_time.drop_duplicates(inplace=True, ignore_index=True)
        df_with_time.info()

        return df_with_time


if __name__ == "__main__":

    config: ConfigUtil = ConfigUtil(constants.INI_FILE)
    print(f'========== Loading property data from ini file {constants.INI_FILE} ==========')

    # Loads Meteoblue API key and constructs the endpoint url with the key
    api_key = config.get_property(constants.METEOBLUE_SECTION, constants.API_KEY)
    if not api_key:
        print("No API key is specified, terminating the process")
        sys.exit(1)

    # Loads the directory where the input data file is
    input_dir = config.get_property(constants.FILE_PATHS_SECTION, constants.INPUT_FILE_DIR)

    # Loads the directory that stores output weather/soil/failed data files, if not exists create one
    output_dir = config.get_property(constants.FILE_PATHS_SECTION, constants.OUTPUT_FILE_DIR)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f'Output directory <{output_dir}> does not exist, it is now created!')

    # Loads data file name
    source_filename = config.get_property(constants.FILE_PATHS_SECTION, constants.SOURCE_DATA_FILENAME)
    sheet_name = config.get_property(constants.FILE_PATHS_SECTION, constants.SHEET_NAME)

    # Creating output file paths
    data_file_name = os.path.splitext(source_filename)[0]
    data_file_name_path = f'{output_dir}{os.path.sep}{data_file_name}'

    # Weather and Soil related codes and variables files
    codes_file = config.get_property(constants.FILE_PATHS_SECTION, constants.CODES_FILE)
    weather_request_file = config.get_property(constants.FILE_PATHS_SECTION, constants.WEATHER_REQUEST_FILE)
    soil_request_file = config.get_property(constants.FILE_PATHS_SECTION, constants.SOIL_REQUEST_FILE)

    # Loading start and end offset values for Meteoblue
    s_date_offset = int(config.get_property(constants.METEOBLUE_SECTION, constants.START_DATE_OFFSET))
    e_date_offset = int(config.get_property(constants.METEOBLUE_SECTION, constants.END_DATE_OFFSET))

    # Geolocation information
    id_column = config.get_property(constants.METEOBLUE_SECTION, constants.ID_COL)
    lat_column = config.get_property(constants.METEOBLUE_SECTION, constants.LATITUDE_COL)
    lon_column = config.get_property(constants.METEOBLUE_SECTION, constants.LONGITUDE_COL)
    country_code_column = config.get_property(constants.METEOBLUE_SECTION, constants.COUNTRY_CODE_COL)

    # Check if start_date_offset > 0 set to = 0, if end_date_offset < 0 set to 0,
    # any dates in between start and end dates are already covered!
    if s_date_offset > 0:
        print(f'start_date_offset should be set to less than 0, use 0 now instead of {s_date_offset}')
        start_date_offset = 0
    if e_date_offset < 0:
        print(f'end_date_offset should be set to more than 0, use 0 now instead of {e_date_offset}')
        end_date_offset = 0

    # Loading user selected date columns
    user_interested_date_cols: list = config.get_property(constants.METEOBLUE_SECTION,
                                                          constants.USER_INTERESTED_DATE_COLS).split(',')

    # Loading best domain for countries
    precipitation_dom: dict = config.get_all_keys_properties(constants.BEST_PRECIPITATION_DOMAINS)
    temperature_dom: dict = config.get_all_keys_properties(constants.BEST_TEMPERATURE_DOMAINS)
    wind_dom: dict = config.get_all_keys_properties(constants.BEST_WIND_DOMAINS)

    internal_cols: dict = {}
    if not bool(country_code_column and country_code_column.strip()):
        print('No country_code column is specified')
        country_code_column = 'country_code'  # internal value, doesn't matter what it is
        internal_cols[country_code_column] = 'BR'  # TODO this can not be hardcoded

    print(f'\n=========== Validating column headers ==========')
    data_df = MeteoBlueConnector.load_data(input_dir, source_filename, sheet_name, internal_cols)
    MeteoBlueConnector.validate_col_names([id_column, lat_column, lon_column, country_code_column]
                                          + user_interested_date_cols, data_df)

    print(f'\n=========== Loading {source_filename} {sheet_name} into dataframe ==========')
    mb: MeteoBlueConnector = MeteoBlueConnector(api_key, id_column, lat_column, lon_column,
                                                country_code_column, codes_file)
    time_df: pd.DataFrame = mb.time_data(data_df, user_interested_date_cols, s_date_offset, e_date_offset)

    # Getting weather data from Meteoblue
    print(f'\n=========== Getting Weather Data from Meteoblue ==========')
    weather_df: pd.DataFrame = pd.DataFrame()
    failed_weather_df: pd.DataFrame = time_df.copy()

    load_w_file = input("Load weather json from weather_request.json file? type y/n: ")
    weather_queries = None
    if load_w_file == 'y':
        weather_queries = MeteoBlueConnector.load_json_from_file(weather_request_file)
    for weather_counter in range(len(time_df)):
        if load_w_file == 'n':
            weather_queries = mb.build_weather_data_query_best_dataset(time_df[mb.country_code_col][weather_counter],
                                                                       precipitation_dom,
                                                                       temperature_dom, wind_dom)

        weather_response = mb.get_meteoblue_data(time_df[mb.lat_col][weather_counter],
                                                 time_df[mb.lon_col][weather_counter],
                                                 time_df[START_DATE_COLUMN][weather_counter],
                                                 time_df[END_DATE_COLUMN][weather_counter],
                                                 weather_queries)
        try:
            response_dict = mb.convert_weather_json_to_dict(weather_response, id_column,
                                                            time_df[mb.id_col][weather_counter])
            each_field_df = pd.DataFrame(response_dict)
            weather_df = weather_df.append(each_field_df, ignore_index=True)
            failed_weather_df.drop([weather_counter], axis=0, inplace=True)
        except Exception as exe:
            print(f"Failed to extract weather data for latitude <{time_df[mb.lat_col][weather_counter]}> "
                  f"and longitude <{time_df[mb.lon_col][weather_counter]}> with error: <{exe}>")

    print(f'<{len(failed_weather_df)}> out of <{len(time_df)}> records failed to extract weather data from Meteoblue')
    weather_df.info()

    # Getting Soil data from Meteoblue
    print(f'\n=========== Getting Soil Data from Meteoblue ==========')
    soil_df: pd.DataFrame = pd.DataFrame()
    failed_soil_df: pd.DataFrame = time_df.copy()

    load_s_file = input("Load soil json from soil_request.json file? type y/n: ")
    if load_s_file == 'y':
        soil_queries = MeteoBlueConnector.load_json_from_file(soil_request_file)
    else:
        soil_queries = [mb.build_soil_query(START_DEPTH_0, END_DEPTH_30),
                        mb.build_soil_query(START_DEPTH_0, END_DEPTH_60)]

    for soil_counter in range(len(time_df)):
        soil_response = mb.get_meteoblue_data(time_df[mb.lat_col][soil_counter],
                                              time_df[mb.lon_col][soil_counter],
                                              time_df[START_DATE_COLUMN][soil_counter],
                                              time_df[END_DATE_COLUMN][soil_counter],
                                              soil_queries)
        try:
            response_dict = mb.convert_soil_json_to_dict(soil_response, id_column, time_df[mb.id_col][soil_counter])
            each_field_df = pd.DataFrame(response_dict)
            soil_df = soil_df.append(each_field_df, ignore_index=True)
            failed_soil_df.drop([soil_counter], axis=0, inplace=True)
        except Exception as exe:
            print(f"Failed to extract soil data for latitude <{time_df[mb.lat_col][soil_counter]}> "
                  f"and longitude <{time_df[mb.lon_col][soil_counter]}> with error: <{exe}>")

    print(f'<{len(failed_soil_df)}> out of <{len(time_df)}> records failed to extract soil data from Meteoblue')
    soil_df.info()

    print(f'\n\n========== Writing Weather Data to {output_dir}{os.path.sep} ==========')
    if len(weather_df) == 0:
        print('No weather data was retrieved from Meteoblue, please check connections or API key')
    else:
        weather_df.drop_duplicates().to_csv(data_file_name_path + '_weather_data_only_best_domains.csv', index=False,
                                            header=weather_df.columns, encoding='UTF-8-sig')
        print(f"Finished writing {data_file_name_path + '_weather_data_only_best_domains.csv'}")

    if len(failed_weather_df) > 0:
        failed_weather_df.drop_duplicates().to_csv(data_file_name_path + '_weather_data_only_best_domains_failed.csv',
                                                   index=False, header=failed_weather_df.columns, encoding='UTF-8-sig')
        print(f"Finished writing {data_file_name_path + '_weather_data_only_best_domains_failed.csv'} file")

    print(f'\n========== Writing Soil Data to {output_dir}{os.path.sep} ==========')
    if len(soil_df) == 0:
        print('No soil data was retrieved from Meteoblue, please check connections or API key')
    else:
        soil_df.drop_duplicates().to_csv(data_file_name_path + '_soil_data_only.csv', index=False,
                                         header=soil_df.columns, encoding='UTF-8-sig')
        print(f"Finished writing {data_file_name_path + '_soil_data_only.csv'}")

    if len(failed_soil_df) > 0:
        failed_weather_df.drop_duplicates().to_csv(data_file_name_path + '_soil_data_only_failed.csv',
                                                   index=False, header=failed_soil_df.columns, encoding='UTF-8-sig')
        print(f"Finished writing {data_file_name_path + '_soil_data_only_failed.csv'} file")
