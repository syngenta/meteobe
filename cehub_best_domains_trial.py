"""Module to retrieve CE Hub weather and soil data by CE Hub recommended best datasets"""
import pathlib

import pandas as pd
import json
import urllib.request
import urllib.error
from typing import List, Any
import time
import configparser
import re
import os
from datetime import timedelta

# Constants Section
INI_FILE = 'cehub_trial.ini'

# Section names
FILE_PATHS_SECTION = 'File_Paths'
CEHUB_SECTION = 'CE_Hub'

CEHUB_PRECIPITATION_DOMAINS = 'CE_Hub_Precipitation_Domains'
CEHUB_TEMPRETURE_DOMAINS = 'CE_Hub_Tempreture_Domains'
CEHUB_WIND_DOMAINS = 'CE_Hub_Wind_Domains'

# Property names
INPUT_FILE_DIR = 'input_file_dir'
OUTPUT_FILE_DIR = 'output_file_dir'
SOURCE_DATA_FILENAME = 'source_data_filename'
SHEET_NAME = 'sheet_name'
API_KEY = 'api_key'

TRIAL_COL = 'TRIAL_COL'
LATITUDE_COL = 'LATITUDE_COL'
LONGITUDE_COL = 'LONGITUDE_COL'
COUNTRY_CODE_COL = 'COUNTRY_CODE_COL'

USER_INTERESTED_COLS = 'user_interested_columns'

START_DATE_OFFSET = 'start_date_offset'
END_DATE_OFFSET = 'end_date_offset'

# CE Hub URL
CEHUB_URL = 'http://my.meteoblue.com/dataset/query?apikey='

# CE Hub data domains
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

# CE Hub REST Response JSON Keys
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


class PropertyUtil:
    """Gets user input values from the cehub_field.ini file"""

    @staticmethod
    def get_property(ini_file_name: str, section: str, key: str) -> str:
        """
        Gets a property value from the ini file.
        :param ini_file_name: The ini file name.
        :param section: The section name.
        :param key: The key name withing the section
        :return: The value of the given section and key.
        """
        config = configparser.ConfigParser()
        config.read(ini_file_name)

        if not config.has_section(section):
            print(
                f'Config file does not have section {section}, please check the name of section or api key file '
                f'existence')

        key_value: str = config[section][key]
        print(f'Property value <{key}> = {key_value}')
        return key_value

    @staticmethod
    def get_property_sections_with_regex(ini_file_name: str, search_words: str) -> list:
        """
        Gets all the section names by using regular expression.
        :param ini_file_name: The ini file name
        :param search_words:
        :return:
        """
        config = configparser.ConfigParser()
        config.read(ini_file_name)
        find_sections: list = []

        for section_name in config.sections():
            if bool(re.search(search_words, section_name)):
                find_sections.append(section_name)

        return find_sections

    @staticmethod
    def get_all_keys_properties(ini_file_name: str, section: str) -> dict:
        """
        Gets Key values by scanning the values. An example is to get the best domain based on country code
        :param ini_file_name: The ini file name.
        :param section: The section name.
        :return: A dictionary with value as the key and key as value.
        """
        config = configparser.ConfigParser()
        config.optionxform = str  # to preserve the case
        config.read(ini_file_name)
        keys = list(config[section].keys())

        country_domain: dict = {}
        for key in keys:
            print(f'key is <{key}> value is <{config[section][key]}>')
            values: list = config[section][key].split(',')
            for value in values:
                country_domain.update({value: key})
        return country_domain


class CeHubConnector:
    """Connecting to CE Hub via REST API and retrieve data by user input parameters"""

    def __init__(self, key: str, trial_col: str, lat_col: str, long_col: str, country_code_col: str) -> None:
        """Instance of a CeHubConnector with user API key"""
        self.cehub_endpoint = CEHUB_URL + key
        self.trial_col = trial_col
        self.lat_col = lat_col
        self.long_col = long_col
        self.country_code_col = country_code_col

    @staticmethod
    def convert_to_datetime(data: pd, col_names: list):
        """
        Converts date in any format into a standard format that can be consumed by CE Hub REST API
        :param data: The original data in data frame.
        :param col_names: The original date column name.
        :return: None
        """
        for col_name in col_names:
            data[col_name] = pd.to_datetime(data[col_name], infer_datetime_format=True)

    @staticmethod
    def build_weather_data_query_best_dataset(country_code: str, precipitation_domains: dict, temperature_domains: dict,
                                              wind_domains: dict) -> list:
        """
        Builds weather data query for CE Hub by using recommended best datasets.
        :param country_code: ISO country code.
        :param precipitation_domains: The best precipitation dataset for a specific country.
        :param temperature_domains: The best temperature dataset for a specific country.
        :param wind_domains: The best wind dataset for a specific country
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
        Builds soil data query for CE Hub from only one domain.
        :param start_depth: The start value of soil depth.
        :param end_depth: The end value of soil depth.
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
    def build_json_payload(lat, lon, trial, start_date, end_date, queries):
        """
        Builds CE Hub REST JSON payload by using the queries built from query building function.
        :param lat: The latitude of required weather data.
        :param lon: The longitude of required weather data.
        :param trial: The trial id.
        :param start_date: The start date of interested data range.
        :param end_date: The end date of interested data range.
        :param queries: The query that contains interested weather/soil attributes.
        :return: Fully constructed JSON request ready to submit to CE Hub REST API.
        """
        PARAMS = {
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
                    f"{trial}"
                ]
            },
            "format": "json",
            "timeIntervals": [
                f"{start_date}T+10:00\/{end_date}T+10:00"
            ],
            "timeIntervalsAlignment": "none",
            "queries": queries
        }

        return PARAMS

    def get_cehub_data(self, lat, lon, trial, start_date, end_date, queries):
        """
        Sends REST Request to CE Hub.
        :param lat: The latitude of required weather data.
        :param lon: The longitude of required weather data.
        :param trial: The trial id.
        :param start_date: The start date of interested data range.
        :param end_date: The end date of interested data range.
        :param queries:
        :return: The response from CE Hub.
        """
        PARAMS = self.build_json_payload(lat, lon, trial, start_date, end_date, queries)
        print(f'Getting trial <{trial}> for date range from <{start_date}> to <{end_date}>')

        try:
            response = urllib.request.Request(self.cehub_endpoint, json.dumps(PARAMS).encode("utf-8"),
                                              headers={'Content-type': 'application/json',
                                                       'Accept': 'application/json'})
            return json.loads(urllib.request.urlopen(response).read())

        except ConnectionError as ce:
            print(f'Got connection error with exception {ce}')
            time.sleep(10)
        except Exception as exe:
            print(f'No coordinates was found for trial: {trial}, exception is {exe}')

    def convert_weather_json_to_dict(self, json_response: List[Any]) -> dict:
        """
        Converts weather data REST call response JSON to dictionary.
        :param json_response: CE Hub JSON response.
        :return: A dictionary with required key value pair.
        """

        response_dict = {}
        for i in range(len(json_response)):
            json = json_response[i]

            # geometry
            geometry = json[GEOMETRY]
            coordinates = geometry[COORDINATES][0]
            response_dict[self.trial_col] = geometry[LOCATION_NAMES][0]
            response_dict[self.lat_col] = coordinates[0]
            response_dict[self.long_col] = coordinates[1]
            # response_dict[ALTITUDE] = coordinates[2]
            # dates
            response_dict[DATES] = json[TIME_INTERVALS][0]  # str(json[TIME_INTERVALS][0]).replace('T0000','')
            # codes
            codes = json[CODES]

            for j in range(len(codes)):
                agg: str = str(codes[j][AGGREGATION])
                unit: str = codes[j][UNIT]
                response_dict[str(codes[j][VARIABLE]).replace(' ', '_') + '_(' + ''.join([agg[0].upper(), agg[1:]]) +
                              ')_(' + unit + ')'] = codes[j][DATA_PER_TIME_INTERVAL][0][DATA][0]

        return response_dict

    def convert_soil_json_to_dict(self, json_response: List[Any]) -> dict:
        """
        Converts soil data REST call response JSON to dictionary.
        :param json_response:
        :return:
        """

        response_dict = {}
        for i in range(len(json_response)):
            json = json_response[i]

            # geometry
            geometry = json[GEOMETRY]
            coordinates = geometry[COORDINATES][0]
            response_dict[self.trial_col] = geometry[LOCATION_NAMES][0]
            response_dict[self.lat_col] = coordinates[0]
            response_dict[self.long_col] = coordinates[1]

            # codes
            codes = json[CODES]
            for j in range(len(codes)):
                column_name: str
                unit: str = codes[j][UNIT]
                if codes[j][LEVEL] == LVL_AGGREGATE:
                    start_depth: int = codes[j][START_DEPTH]
                    end_depth: int = codes[j][END_DEPTH]
                    column_name = str(codes[j][VARIABLE]).replace(' ', '_') + '_(' + str(start_depth) + '-' + str(
                        end_depth) + ')_(' + unit + ')'
                else:
                    column_name = str(codes[j][VARIABLE]).replace(' ', '_') + '_(' + codes[j][
                        LEVEL] + ')_(' + unit + ')'

                response_dict[column_name] = codes[j][DATA_PER_TIME_INTERVAL][0][DATA][0]

        return response_dict

    def load_trial_data(self, input_file_dir: str, source_data_filename: str, sheet_name: str,
                        interested_dates_cols: list, start_date_offset, end_date_offset) -> pd:
        """
        Loads user data.
        :param input_file_dir: The input file directory.
        :param source_data_filename: The input file name.
        :param sheet_name: The sheet name.
        :param interested_dates_cols: The date columns provided by the user.
        :param start_date_offset: The start date offset provided by the user.
        :param end_date_offset: The end date offset provided by the user.
        :return: A Pandas dataframe.
        """
        # Loads trial data into a dataframe, the crop type can be corn, grape etc.
        # Preprocessing BITs trial data
        file_name_path = f'{input_file_dir}{os.path.sep}{source_data_filename}'
        print(f'Loading bits trial data from {file_name_path}... ')

        if pathlib.Path(file_name_path).suffix == '.csv':
            trial_df = pd.read_csv(file_name_path)
        else:
            trial_df = pd.read_excel(file_name_path, sheet_name=sheet_name)

        joined_on_cols: list = [self.trial_col, self.lat_col, self.long_col, self.country_code_col]
        start_end_cols: list = [START_DATE_COLUMN, END_DATE_COLUMN]

        # Converts the date columns to datetime for calculation
        self.convert_to_datetime(trial_df, interested_dates_cols)

        # Calculates offset dates from the dates_of_interest columns, and add them back to the dataframe
        # This date will be used to extract the CE Hub data.
        trial_df[START_DATE_COLUMN] = trial_df.apply(
            lambda x: min(x[interested_dates_cols]) + timedelta(days=start_date_offset), axis=1)
        trial_df[END_DATE_COLUMN] = trial_df.apply(
            lambda x: max(x[interested_dates_cols]) + timedelta(days=end_date_offset), axis=1)

        trial_df[START_DATE_COLUMN] = pd.to_datetime(trial_df[START_DATE_COLUMN]).dt.date
        trial_df[END_DATE_COLUMN] = pd.to_datetime(trial_df[END_DATE_COLUMN]).dt.date

        # Removes 'Unnamed' columns from the dataframe
        trial_df.drop(trial_df.columns[trial_df.columns.str.contains('Unnamed')], axis=1, inplace=True)

        pd.set_option('display.max_rows', 100)
        pd.set_option('display.max_columns', None)

        trial_df.info()
        trial_df.loc[:, joined_on_cols + start_end_cols]

        trial_time = trial_df[joined_on_cols + start_end_cols]
        trial_time.drop_duplicates(inplace=True, ignore_index=True)
        trial_time.info()

        return trial_time


if __name__ == "__main__":

    print(f'========== Loading property data from ini file {INI_FILE} ==========')
    # Loads the directory where the input data file is
    input_dir = PropertyUtil.get_property(INI_FILE, FILE_PATHS_SECTION, INPUT_FILE_DIR)

    # Loads the directory that stores output weather/soil/merged data files, if not exists create one
    output_dir = PropertyUtil.get_property(INI_FILE, FILE_PATHS_SECTION, OUTPUT_FILE_DIR)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f'Output directory <{output_dir}> does not exist, it is now created!')

    # Loads trial data file name
    source_filename = PropertyUtil.get_property(INI_FILE, FILE_PATHS_SECTION, SOURCE_DATA_FILENAME)
    sheet_name = PropertyUtil.get_property(INI_FILE, FILE_PATHS_SECTION, SHEET_NAME)

    # Creating output file paths
    trial_data_file_name = os.path.splitext(source_filename)[0]
    trial_data_file_name_path = f'{output_dir}{os.path.sep}{trial_data_file_name}'

    # Loads CE Hub API key and constructs the endpoint url with the key
    api_key = PropertyUtil.get_property(INI_FILE, CEHUB_SECTION, API_KEY)

    # Loading start and end offset values for CE Hub
    start_date_offset = int(PropertyUtil.get_property(INI_FILE, CEHUB_SECTION, START_DATE_OFFSET))
    end_date_offset = int(PropertyUtil.get_property(INI_FILE, CEHUB_SECTION, END_DATE_OFFSET))

    # Geo location information
    trial_column = PropertyUtil.get_property(INI_FILE, CEHUB_SECTION, TRIAL_COL)
    lat_column = PropertyUtil.get_property(INI_FILE, CEHUB_SECTION, LATITUDE_COL)
    long_column = PropertyUtil.get_property(INI_FILE, CEHUB_SECTION, LONGITUDE_COL)
    country_code_column = PropertyUtil.get_property(INI_FILE, CEHUB_SECTION, COUNTRY_CODE_COL)

    # Check if start_date_offset > 0 set to = 0, if end_date_offset < 0 set to 0,
    # any dates in between start and end dates are already covered!
    if start_date_offset > 0:
        print(f'start_date_offset should be set to less than 0, use 0 now instead of {start_date_offset}')
        start_date_offset = 0
    if end_date_offset < 0:
        print(f'end_date_offset should be set to more than 0, use 0 now instead of {end_date_offset}')
        end_date_offset = 0

    # Loading user selected date columns
    user_interested_date_cols: list = PropertyUtil.get_property(INI_FILE, CEHUB_SECTION,
                                                                USER_INTERESTED_COLS).split(',')

    # Loading best domain for countries
    precipitation_dom: dict = PropertyUtil.get_all_keys_properties(INI_FILE, CEHUB_PRECIPITATION_DOMAINS)
    temperature_dom: dict = PropertyUtil.get_all_keys_properties(INI_FILE, CEHUB_TEMPRETURE_DOMAINS)
    wind_dom: dict = PropertyUtil.get_all_keys_properties(INI_FILE, CEHUB_WIND_DOMAINS)

    cehub: CeHubConnector = CeHubConnector(api_key, trial_column, lat_column, long_column, country_code_column)
    print(f'\n=========== Loading {source_filename} {sheet_name} into dataframe ==========')
    trial_time: pd = cehub.load_trial_data(input_dir, source_filename, sheet_name, user_interested_date_cols,
                                           start_date_offset, end_date_offset)
    total_trial: int = len(trial_time)

    # Getting weather data from CE Hub
    print(f'\n=========== Getting Weather data from CE Hub ==========')
    weather_df = pd.DataFrame()
    failed_weather_trials: int = 0

    for i in range(total_trial):
        weather_queries = cehub.build_weather_data_query_best_dataset(trial_time[cehub.country_code_col][i],
                                                                      precipitation_dom,
                                                                      temperature_dom, wind_dom)
        json_response = cehub.get_cehub_data(trial_time[cehub.lat_col][i], trial_time[cehub.long_col][i],
                                             trial_time[cehub.trial_col][i], trial_time[START_DATE_COLUMN][i],
                                             trial_time[END_DATE_COLUMN][i], weather_queries)
        try:
            response_dict = cehub.convert_weather_json_to_dict(json_response)
            each_trial_df = pd.DataFrame(response_dict)
            weather_df = weather_df.append(each_trial_df, ignore_index=True)
        except Exception as exe:
            print(f"Not working for {trial_time[cehub.trial_col][i]} and it failed with: {exe}")
            failed_weather_trials += 1

    print(f'<{failed_weather_trials}> trials has failed to retrieve from CE Hub out of <{total_trial}>')
    weather_df.info()

    print(f'\nModifying datetime from yyyymmhhT0000 to yyyy-mm-dd...')
    weather_df[DATES] = weather_df[DATES].str.replace(r'T0000', '')
    weather_df[DATES] = pd.to_datetime(weather_df[DATES].str[:8], format='%Y-%m-%d')

    # Getting Soil data from CE Hub
    print(f'\n=========== Getting Soil data from CE Hub ==========')
    soil_df = pd.DataFrame()
    failed_soil_trials: int = 0

    for i in range(len(trial_time)):
        soil_queries = [cehub.build_soil_query(START_DEPTH_0, END_DEPTH_30),
                        cehub.build_soil_query(START_DEPTH_0, END_DEPTH_60)]
        json_response = cehub.get_cehub_data(trial_time[cehub.lat_col][i], trial_time[cehub.long_col][i],
                                             trial_time[cehub.trial_col][i], trial_time[START_DATE_COLUMN][i],
                                             trial_time[END_DATE_COLUMN][i], soil_queries)
        try:
            response_dict = cehub.convert_soil_json_to_dict(json_response)
            each_trial_df = pd.DataFrame(response_dict)
            soil_df = soil_df.append(each_trial_df, ignore_index=True)
        except Exception as exe:
            print(f"Not working for {i} and it failed with: {exe}")
            failed_soil_trials += 1

    print(f'<{failed_soil_trials}> trials has failed to retrieve from CE Hub out of <{total_trial}>')
    soil_df.info()

    print(f'\n========== Writing Weather and Soil Data to {trial_data_file_name_path} ==========')
    weather_df.to_csv(trial_data_file_name_path + '_weather_data_only_best_domains.csv', index=False,
                      header=weather_df.columns, encoding='UTF-8-sig')
    soil_df.to_csv(trial_data_file_name_path + '_soil_data_only.csv', index=False, header=soil_df.columns,
                   encoding='UTF-8-sig')
    print(
        f"Finished writing {trial_data_file_name_path + '_weather_data_only_best_domains.csv'} and "
        f"{trial_data_file_name_path + '_soil_data_only.csv'}")
