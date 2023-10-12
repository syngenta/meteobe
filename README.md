# About The Project

## Project Name
Meteoblue Environmental Data Extractor

## Description
The repository contains Python scripts that extract weather and soil data from Meteoblue by submitting request to the 
Meteoblue REST API endpoint. 
The request should contain geolocation information in either a CSV file or Excel sheet together with interested start and end date.

The advantage of these scripts is that users can retrieve weather and soil data in bulk in an automated way,
instead of one set of data per request.

It also helps to select best dataset to use based on the country code.

# Getting Started

## Prerequisites
In order to run the Python scripts successfully, you will need to install a Python IDE and the following libraries:

* pandas
* configparser
* configupdater
* meteoblue_dataset_sdk
* setuptools

Please contact Meteoblue for an API key to submit REST API requests.

## Installation
The project repo can be cloned by running this command:

`git clone https://github.com/syngenta/meteobe.git`

## Usage
### Fields in INI File Explained
The INI file contains the following fields:
* input_file_dir: the absolute path to your input CSV or Excel file
* output_file_dir: the absolute path to the output directory
* source_data_filename: the name of your input file
* sheet_name: in the case of Excel file, specify which sheet to load the data from
* latitude_col: latitude geo-location column
* longitude_col: longitude geo-location column
* country_code_col: the country code column in order to get the best domain dataset
* id_col: any ID column that can be used to join environmental data back to the original data file
* user_interested_columns: any date columns that can be used to define a start and end date range for data extraction.
* start_date_offset: number of offset dates to the start date column
* end_date_offset: number of offset dates to the end date column
* [BEST_Precipitation_Domains]: key value pair to set the best precipitation domain for specific countries.
* [BEST_Temperature_Domains]: key value pair to set the best temperature domain for specific countries.
* [BEST_Wind_Domains]: key value pair to set the best wind domain for specific countries.


### Usage in Python

The project provides a way to bulk extract environmental data from Meteoblue, it consists of two main functions:
* configurator.py
  * This script can be used to set user information in the sections in the INI file
  * It also provides clear_value() function to clear all the setting
* meteoblue_data_extractor.py
  * This is the main script for extracting weather and soil data from Meteoblue
  * Please make sure you use config.py to set user specific properties before executing this script.
There are also three JSON files in the config directory:
* codes.json: A JSON file downloaded from Meteoblue website, which contains all the weather and soil attributes available to use. 
  * Call configurator.get_code_json() to see the content
  * Call configurator.update_code_json(upload_code_json_file) to update new codes
* soil_request.json: A soil REST request JSON, update this file if you want to customise the weather and soil attributes 
  * Call configurator.get_soil_json_request() to see the content
  * Call configurator.update_soil_json_request(upload_soil_json_file) to update new soil REST request JSON
* weather_request.json: A weather REST request JSON, update this file if you want to customise the weather and soil attributes
  * Call configurator.get_weather_json_request() to see the content
  * Call configurator.update_weather_json_request(upload_weather_json_file) to update new weather REST request JSON


## Roadmap
- [ ] Upgrade pandas to above 2.0
- [ ] Add test codes
- [ ] Enlarge weather and soil JSON scope


## Contact
* Vivian Lee - vivianlee.southern@gmail.com
* Adam Tod - adam.tod@syngenta.com

## Acknowledgment
Many thanks to the following colleagues who have helped to shape this project:
* Julita Stadnicka-Michalak
* Pradeep Kethireddy
* George Papadatos

## License
MIT license is used.