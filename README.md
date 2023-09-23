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
* json
* pathlib 
* configparser
* configupdater
* meteoblue_dataset_sdk
* setuptools

Please contact Meteoblue for an API key to submit REST API requests.

## Installation
The project repo can be cloned by running this command:

`git clone git@gitlab.com:syngentagroup/tdi-data-pipelines/meteobe.git`

## Usage
### Fields in INI File Explained
The INI file contains the following fields:
* input_file_dir: the file path of your input CSV or Excel file
* output_file_dir: the file path of the output directory
* source_data_filename: the name of your input file
* sheet_name: in the case of Excel file, specify which sheet to load the data from
* codes_file=..\config\codes.json , the code attributes value file from Meteoblue
* weather_request_file=..\config\weather_request.json, a default Meteoblue weather JSON request file
* soil_request_file=..\config\soil_request.json, a default Meteoblue soil JSON request file
* latitude_col: latitude geo-location column
* longitude_col: longitude geo-location column
* country_code_col: the country code column in order to get the best domain dataset
* id_col: any ID column that can be used to join environmental data back to the original data file
* user_interested_columns: any date columns that can be used to define a start and end date range to extract the environmental data.
* start_date_offset: number of offset dates to the start date column
* end_date_offset: number of offset dates to the end date column
* user_interested_domains: specify the dataset domain here if prefer not to use the recommended best domain.
* [BEST_Precipitation_Domains]: key value pair to set the best precipitation domain for specific countries.
* [BEST_Temperature_Domains]: key value pair to set the best temperature domain for specific countries.
* [BEST_Wind_Domains]: key value pair to set the best wind domain for specific countries.


### Usage in Python

The project provide a way to bulk extract environmental data from Meteoblue, it consists of two main functions:
* configurator.py
  * This script can be used to set user information in the above section
  * It also provides clear_value() function to clear all the setting
* meteoblue_data_extractor.py
  * This is the main script to extract weather and soil data from Meteoblue
  * Please make sure you use config.py to set user specific properties before executing this script.


### Usage in R

R project is provided to allow running Python scripts from RStudio GUI. First open RStudio project cehub.Rprojand then open script `py_run.R` contains code describing how to: 
* Install&load the necessary R libraries   
* Provide credentials
* Python installation verification
* Modify `ini` files from within R
* Running python scripts from R


## Roadmap
- [x] Publish the repo to Syngenta internal users
- [x] Add R code example to call Python Scripts
- [ ] Remove CE Hub related content
- [ ] Make the repo available to public


## Contact
* Vivian Lee - Vivian.Lee@Syngenta.com
* Mladen Cucak - Mladen.Cucak@syngenta.com

## Acknowledgment
Many thanks to the following colleagues who have helped to shape this project:
* Julita Stadnicka-Michalak
* Pradeep Kethireddy
* George Papadatos

## License
MIT license is used.