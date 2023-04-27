# CE Hub Data Extractor

## About The Project

### Project Name

CE Hub Data Extractor

### Description

The repository contains Python scripts that extract weather and soil data from CE Hub by submitting requests to the CE Hub REST API endpoint.
The request should contain geolocation information in either a CSV file or Excel sheet together with the interested start and end date.

The advantage of these scripts is that users can retrieve weather and soil data in bulk in an automated way,
instead of one set of data per request.

It also helps to select the best dataset to use based on the country code.

## Getting Started

### Prerequisites

In order to run the Python scripts successfully, you will need to install a Python IDE and the following libraries:

* pandas
* json
* urllib

Please contact CE Hub team pradeep.kethireddy@syngenta.com for an API key to submit REST API requests.

### Installation

The project repo can be cloned by running this command:

`git clone git@github.com:syngenta/ce-hub-data-extractor.git`

### Usage

#### Usage in Python

The project provides three ways to extract data from CE Hub:

* CE Hub data extraction for trial data with trial id. (with the best data domains recommended by CE Hub analytics team)
  * configure `cehub_trial.ini` file
  * execute `cehub_best_domains_trial.py`
* CE Hub data extraction for field data no trial id. (with the best data domains recommended by CE Hub analytics team)
  * configure `cehub_field.ini` file
  * execute `cehub_best_domains_field.py`
* CE Hub data extraction for trial data with data domains selected by user.
  * configure `cehub_trial.ini` file
  * execute `cehub_select_domains_trial.py`

Please configure cehub_trial.ini and cehub_field.ini files to use the information in your environment, file and column headers.

#### Usage in R

R project is provided to allow running Python scripts from RStudio GUI. First open RStudio project `cehub.Rproj` and then open script `py_run.R` contains code describing how to:

* Install & load the necessary R libraries
* Provide credentials
* Python installation verification
* Modify `ini` files from within R
* Running Python scripts from R

### Contact

* Vivian Lee - Vivian.Lee@Syngenta.com
* Mladen Cucak - mladen.cucak@syngenta.com

### Acknowledgment

Many thanks to the following colleagues who have helped to shape this project:

* Julita Stadnicka-Michalak
* Pradeep Kethireddy
* George Papadatos
