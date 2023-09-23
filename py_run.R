# Libraries ---------------------------------------------------------------
source(file = "funs/Rfuns.R")

# Function to check if packages needed to run this script are installed
pacman_install()
pacman::p_load("reticulate", "here", "ini", "usethis")
 
#' Check and install Python and specific packages 
check_py_install()

#' Check if WTH_GRD_PAT environment variable is available, if not prompt user to input the value.
get_weather_token()
 
# Sort the initialization file  -------------------------------------------
#' Ini file defines several variables that necessary to run the Python code
update_ini_file("trial_data_sample.csv")

# Runs the entire script
# There will be several queries where the answer is needed to decide
#' whether user wants to use some extra variables. Please consult the documentation
reticulate::source_python(  here::here("meteoblue_data_extractor.py"))
