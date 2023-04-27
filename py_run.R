# Libraries ---------------------------------------------------------------

# Function to check if packages needed to run this script are installed
# If they are not avaialble loccaly they are installed and the loaded
LoadPck <- function(list.of.packages){
  
  new.packages <-
    list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
  
  #Download packages that are not already present in the library
  if (length(new.packages))
    try(install.packages(new.packages))
  
  try(packages_load <-
        lapply(list.of.packages, require, character.only = TRUE))
  
  #Print warning if there is a problem with installing/loading some of packages
  if (any(as.numeric(packages_load) == 0)) {
    warning(paste("Package/s: ", paste(list.of.packages[packages_load != TRUE], sep = ", "), "not loaded!"))
  } else {
    print("All packages were successfully loaded.")
  }
  
  rm(list.of.packages, new.packages, packages_load)
  
  #if instal is not working try 
  #install.packages("ROCR", repos = c(CRAN="https://cran.r-project.org/"))
}

LoadPck(c("reticulate", "here", "ini", "usethis"))

# Python installation  ----------------------------------------

#' find out more about 'reticulate' package
# browseURL("https://public.deq.virginia.gov/WPS/R/reticulate.pdf")

# use_python("C:/Users/USER/Local/Microsoft/WindowsApps", required = T)
Sys.which("python")

#' install miniconda if there is no python installation in the system 
py_config()

# py_install("pandas", ssl)

os <- reticulate::import("os")

os$listdir(".")

# Editing .Renviron variables ---------------------------------------

# One can store passwords as environmental variables which helps safety 
#  of your credentials and reduces the need to repeating typing passwords 
usethis::edit_r_environ()
# Separate file named '.Renviron' will open and one can add this variable
# WTH_GRD_PAT = "Change_me_please" # grid data API key

# Close and save the file 
# restart R

# Sort the initialization file  -------------------------------------------
#' Ini file defines several variables that necessary to run the Python code
#' There are two versions and both examples are presented here.

# Run command and the output in console should show your API key
Sys.getenv("WTH_GRD_PAT")# read API key 
# You should not see empty string of quotation marks. Repeat the above steps

# Trial script ------------------------------------------------------------
#' Set up ini file fo the 'cehub_best_domains_trial.py' scrips
ini <- ini::read.ini(here::here("cehub_trial.ini"))

# Change filepath to fit each user add variable/
ini$File_Paths$input_file_dir <- 
  gsub("/", "\\", paste(here::here("input"),sep=.Platform$file.sep), fixed=TRUE)

# Provide credentials 
ini$CE_Hub$api_key <- Sys.getenv("WTH_GRD_PAT")

# Define the name of the file
ini$File_Paths$source_data_filename <-  
  "trial_data_sample.csv"

ini$File_Paths$output_file_dir <-   
  gsub("/", "\\", paste(here::here("output"),sep=.Platform$file.sep), fixed=TRUE)

# Any variable that is empty needs to be provided as such as the ini file is 
# does not read empty variables in ini file, so they have ot be additionally defined
ini$File_Paths$sheet_name <- ""

ini::write.ini(ini, filepath = here::here("cehub_trial.ini"))

# Runs the entire script
reticulate::source_python(  here::here("cehub_best_domains_trial.py"))

# Field script ------------------------------------------------------------
# Same code as above, just modified to run the other script
ini <- ini::read.ini(here::here("cehub_field.ini"))

# Change filepath to fit each user add variable
ini$File_Paths$input_file_dir <- 
  gsub("/", "\\", paste(here::here("input"),sep=.Platform$file.sep), fixed=TRUE)

ini$CE_Hub$api_key <-Sys.getenv("WTH_GRD_PAT")

ini$File_Paths$source_data_filename <-  
  "field_data_sample.csv"

ini$File_Paths$output_file_dir <-   
  gsub("/", "\\", paste(here::here("output"),sep=.Platform$file.sep), fixed=TRUE)

ini$File_Paths$sheet_name <- ""
ini$CE_Hub$COUNTRY_CODE_COL <- ""

ini::write.ini(ini, filepath = here::here("cehub_field.ini"))

# Runs the entire script
reticulate::source_python(here::here("cehub_best_domains_field.py"))


