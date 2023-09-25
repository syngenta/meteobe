#' Check and install pacman package
#' 
#' Checks if pacman package is installed, if not installs it.
#' 
#' @import pacman
#' 
#' @examples
#' # Call `pacman_install()` function
#' pacman_install()
#' library(pacman)
#' p_load(dplyr)
#' 
#' @export
pacman_install <- function() {
  if (!require(pacman)) {
    install.packages("pacman")
    library(pacman)
    message("pacman package installed successfully!")
  } else {
    message("pacman is already installed.")
  }
}

#' Get Weather Token
#' 
#' Check if WTH_GRD_PAT environment variable is available, if not prompt user to input the value.
#' @return A character string of the weather token.
#' @export
#' @examples
#' get_weather_token()
#' @export
get_weather_token <- function() {
  token <- Sys.getenv("WTH_GRD_PAT")
  if (token == "") {
    message("No weather token found. Please enter your token in console and press ENTER:")
    token <- readline(prompt = "")
    Sys.setenv(WTH_GRD_PAT = token)
  }else{
    cat(sprintf("\033[32m%s\033[0m\n", "API token is found!"))
  }
}

#' Check and install Python and specific packages 
#' 
#' Checks if Python is installed, and if not, installs Miniconda. 
#' Then, installs a specific package and checks if it works. 
#' 
#' @importFrom reticulate use_python py_config py_install
#' @importFrom base Sys.which
#' 
#' @examples
#' # Call `check_py_install()` function
#' check_py_install()
#' 
#' @export
check_py_install <- function() {
  
  python_path <- Sys.getenv("PYTHON")
  miniconda_url <- "https://docs.conda.io/en/latest/miniconda.html"
  py_install_msg <- "To install the specific package, use reticulate::py_install(package_name, ssl_verify = TRUE)."
  reticulate_learn_url <- "https://public.deq.virginia.gov/WPS/R/reticulate.pdf"
  
  # Check if Python is installed
  if (is.null(Sys.which("python"))) {
    
    # If not, install Miniconda
    cat(sprintf("\033[31m%s\033[0m\n", paste0("Please install Python by visiting ", miniconda_url, ".")))
    py_config()
    reticulate::use_python(python_path, required = TRUE)
    cat(sprintf("\033[32m%s\033[0m\n", "Python installed successfully!"))
    
  } else {
    cat(sprintf("\033[33m%s\033[0m\n", "Python is already installed."))
    py_config()
  }
  
  cat(sprintf("\033[34m%s\033[0m\n", py_install_msg))
  
  # Install a specific package and check if it works
  cat(sprintf("\033[35m%s\033[0m\n", paste0("To learn more about the 'reticulate' package, visit ", reticulate_learn_url, ".")))
  py_config()
  
  os <- reticulate::import("os")
  unlist(os$listdir("."))
}

#' Modify INI file based on user-provided filename
#' 
#' This function modifies an INI file on the fly based on the user-provided filename.
#' It changes the input file directory, source data filename, and output file directory in the INI file.
#' It also provides credentials for the METEOBLUE API.
#' 
#' @importFrom ini read.ini write.ini
#' @importFrom base substring Sys.getenv stopifnot
#' @importFrom here here
#' 
#' @param filename The name of the source data filename to be used in the INI file.
#'   Default is `NULL`, and it must be provided by the user.
#' 
#' @examples
#' # Call `update_ini_file()` function
#' update_ini_file("some_other_file.csv")
#' 
#' @export
update_ini_file <- function(filename = NULL) {
  
  stopifnot(!is.null(filename))
  
  # Read INI file
  ini <- ini::read.ini(here::here("mbe.ini"))
  
  # Change filepath to fit each user add variable
  ini$File_Paths$input_file_dir <- gsub("/", "\\", paste(here::here("input"),sep=.Platform$file.sep), fixed=TRUE)
  
  # Provide credentials
  ini$CE_Hub$api_key <- Sys.getenv("WTH_GRD_PAT")
  
  # Define the name of the file
  ini$File_Paths$source_data_filename <- filename
  
  ini$File_Paths$output_file_dir <- gsub("/", "\\", paste(here::here("output"),sep=.Platform$file.sep),fixed=TRUE)
  
  # Any variable that is empty needs to be provided as such as the ini file is not read
  # with empty variables in ini file, so they have ot be additionally defined
  ini$File_Paths$sheet_name <- ""
  
  # Write INI file
  ini::write.ini(ini, filepath = here::here("mbe.ini"))
}

