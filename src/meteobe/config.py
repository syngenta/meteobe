"""Module to write and read user specific properties to mbe.ini file"""
import configparser
import re
import constants as c
from configupdater import ConfigUpdater


class ConfigUtil:

    def __init__(self, ini_file_name: str):
        self.ini_file_name = ini_file_name
        self.config = configparser.ConfigParser()
        self.config.read(self.ini_file_name)

    def get_property(self, section: str, key: str) -> str:
        """
        Gets a property value from the ini file.
        param section: The section name.
        param key: The key name withing the section
        :return: The value of the given section and key.
        """
        if not self.config.has_section(section):
            print(
                f'Config file does not have section {section}, please check the name of section or api key file '
                f'existence')

        key_value: str = self.config[section][key]
        print(f'Property value <{key}> = {key_value}')
        return key_value

    def get_property_sections_with_regex(self, search_words: str) -> list:
        """
        Gets all the section names by using regular expression.
        param search_words:
        :return: A list of sections names in the config file
        """
        find_sections: list = []

        for section_name in self.config.sections():
            if bool(re.search(search_words, section_name)):
                find_sections.append(section_name)

        return find_sections

    def get_all_keys_properties(self, section: str) -> dict:
        """
        Gets Key values by scanning the values. An example is to get the best domain based on country code
        param section: The section name.
        :return: A dictionary with value as the key and key as value.
        """
        self.config.optionxform = str  # to preserve the case
        keys = list(self.config[section].keys())

        country_domain: dict = {}
        for key in keys:
            print(f'key is <{key}> value is <{self.config[section][key]}>')
            values: list = self.config[section][key].split(',')
            for value in values:
                country_domain.update({value: key})
        return country_domain

    def set_value(self, section: str, key_value: dict):
        """
        Sets or overwrites the value for a key.
        param section: The section name of input list of keys and values
        param key_value: The key and value pairs in a list
        :return: None
        """
        updater: ConfigUpdater = ConfigUpdater()
        updater.read(self.ini_file_name)
        for key in key_value:
            updater[section][key].value = key_value[key]

        updater.update_file()

    def clear_value(self):
        """
        Remove some user specific fields
        :return: None
        """
        filepath_sec_clear_lst: dict = {c.INPUT_FILE_DIR: "", c.OUTPUT_FILE_DIR: "",
                                        c.SOURCE_DATA_FILENAME: "", c.SHEET_NAME: ""}
        meteoblue_sec_clear_lst: dict = {c.API_KEY: "", c.ID_COL: "", c.LATITUDE_COL: "",
                                         c.LONGITUDE_COL: "", c.COUNTRY_CODE_COL: "", c.USER_INTERESTED_DATE_COLS: ""}

        print('Clearing the user specific values in the config ini file...')
        self.set_value(c.FILE_PATHS_SECTION, filepath_sec_clear_lst)
        self.set_value(c.METEOBLUE_SECTION, meteoblue_sec_clear_lst)


if __name__ == "__main__":
    print("The config file contains the following properties:\n"
          "===================================================================================\n"
          "|| 1: input file directory - the input file path                                 ||\n"
          "|| 2: output file directory - the output file path                               ||\n"
          "|| 3: source data file name -  use this file to retrieve Meteoblue data          ||\n"
          "|| 4: sheet name - only if the source file is an Excel file                      ||\n"
          "|| 5: api key - to call Meteoblue REST API                                       ||\n"
          "|| 6: id column - the id column in your input file                               ||\n"
          "|| 7: latitude column - the latitude column in your input file                   ||\n"
          "|| 8: longitude column - the longitude column in your input file                 ||\n"
          "|| 9: country code column - the Alpha-2 country code                             ||\n"
          "|| 10: dates columns - comma separated list of dates columns in your input file  ||\n"
          "|| 11: start date offset - number of dates as offset to start date               ||\n"
          "|| 12: end date offset - number of dates as offset to end date                   ||\n"
          "|| exit: to exit the config file setup                                           ||\n"
          "|| clear: to clear up all the user properties                                  ||\n"
          "===================================================================================")

    filepath_sec_d: dict = {}
    meteoblue_sec_d: dict = {}
    config: ConfigUtil = ConfigUtil(c.INI_FILE)

    while True:
        o = input("Type an option from the above list, e.g. 1, 2, 12 or exit: ")
        if o == '1':
            filepath_sec_d.update({c.INPUT_FILE_DIR: input("Please type the input file directory: ")})
        elif o == '2':
            filepath_sec_d.update({c.OUTPUT_FILE_DIR: input("Please type the output file directory: ")})
        elif o == '3':
            filepath_sec_d.update({c.SOURCE_DATA_FILENAME: input("Please type the source data file name: ")})
        elif o == '4':
            filepath_sec_d.update({c.SHEET_NAME: input("Please type the sheet name if source file is Excel: ")})
        elif o == '5':
            meteoblue_sec_d.update({c.API_KEY: input("Please type the API key for Meteoblue API call: ")})
        elif o == '6':
            meteoblue_sec_d.update({c.ID_COL: input("Please type the ID column name: ")})
        elif o == '7':
            meteoblue_sec_d.update({c.LATITUDE_COL: input("Please type the latitude column name: ")})
        elif o == '8':
            meteoblue_sec_d.update({c.LONGITUDE_COL: input("Please type the longitude column name: ")})
        elif o == '9':
            meteoblue_sec_d.update({c.COUNTRY_CODE_COL: input("Please type the country code column name: ")})
        elif o == '10':
            meteoblue_sec_d.update({c.USER_INTERESTED_DATE_COLS: input(
                "Please type the comma separated list of dates columns: ")})
        elif o == '11':
            meteoblue_sec_d.update({c.START_DATE_OFFSET: input("Please type the start date offset: ")})
        elif o == '12':
            meteoblue_sec_d.update({c.END_DATE_OFFSET: input("Please type the end date offset: ")})
        elif o == 'exit':
            print("\nPlease review the input values before exiting: ")
            print(f'File path section: {filepath_sec_d}')
            print(f'Meteoblue section: {meteoblue_sec_d}')
            before_exit = input("\nTo reset a value please type an option again, to exit type 'y': ")
            if before_exit == 'y':
                break
        elif o == 'clear':
            print("\nRemoving all the user properties...")
            config.clear_value()

    if len(filepath_sec_d) > 0:
        config.set_value(c.FILE_PATHS_SECTION, filepath_sec_d)
    if len(meteoblue_sec_d) > 0:
        config.set_value(c.METEOBLUE_SECTION, meteoblue_sec_d)
