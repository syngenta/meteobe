from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

# The directory containing this file
HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="meteobe",
    version="0.0.1",
    packages=["src"],
    include_package_data=True,
    package_data={"src/config": ["*.ini", "*.json"]},
    install_requires=[
        "pathlib",
        "pandas",
        "json",
        "configparser",
        "meteoblue_dataset_sdk"]
    )
