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
    version="0.1.0",
    description="Meteoblue environmental data extractor",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Vivian Lee",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords=["weather data", "soil data"],
    packages=["src"],
    include_package_data=True,
    package_data={"src/config": ["*.ini", "*.json"]},
    install_requires=[
        "pathlib",
        "pandas",
        "json",
        "configparser",
        "configupdater",
        "meteoblue_dataset_sdk"]
)
