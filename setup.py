# Databricks notebook source
from setuptools import find_packages
from setuptools import setup

with open("requirement.txt", "r") as f:
    requires = [
        line.strip() for line in f.read().splitlines()
        if not line.startswith("#") and not line.startswith("-") and not line.startswith("--")

    ]
setup(
    name = 'satish_package',
    version = '0.1',
    packages=find_packages(),
    install_requires=requires,
    python_requires='>=3.7.0',
    include_package_data=True
)

# COMMAND ----------


