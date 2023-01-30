# Spotify-Api
Creating a Spotify API.

## Project Goal
- The goal of the project is to create a Spotify API ETL job and schedule it using Apache Airflow.

## Overview
There are a few steps to the process, so before we dive in let’s first summarize what we need to do. In total, there are four key parts:
- Extracting The Data
- Transforming The Data
- Loading The Data
- Run Spotify ETL Job using Apache Airflow

## Extracting The Data
- Extracted data from Spotify API using Python, pulled the data from SPOTIFY API in JSON format, and converted it into a data frame.
## Transforming The Data
- Transformed the data by creating a function that imposes a primary key constraint that checks that the data being pulled is not empty, has a primary key, and is not null.
## Loaded The Data
- Loaded the data into a database administration tool (dbeaver).
## Apache Airflow
- Scheduled the ETL job in Apache Airflow.
