# AZ case study

## Requirements

Docker:
* CPUs: 2
* Memory: 6GB
* Swap: 2GB

## Introduction 

The case study is about creating a simple pipeline to store a subset of the Statsbomb open data. 
Below are the subsets that have been used for this work :
* Ligue 1 season 2021/2022
* Ligue 1 season 2022/2023 
* Africa Cup of Nations 2023

The repository contains the following files: 
* A Dockerfile to run the ETL coded in python
* A dockercompose.yml file to create a database instance, run the ETL and access the database from the browser
* A folder with the following scripts : 
	* config.py : variables of the project (variables related to the data subset, the database, the credentials, …)
	* utils.py : various code snippets 
	* extract_data.py :  to download the raw data from Statsbomb and save it to a folder named « raw data »
	* transform_data.py : to transform the raw data into clean data
	* load_data.py : to load the data into the relational database
	* sql_queries.py : SQL queries to create the tables
	* etl.py : the main file to run the ETL

## Usage

To run the code of this project (daemon mode) : 
`docker compose up -d`

To access the database in the browser : 
`localhost:8080`

- Server : db
- Username : root
- Password : changethis!

The etl.py has one argument with 2 possible values :
- `python3 etl.py --no-update` to load the data the first time
- `python3 etl.py --update` to update the database 

## Further Improvements (that were not implemented)

* Add the unit tests
* Replace the requirements.txt by a poetry package
