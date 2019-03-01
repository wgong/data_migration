# Table of Contents
1. [Introduction](README.md#introduction)
2. [Implementation](README.md#implementation-details)
3. [How to run](README.md#how-to-run)
4. [Test case](README.md#test-case)
5. [To Do](README.md#to-do)

# Introduction 

## Data Migration

Imagine a scenario where you were given the task create an ETL (Extract, Transform, Load) so that API d
ata is consumable by business analysts. Fortunately, your co-workers have already done the Extract step
 and has provided you with a .zip file containing retail order data in the raw JSON format. Your projec
t manager has put you on the task to support these business analysts so that they can query that data u
sing SQL from a PSQL database.
While you’re at it, they would also want you to create a user table that would contain summary metrics 
that you think business analysts would find useful.  
**Note:** Keep in mind that the newly created tables have to be sanely structured and those steps shoul
d be reproducible with the expectation that the **ETL would run daily**.

# Implementation

* Install PostgreSQL
* Analyzed .json file and decided to create two tables: `dm_orders`, `dm_line_items`.

  DDL script is found at `./scripts/data_migration.sql`
* write python program to parse .json file and load data into above 2 tables. 

  For good performance, we use `psycopg2.extras.execute_values` API for batch insert.
  Code is at `./src/load_json_data.py`

# How to run
see `run.sh` shell script, e.g.

```
$ python ./src/load_json_data.py -i ./input/2017-11-10.json -p ./input/batch_size.cfg 

Processed (802, 2010) rows in 0.085 sec

BATCH_SIZE=200:
Processed 25000 orders / 60555 line_items in 2.112 sec

BATCH_SIZE=800:
Processed 25000 orders / 60555 line_items in 2.157 sec

```

# Test case

# To Do

## Import CSV file into table using COPY statement  
`http://www.postgresqltutorial.com/import-csv-file-into-posgresql-table/`

* write a program to convert json to csv files for orders and line_items
or use online tool such as https://json-csv.com/
* run COPY statement

# Credit

this repo is based on https://github.com/Samariya57/coding_challenges.git
