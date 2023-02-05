# cdc-mysql-upsolver
Reads from mysql or postgres database and replicates to Upsolver Data Lake.
Pushes out to SnowFlake 

## main.py
Main module with example data 
Passwords for OUTBOUND_DB_PASSWORD and UPSOLVER_TOKEN should be placed in .env file

Expected to already have created the following Upsolver connections
1. Glue Catalog Connection (GLUE - catalog)
2. MySQL or PostGres connection for ingest (INBOUND_UPSOLVER_DB_CONN)
3. Optional SnowFlake connection for output (OUTBOUND_DB - upsolver_conn)

## clean.py
Optionally called from main.py to drop old jobs and tables

## db_to_lake.py
Replicates db tables to Upsolver Lake tables from MySQL or PostGres
Supports schema evolution on adding new columns. Updated data type columns  will cause a change to the Upsolver columns to get the most general type
Deleted columns will not effect the Data Lake tables


## lake_to_db.py
Replicates Upsolver Lake tables to SnowFlake
Supports schema evolution on adding new columns.
Does not support changes to data type

## Requirements
pysqlake - (Reads Upsolver CLI)
snowflake-connector-python - (Optional to replicate to SnowFlake)
python-dotenv - for passport hiding support

## Running as Job / on Push
Includes Github action to show how script can be scheduled or triggered on action
