import os
from db_to_lake import *
from cleanup import *
from lake_to_db import *
from dotenv import load_dotenv
load_dotenv()

UPSOLVER_TOKEN = os.getenv("UPSOLVER_TOKEN")


# connection definitions
GLUE = {
    "catalog":"my_glue_catalog_connection_david",
    "db":"david"
}

COMPUTE_CLUSTER = "sqlake"
INBOUND_UPSOLVER_DB_CONN = "mysql_cdc_david"
TABLE_INCLUDE_LIST = "('stock.test1','stock.test2','stock.rating')"
# currently only supported can also read from incoming db
DATA_TYPES_FROM = "GLUE_CATALOG"
# used to discover tables for cdc
BIN_LOG_TABLE = "mysql_binlog"
# used for non cdc - no way to discover tables
LAKE_TABLE_LIST = ['a','b','c']
 
OUTBOUND_DB = {
    "type":"SF",
    "upsolver_conn":"david_sf_conn",
    "user":"david",
    "account":"baa55269.us-east-1",
    "warehouse":"COMPUTE_WH",
    "db":"DEMO_DB",
    "schema":"DAVID",
    "glue_catalog_mappings" : {
        "bigint":"bigint",
        "varchar":"varchar",
        "date":"date",
        "boolean":"boolean",
        "timestamp":"timestamp",
        "double":"double"
    }
}

OUTBOUND_DB["pwd"] = os.getenv("OUTBOUND_DB_PASSWORD")

# clean up by dropping tables and jobs including in outbound db
def cleanup(withOutput:bool):
    output = OUTBOUND_DB if withOutput else None
    cleanup_handler = Cleanup(UPSOLVER_TOKEN,GLUE,BIN_LOG_TABLE,COMPUTE_CLUSTER,output)
    cleanup_handler.process()

# writes from mysql or postgres into upsolver lake, breaks up to individual tables by querying the bin log $table_name
def db_to_lake():
    inbound_handler = Db_To_Lake(UPSOLVER_TOKEN,GLUE,BIN_LOG_TABLE,COMPUTE_CLUSTER,INBOUND_UPSOLVER_DB_CONN,TABLE_INCLUDE_LIST)
    inbound_handler.process()

# replicates lake tables to snowflake or redshift supporting schema evolution -
#  uses the information schema of the lake tables to define the schema in the database using a data type mapping
#  checks to see if new columns added and adds the new columns to db and replays the pipeline from the first time 
# the new colmnn appears by dropping and recreating job
# if cdc will automatically discover new tables but for other inputs (e.g. Kafka) expects a list of tables

def lake_to_db(isCDC:bool):
    if isCDC:
        outbound_handler = Lake_To_Db(UPSOLVER_TOKEN,GLUE,BIN_LOG_TABLE,None,COMPUTE_CLUSTER,INBOUND_UPSOLVER_DB_CONN,TABLE_INCLUDE_LIST,DATA_TYPES_FROM,OUTBOUND_DB)
    else:
        outbound_handler = Lake_To_Db(UPSOLVER_TOKEN,GLUE,None,LAKE_TABLE_LIST,COMPUTE_CLUSTER,INBOUND_UPSOLVER_DB_CONN,TABLE_INCLUDE_LIST,DATA_TYPES_FROM,OUTBOUND_DB)
    
    outbound_handler.process()

def main():
    cleanup(withOutput=True)
    db_to_lake()
    lake_to_db(isCDC=True)




if __name__ == "__main__":
    main()