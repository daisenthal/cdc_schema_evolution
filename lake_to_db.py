import json
import subprocess
import snowflake.connector
from datetime import datetime
from pysqlake import cli


class Lake_To_Db:

    def __init__(self, UPSOLVER_TOKEN,GLUE,BIN_LOG_TABLE,LAKE_TABLE_LIST,COMPUTE_CLUSTER,INBOUND_UPSOLVER_DB_CONN,TABLE_INCLUDE_LIST,DATA_TYPES_FROM,OUTBOUND_DB):

        self.upsolver_token = UPSOLVER_TOKEN
        self.glue = GLUE
        self.bin_log_table = BIN_LOG_TABLE
        self.lake_table_list = LAKE_TABLE_LIST
        self.compute_cluster = COMPUTE_CLUSTER
        self.inbound_upsolver_db_conn = INBOUND_UPSOLVER_DB_CONN
        self.table_include_list = TABLE_INCLUDE_LIST
        self.data_types_from = DATA_TYPES_FROM
        self.outbound_db = OUTBOUND_DB
        
       
    def cli_run(self,cmd):
        return cli.run(cmd,self.upsolver_token)


    def existsTable(self,table_name):
        cmd = "SELECT count(1) as count FROM {GLUE_CATALOG}.information_schema.tables where table_schema = '{DB}' and table_name = '{TABLE_NAME}'".format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],TABLE_NAME=table_name) 
        output = self.cli_run(cmd)
        if output[0]:
            return True if int(output[1][0]["count"]) > 0 else False
        else:
            print(cmd)
            return False


    def returnTablesInBinLog(self)->str:
        cmd = 'select distinct "$table_name" as table_name from {GLUE_CATALOG}.{DB}.{BIN_LOG_TABLE}'.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],BIN_LOG_TABLE=self.bin_log_table)
        output = self.cli_run(cmd)
        return output[1]


    def dropJob(self,table)->bool:
        cmd = """ 
        DROP JOB {TABLE}_job 
        """.format(TABLE=table) 

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False


    def createOutboundJob(self,table,cols,outboundType,startFrom=None):
    
        START_FROM  = ""
        columns = ""
        if startFrom != None:
            START_FROM = "TIMESTAMP '{START_FROM}'".format(START_FROM=startFrom)
        else:
            START_FROM = "BEGINNING"

        for index, col in enumerate(cols):
            if col["column_name"] != "pid":
                columns = columns + "{COL} AS {COL_UPPER}, ".format(COL=col["column_name"],COL_UPPER=col["column_name"].upper())
        
        upsolver_output_type = "SNOWFLAKE" if outboundType == "SF" else "REDSHIFT"
        
        cmd = """
        
        CREATE SYNC JOB {JOB_PREFIX}_{TABLE}_job 
            COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}"
            START_FROM = {START_FROM}
        AS 
            MERGE INTO {OUTPUT_TYPE} {CONN}.{SCHEMA}.{TABLE} target USING 
            
                (SELECT 
                    {COLS} $primary_key::bigint as PID, $is_delete::boolean as IS_DELETE 
                FROM 
                    {GLUE_CATALOG}.{DB}.{BIN_LOG_TABLE}
                WHERE 
                    $event_time BETWEEN run_start_time() AND run_end_time() 
                    and $table_name = '{TABLE}') source 
                
                ON target.PID = source.pid 
            
            WHEN MATCHED AND source.IS_DELETE THEN DELETE 
            WHEN MATCHED THEN REPLACE 
            WHEN NOT MATCHED THEN INSERT MAP_COLUMNS_BY_NAME
                

        """.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],COMPUTE_CLUSTER=self.compute_cluster,START_FROM=START_FROM,COLS=columns,CONN=self.outbound_db["upsolver_conn"],SCHEMA=self.outbound_db["schema"],TABLE=table.upper(),BIN_LOG_TABLE=self.bin_log_table,OUTPUT_TYPE=upsolver_output_type,JOB_PREFIX=outboundType) 


        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False

    
    def createTable(self,table_name):
        cmd = """ 
        CREATE TABLE 
                {GLUE_CATALOG}.{DB}.{TABLE_NAME}(pid bigint)      
                PRIMARY KEY pid
        COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}" 
        """.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],TABLE_NAME=table_name,COMPUTE_CLUSTER=self.compute_cluster) 
        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False

    def outbound_db_connect(self,type):
        if type == "SF":
            ctx = snowflake.connector.connect(
            user=self.outbound_db["user"],
            password=self.outbound_db["pwd"],
            account=self.outbound_db["account"]
            )
            cs = ctx.cursor()
            cs.execute("USE WAREHOUSE {WAREHOUSE}".format(WAREHOUSE=self.outbound_db["warehouse"]))
            cs.execute("USE DATABASE {DATABASE}".format(DATABASE=self.outbound_db["db"]))
            cs.execute("USE SCHEMA {SCHEMA}".format(SCHEMA=self.outbound_db["schema"]))
            return cs
    


    def outbound_table_exists(self,cs,table_name,type):
        if type == 'SF':
            count = cs.execute("select count(1) from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '{SCHEMA}' and TABLE_NAME = '{TABLE_NAME}'".format(SCHEMA=self.outbound_db["schema"],TABLE_NAME=table_name.upper())).fetchone()

        return True if count[0] > 0 else False



    def outbound_table_col_exists(self, cs,outbound_type,table_name,col):
        count = cs.execute("select count(1) from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '{SCHEMA}' and TABLE_NAME = '{TABLE_NAME}' and COLUMN_NAME = '{COLUMN_NAME}'".format(SCHEMA=self.outbound_db["schema"],TABLE_NAME=table_name.upper(),COLUMN_NAME=col["column_name"].upper())).fetchone()
        return True if count[0] > 0 else False


    def getTableColumnNameAndTypes(self, table_name):
        cmd = "select column_name, data_type from {GLUE_CATALOG}.information_schema.columns where SUBSTR(column_name, 1, 1) <> '$' and TABLE_SCHEMA = '{SCHEMA}' and TABLE_NAME = '{TABLE_NAME}'".format(SCHEMA=self.glue["db"],TABLE_NAME=table_name,GLUE_CATALOG=self.glue["catalog"])
        output = self.cli_run(cmd)
        return output[1]

    def createAlterOutboundTable(self,cs,table_name,cols,outbound_type,ddl_type):
        cmd = ""
        columns = ""
        for index, col in enumerate(cols):
            columns = columns + "{COL} {DATA_TYPE}".format(COL=col["column_name"],DATA_TYPE=self.outbound_db["glue_catalog_mappings"][col["data_type"]])
            if index != len(cols) - 1:
                columns = columns + ", "
        if ddl_type =="CREATE":
            cmd = "CREATE OR REPLACE TABLE {TABLE} ({COLUMNS})".format(TABLE=table_name,COLUMNS=columns)
        else:
            cmd = "ALTER TABLE {TABLE} ADD {COLUMNS}".format(TABLE=table_name,COLUMNS=columns)
        cs.execute(cmd)
        if ddl_type =="CREATE":
            cmd = "ALTER TABLE {TABLE} ADD IS_DELETE boolean".format(TABLE=table_name,COLUMNS=columns)
            cs.execute(cmd)



    def getEarliestTime(self,table,col, earliestTime):

        cmd = 'select DATE_TRUNC(\'minute\',"$event_time")  as first_time from {GLUE_CATALOG}.{DB}.{TABLE} where {COL} is not null order by "$event_time" desc limit 1'.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],TABLE=table,COL=col["column_name"])
        output = self.cli_run(cmd)
        firstTime = datetime.strptime(output[1][0]["first_time"], '%Y-%m-%d %H:%M:%S.%f') 
        return firstTime if firstTime < earliestTime else earliestTime



    def process(self):

        # checks the binlog table and lake table and add outbound tables and jobs for each
        tables = self.returnTablesInBinLog()
        if len(tables) > 0 :
            # get conection to outbound db
            outbound_type = self.outbound_db["type"]
            cs = self.outbound_db_connect(outbound_type) 

            for item in tables:
                table = item["table_name"]

                # check exists in lake
                if self.existsTable(table):
            
                    # check if outbound table exists
                    OUTBOUND_TABLE_EXISTS = self.outbound_table_exists(cs,table,outbound_type)
                    
                    
                    # query information schema in Glue to get fields and data types
                    cols = self.getTableColumnNameAndTypes(table)
                    

                    if self.data_types_from == "DB":
                        # query information schema in DB to get true field data types, TD
                        continue

                    if not OUTBOUND_TABLE_EXISTS:
                        # create outbound table based on fields
                        self.createAlterOutboundTable(cs,table,cols,outbound_type,"CREATE")
                        # create new outbound job
                        self.createOutboundJob(table,cols,outbound_type,None)

                    if OUTBOUND_TABLE_EXISTS:
                        NEW_FIELDS = False
                        new_cols = []
                        earliestColTime = datetime.now()
                        # for each field check if exists in SF 
                        for col in cols:
                            if not self.outbound_table_col_exists(cs,outbound_type,table,col):
                                NEW_FIELDS = True
                                new_cols.append(col)

                                # if field does not exist find earliest entry 
                                earliestColTime = self.getEarliestTime(table,col,earliestColTime)

                        # alter table based on changed fields
                        if NEW_FIELDS:
                            self.createAlterOutboundTable(cs,table,new_cols,outbound_type,"ALTER")

                            # end old outbound job
                            self.dropJob("{OUTBOUND_TYPE}_{TABLE}".format(OUTBOUND_TYPE=outbound_type,TABLE=table))

                            # create new outbound job with earliest time
                            self.createOutboundJob(table,cols,outbound_type,earliestColTime)












