import json
import subprocess
import snowflake.connector
from datetime import datetime
from pysqlake import cli


#can be improved to create the database basd upon the source db as opposed to placing all tables in 1 db
class Db_To_Lake:

    
    def __init__(self, UPSOLVER_TOKEN,GLUE,BIN_LOG_TABLE,COMPUTE_CLUSTER,INBOUND_UPSOLVER_DB_CONN,TABLE_INCLUDE_LIST):

        self.upsolver_token = UPSOLVER_TOKEN
        self.glue = GLUE
        self.bin_log_table = BIN_LOG_TABLE
        self.compute_cluster = COMPUTE_CLUSTER
        self.db_conn = INBOUND_UPSOLVER_DB_CONN
        self.table_include_list = TABLE_INCLUDE_LIST

    def cli_run(self,cmd):
        return cli.run(cmd,self.upsolver_token)


    def existsTable(self,table_name):
        cmd = """
        SELECT count(1) as count FROM {GLUE_CATALOG}.information_schema.tables where table_schema = '{DB}' 
        and table_name = '{TABLE_NAME}'
        """.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],TABLE_NAME=table_name) 
        output = self.cli_run(cmd)
        if output[0]:
            return True if int(output[1][0]["count"]) > 0 else False
        else:
            print(cmd)
            return False


    def returnTablesInBinLog(self):
        cmd = 'select distinct "$table_name" as table_name from {GLUE_CATALOG}.{DB}.{BIN_LOG_TABLE}'.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],BIN_LOG_TABLE=self.bin_log_table)
        output = self.cli_run(cmd)
    
        return output[1]


    def dropJob(self,table):
        cmd = """ 
        DROP JOB {TABLE}_job 
        """.format(TABLE=table) 

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False

    def dropTable(self,table):

        cmd = """ 
        DROP TABLE {GLUE_CATALOG}.{DB}.{TABLE}
        DELETE_DATA = true
        COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}" 
        """.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],TABLE=table,COMPUTE_CLUSTER=self.compute_cluster) 

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False

    def createBinLogTable(self):
        cmd = """ 
        CREATE TABLE 
                {GLUE_CATALOG}.{DB}.{BIN_LOG_TABLE}($table_name string) 
                PARTITIONED BY $table_name
    
        COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}" 
        """.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],BIN_LOG_TABLE=self.bin_log_table,COMPUTE_CLUSTER=self.compute_cluster) 

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False


    def createBinLogJob(self):

        cmd = """
        
        CREATE SYNC JOB {BIN_LOG_TABLE}_job 
            COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}"
        AS 
            COPY FROM MYSQL {MYSQL_CONN}
            TABLE_INCLUDE_LIST = {TABLE_INCLUDE_LIST}
            INTO {GLUE_CATALOG}.{DB}.{BIN_LOG_TABLE}

        """.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],BIN_LOG_TABLE=self.bin_log_table,COMPUTE_CLUSTER=self.compute_cluster,MYSQL_CONN=self.db_conn,TABLE_INCLUDE_LIST=self.table_include_list) 

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False

    def alterBinLogJob(self):

        cmd = """
        
        ALTER JOB {BIN_LOG_TABLE}_job 
        SET TABLE_INCLUDE_LIST = {TABLE_INCLUDE_LIST}

        """.format(TABLE_INCLUDE_LIST=self.table_include_list,BIN_LOG_TABLE=self.bin_log_table) 

        output = self.cli_run(cmd)
        if output[0]:
            return True
        else:
            print(cmd)
            return False


    def createTableJob(self,table):
            
        cmd = """
        
        CREATE SYNC JOB {table}_job 
            COMPUTE_CLUSTER = "{COMPUTE_CLUSTER}"
            ADD_MISSING_COLUMNS = TRUE 
            START_FROM = BEGINNING
        AS 
            MERGE INTO {GLUE_CATALOG}.{DB}.{table} target USING 
            
                (SELECT 
                    *, $primary_key::bigint as pid,$is_delete::boolean as is_delete 
                FROM 
                    {GLUE_CATALOG}.{DB}.{BIN_LOG_TABLE}
                WHERE 
                    $event_time BETWEEN run_start_time() AND run_end_time() 
                    and $table_name = '{table}') source 
                
                ON target.pid = source.pid 
            
            WHEN MATCHED AND source.is_delete THEN DELETE 
            WHEN MATCHED THEN REPLACE 
            WHEN NOT MATCHED THEN INSERT MAP_COLUMNS_BY_NAME EXCEPT source.is_delete

        """.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],BIN_LOG_TABLE=self.bin_log_table,COMPUTE_CLUSTER=self.compute_cluster,table=table) 

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


    def process(self):
        # creates bin log table if does not exist
        if not self.existsTable(self.bin_log_table):
            self.createBinLogTable()
            self.createBinLogJob()
        else:
            # pick up any changes to table list
            self.alterBinLogJob()

        # checks the binlog table for new mysql tables and adds tables and jobs for each
        tables = self.returnTablesInBinLog()
        if len(tables) > 0:
            for item in tables :
                table = item["table_name"]
                if not self.existsTable(table):
                    self.createTable(table)
                    self.createTableJob(table)

