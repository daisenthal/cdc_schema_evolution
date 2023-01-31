import json
import subprocess
import snowflake.connector
from datetime import datetime
from pysqlake import cli


class Cleanup:

    
    def __init__(self, UPSOLVER_TOKEN,GLUE,BIN_LOG_TABLE,COMPUTE_CLUSTER,OUTBOUND_DB):

        self.upsolver_token = UPSOLVER_TOKEN
        self.glue = GLUE
        self.bin_log_table = BIN_LOG_TABLE
        self.compute_cluster = COMPUTE_CLUSTER
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


    def returnTablesInBinLog(self):
        cmd = 'select distinct "$table_name" as table_name from {GLUE_CATALOG}.{DB}.{BIN_LOG_TABLE}'.format(GLUE_CATALOG=self.glue["catalog"],DB=self.glue["db"],BIN_LOG_TABLE=self.bin_log_table)
        output = self.cli_run(cmd)
        return output


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

    
    def drop_outbound_table(self, cs, table_name,outbound_type):
        if outbound_type == "SF":
            cs.execute("DROP TABLE IF EXISTS {TABLE_NAME}".format(TABLE_NAME=table_name.upper()))


    def process(self):
    #   drops jobs and tables including tables in db
        if self.existsTable(self.bin_log_table):
            tables = self.returnTablesInBinLog()
            for item in tables[1]:
                table = item["table_name"]
                if self.existsTable(table):
                    self.dropJob(table)
                    self.dropTable(table)
                if self.outbound_db != None:
                    outbound_type = self.outbound_db["type"]
                    self.dropJob("{TYPE}_{TABLE}".format(TYPE=outbound_type,TABLE=table))
                    self.drop_outbound_table(self.outbound_db_connect(outbound_type),table,outbound_type)
        
            self.dropJob(self.bin_log_table)
            self.dropTable(self.bin_log_table)

       

