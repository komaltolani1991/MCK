import os
import json
import mysql.connector
import pandas as pd
from pyspark.sql.types import StructType
from pyspark.sql.types import *
import com.mck.grip.exception.exception as s
from pyspark.sql import *
from pyspark.sql.session import SparkSession
from sqlalchemy import create_engine

spark = SparkSession.builder.appName("Rules").getOrCreate()

class Helperclass(object):

    def __init__(self, Input_table, Output_table, year, mydb):
        self.Input_table = Input_table
        self.Output_table = Output_table
        self.year = year
        self.mydb=mydb

    def connectionpool(self):

        try:
            config_data = read_json_file(
                'C:\\Users\\HArsh\\Desktop\\workspace\\com\\mck\\grip\\helper\\properties.json')
            if config_data is None:
                raise Exception("Please provide proper MSSQL configuration file")

            host = config_data['host']
            username = config_data['username']
            password = config_data['passwd']
            database = config_data['database']
            self.mydb = mysql.connector.connect(host=host, user=username, passwd=password, database=database)
            print("Connection Successful")
            return self
        except:
            s.DataBaseException()

    def get_values(self):
        return self

    def createDataFrame(self):

        sqlquery_rule_master = pd.read_sql_query('select * from rule_master',
                                                self.mydb)  ## takes all the records from master table
        df_rule_master = pd.DataFrame(sqlquery_rule_master,
                                      columns=['job_id', 'year', 'modulename', 'rule_step', 'Input_table',
                                               'output_table', 'persist', 'status', 'start_time',
                                               'end_time'])  ##create dataframe for records

        p_schema = StructType([StructField('job_id', IntegerType(), True),
                               StructField('year', IntegerType(), True),
                               StructField('modulename', StringType(), True),
                               StructField('rule_step', IntegerType(), True),
                               StructField('Input_table', StringType(), True),
                               StructField('output_table', StringType(), True),
                               StructField('persist', StringType(), True),
                               StructField('status', StringType(), True),
                               StructField('start_time', TimestampType(), True),
                               StructField('end_time', TimestampType(), True)
                               ])
        df_master = spark.createDataFrame(df_rule_master, schema=p_schema)

        sqlquery_rule_detail = pd.read_sql_query('select * from rule_detail',
                                                self.mydb)  ## takes all the records for the detail table
        df_rule_detail = pd.DataFrame(sqlquery_rule_detail, columns=['modulename', 'rulename', 'rule_text',
                                                                     'param'])  ## create dataframe for the detail's records

        p_schema = StructType([StructField('modulename', StringType(), True),
                               StructField('rulename', StringType(), True),
                               StructField('rule_text', StringType(), True),
                               StructField('param', StringType(), True)
                               ])

        df_detail = spark.createDataFrame(df_rule_detail, schema=p_schema)

        mt = df_master.alias('mt')
        dt = df_detail.alias('dt')

        df_result = mt.join(dt, mt.modulename == dt.modulename)

        Input_table = df_result.select("Input_table").collect()[0][0]
        Output_table = df_result.select("output_table").collect()[0][0]
        year = df_result.select("year").collect()[0][0]
        obj2 = Helperclass(Input_table, Output_table, year,self.mydb)
        return obj2

def read_json_file(in_directory_file_name):
        if os.path.isfile(in_directory_file_name):
            json_file = open(in_directory_file_name, 'r')
            table_data = json.loads(json_file.read())
            json_file.close()

            return table_data
        else:
            return None

