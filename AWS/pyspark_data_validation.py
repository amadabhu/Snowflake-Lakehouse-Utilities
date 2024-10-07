# Importing necessary libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import numpy as np
from pyspark.sql.functions import col,isnan,when,count
from pyspark.sql import functions as F
import threading
import pandas as pd
from awsglue.dynamicframe import DynamicFrame
from functools import reduce
import re
import time
from datetime import datetime
from pyspark import SparkConf
import boto3

class MetricsComparator:
    def __init__(self):

        ## @params: [JOB_NAME]
        args = getResolvedOptions(sys.argv, ['JOB_NAME','input_path','output_bucket','tables_list'])

        # Spark Context and Glue Context initialization
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)
        job.commit()
        current_date = datetime.today().strftime('%Y/%m/%d')
        tables = args['tables_list'].split(',')

        def create_dataframes():
            """
            Function to create source and target dataframes.
            It pulls data from AWS S3 paths and converts them into DataFrames.
            
            Returns:
            source_df: DataFrame - Source data from S3
            target_df: DataFrame - Target data from S3
            """

            source_input_path = args['input_path'] + "source/" + current_date
            target_input_path = args['input_path'] + "target/" + current_date
            
            source_df = glueContext.create_dynamic_frame_from_options(
                connection_type="s3",
                connection_options={"paths": [source_input_path], "recurse": True},
                format = "csv",
                format_options= {'withHeader': True}
                ).toDF()
                
            # source_df.show(50)
                    
            target_df = glueContext.create_dynamic_frame_from_options(
                connection_type="s3",
                connection_options={"paths": [target_input_path], "recurse": True},
                format = "csv",
                format_options= {'withHeader': True}
                ).toDF()
                
            # target_df.show(50)
            
            print("souce and target dataframes created")
            return source_df, target_df

        def compare_dataframes(src_df, tgt_df):
            """
            Function to compare source and target dataframes.
            
            Parameters:
            src_df: DataFrame - Source dataframe
            tgt_df: DataFrame - Target dataframe
            
            Returns:
            comp_df: DataFrame - Comparison results between source and target
            """

            current_date_format = current_date.replace('/','-')
            validation_list = []
            for t in tables:
                comp_vals = []
                src = src_df.filter(src_df["table_name"] == t).collect()
                tgt = tgt_df.filter(tgt_df["table_name"] == t).collect()
                
                status_str = ""

                # Perform comparison checks
                comp_vals.append(t)
                comp_vals.append(current_date_format)
                if src[0]["has_nulls"] == tgt[0]["has_nulls"]:
                    comp_vals.append(True)
                else:
                    comp_vals.append(False)
                    status_str += f'src has nulls is {src[0]["has_nulls"]} and tgt has nulls is {tgt[0]["has_nulls"]} | '
                    
                if src[0]["has_empty"] == tgt[0]["has_empty"]:
                    comp_vals.append(True)
                else:
                    comp_vals.append(False)
                    status_str += f'src has empty strings is {src[0]["has_empty"]} and tgt has empty strings is {tgt[0]["has_empty"]} | '
                
                if src[0]["row_count"] == tgt[0]["row_count"]:
                    comp_vals.append(True)
                else:
                    comp_vals.append(False)
                    status_str += f'src row count is {src[0]["row_count"]} and tgt row count is {tgt[0]["row_count"]} | '
                
                if src[0]["column_count"] == tgt[0]["column_count"]:
                    comp_vals.append(True)
                else:
                    comp_vals.append(False)
                    status_str += f'src column count is {src[0]["column_count"]} and tgt column count is {tgt[0]["column_count"]}'
                
                src_col_data_types = src[0]["col_data_type_lists"].split(",")  # .replace('[', '').replace(']', '').split(',')
                tgt_col_data_types = tgt[0]["col_data_type_lists"].replace('[', '').replace(']', '').replace('\'','').replace(' ','').split(',')
                
                print("table: ", t)
                print(src_col_data_types)
                print()
                print(tgt_col_data_types)

                col_type_match = True
                for i in range(len(src_col_data_types)):
                    if src_col_data_types[i] == 'int' and tgt_col_data_types[i] != 'int':
                        status_str = 'col type mismatch between int |'
                        col_type_match = False
                    if src_col_data_types[i] == 'decimal' and tgt_col_data_types[i] != 'decimal':
                        status_str = 'col type mismatch between decimal |'
                        col_type_match = False
                    if src_col_data_types[i] in ['char','varchar','nvarchar','uniqueidentifier'] and tgt_col_data_types[i] != 'string':
                        status_str = 'col type mismatch between char/varchar/nvarchar/uniqueidentifier |'
                        col_type_match = False
                    if src_col_data_types[i] == 'datetime' and tgt_col_data_types[i] != 'timestamp':         
                        status_str = 'col type mismatch between datetime |'
                        col_type_match = False
                    if src_col_data_types[i] in ['tinyint','smallint'] and tgt_col_data_types[i] != 'smallint':
                        status_str = 'col type mismatch between tinyint/smallint |'
                        col_type_match = False
                    if src_col_data_types[i] == 'bit' and tgt_col_data_types[i] != 'boolean':
                        status_str = 'col type mismatch between bit |'
                        col_type_match = False
                
                comp_vals.append(col_type_match)
                comp_vals.append(status_str)
                
                validation_list.append(comp_vals)
            
            print(validation_list)
            return pd.DataFrame(validation_list, columns=['table_name','date','null_check','empty_check','row_count_check','column_count_check','col_type_check','information'])

        def write_to_s3(df,output_bucket):
            """
            Function to write DataFrame to AWS S3.
            
            Parameters:
            df: DataFrame - DataFrame to be written to S3
            output_bucket: str - Path to the output location in S3
            """
            bucket_prefix = 'validation/' + current_date # datetime.today().strftime('%Y/%m/%d/')
            
            output_df_spk = spark.createDataFrame(df) # convert to spark
            output_df_dyn = DynamicFrame.fromDF(output_df_spk, glueContext, "dym_frame").repartition(1) # convert to dynamicframe

            s3_client = boto3.client('s3')
            
            response = s3_client.list_objects_v2(Bucket=output_bucket, Prefix=bucket_prefix)
            print("response: ", response)
            try:
                for object in response['Contents']:
                    print('Deleting', object['Key'])
                    s3_client.delete_object(Bucket=output_bucket, Key=object['Key'])
            except:
                print("Exception occured in clearing target folder location")
                
            # write to output bucket
            glueContext.write_dynamic_frame.from_options(
            frame = output_df_dyn,
            connection_options = {'path': 's3://{}/{}'.format(output_bucket,bucket_prefix)},
            connection_type = 's3',
            format = 'csv')
            print("compare table written to s3")

        
        # main
        source_df, target_df = create_dataframes()
        comp_df = compare_dataframes(source_df, target_df)
        write_to_s3(comp_df, args['output_bucket'])


