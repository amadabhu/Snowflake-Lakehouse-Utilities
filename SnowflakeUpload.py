import pandas as pd
import snowflake.connector as snow
import os
from snowflake.connector.pandas_tools import write_pandas
import csv
import datetime as dt

#Change these to match your credentials

#Username = os.environ['SNOWFLAKEUSER']
#Password = os.environ['PASSWORD']
#Account = os.environ['ACCOUNT']
#Warehouse = os.environ['WH']
#Database = os.environ['DB']
#Schema =os.environ['Schema']
#Role =os.environ['Role']


Class SnowflakeUpload:

    def snowflakeconn():
        conn = snow.connect(
            user=Username,
            password=Password,
            account=Account,
            warehouse=Warehouse,
            database=Database,
            schema=Schema,
            role=Role
        )
        return conn
    
    #Adds timestamp to csv files
    def add_timestamp():
        filepath = r'Insert Path Here'
        for root, dirs, files in os.walk(filepath, topdown=False):
            for item in files:
                temp = os.path.join(filepath,item)
                df = pd.read_csv(temp)
                df['DATA_WAREHOUSE_INSERT_DATE'] = dt.datetime.now()
                df.to_csv(temp,index=False)
        print("Timestamps added")
        return None
    
    #upload a single file
    def uploadsingle(conn):
        directory = r'Insert Path here'
            #str(input("What is the filepath for the input files? Make sure there are only files and not folders in this directory"))
        print("entered function")
        cur = conn.cursor()
        role = "USE ROLE --Insert Role here --"
        cur.execute(role)
        print("executed role")
        #file in stage

        sql = r'put file://'
        filepath = directory
        linuxsyntax = r" @~/"

        for root,dirs,files in os.walk(filepath, topdown=False):

            for item in files:
                temppath = os.path.join(filepath, item)
                statement = sql+temppath+linuxsyntax
                print(f'Uploading ({item} to stage ...', end='')
                print(statement)
                cur.execute(statement)
                print('. Done.')

        #file into table
        #statement = r'copy into "DATABASE"."SCHEMA"."TABLE_NAME" from @~/zip-file-gz-name-here FILE_FORMAT=(FORMAT_NAME=insert_format_name_here) ON_ERROR=CONTINUE'
        return filepath

    #insert files from stage
    def insertfromstaging(conn):
        cur = conn.cursor()
        role = "USE ROLE INSERT_ROLE_NAME_HERE"
        cur.execute(role)
        list_statements = [('"DATABASE"."SCHEMA"."TABLE_NAME_1"',"list @~/ pattern='Finance.*';"),
                           ('"DATABASE"."SCHEMA"."TABLE_NAME_2"',"list @~/ pattern='Financial.*';"),
                           ('"DATABASE"."SCHEMA"."TABLE_NAME_3"',"list @~/ pattern='Map.*';"),
                           ('"DATABASE"."SCHEMA"."TABLE_NAME_4"',"list @~/ pattern='Segment.*';")]
        for ls in list_statements:
            cur.execute(ls[1])
            rows = 0
            while True:
                result = cur.fetchmany(50000)
                if not result:
                    break
                df = pd.DataFrame(result, columns=cur.description)
                rows += df.shape[0]
                print(f'\rRetrieving filelist...{rows}', end='')
            print('. Done.')
            file_counter = 0
            for row in df.iloc:
                file_counter += 1
                with open('log.txt', 'a') as f_out:
                    f_out.write(f'{dt.datetime.now()}\t{ls[0]}\t{ls[1]}\t{row[0]}\n')
                print(f'Inserting ({file_counter} of {rows}) {row[0]}...', end='')
                copy_statement = f'copy into {ls[0]} from @~/{row[0]} FILE_FORMAT = (TYPE = "CSV", COMPRESSION = "AUTO", SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = \'"\')'
                cur.execute(copy_statement)
                print('Done.')
        return None

    def remove_cols():
        filepath = str(input("What is the filepath for the input files? Make sure there are only files and not folders in this directory"))
        for root, dirs, files in os.walk(filepath, topdown=False):
            for item in files:
                temp = os.path.join(filepath, item)
                df = pd.read_csv(temp)
                try:
                    df = df.drop(labels=['DATA_WAREHOUSE_INSERT_DATE'], axis=1)
                    df.to_csv(temp, index=False, quoting=csv.QUOTE_NONNUMERIC)
                except KeyError:
                    print(f'This file {item} does not have the necessary columns')

        return filepath

    def get_file_from_stage(conn_object):
        cur = conn_object.cursor()
        file = ' file://'
        filepath = r'INSERT FILE PATH HERE'
        ls_names = ['data_0_0_0.csv.gz', 'data_0_1_0.csv.gz', 'data_0_2_0.csv.gz', 'data_0_3_0.csv.gz', 'data_0_4_0.csv.gz', 'data_0_5_0.csv.gz', 'data_0_6_0.csv.gz', 'data_0_7_0.csv.gz']
        get = "get @~/"
        for item in ls_names:
            statement = get+item+file+filepath
            print(f'Downloading ({item} from stage ...', end='')
            print(statement)
            cur.execute(statement)
            print('. Done.')

        return None

    def snowflake():

        conn = snowflakeconn()
        #filepath = remove_cols()
        add_timestamp()
        uploadsingle(conn)
        insertfromstaging(conn)
        #get_file_from_stage(conn)

        return None
