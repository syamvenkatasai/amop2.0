"""
implemented scheduler
no option of creating a job from code
just loading the jobs direclty from database and running them
last_id list update done
isert+ update enables
and insert retries also done
"""

import os
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd

from asyncio.windows_events import NULL
import logging
import time
from sqlalchemy import create_engine, exc,text
import pyodbc
import time
import psycopg2
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv
import os
import re
from pandas import Timestamp
import json
from datetime import datetime, timedelta
import threading
import time
from psycopg2.extras import execute_values
import math
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(filename='db_migration.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s:%(message)s')

class MigrationScheduler:
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def create_connection(self,db_type='',host='', db_name='',username='', password='',port='',driver='',max_retry=3):
        connection = None
        retry = 1       
        print(f"db_type:{db_type}, host--{host}-db_name-{db_name}, username-{username},password-{password},port-{port},driver-{driver}")
        
        if db_type=='postgresql':            
            try:
                print(f"creating postgresql connection")
                
                connection = psycopg2.connect(
                    host=host,
                    database=db_name,
                    user=username,
                    password=password,
                    port=port
                )
                print("Connection to PostgreSQL DB successful")
            except Exception as e:
                logging.error(f"Failed to connect to PostgreSQL DB: {e}")
        elif db_type=='mssql':
            try:
                connection_string= f"""DRIVER={driver};SERVER={host};DATABASE={db_name};UID={username};PWD={password};"""
                connection = pyodbc.connect(connection_string)
                print("Connection to MSSQL successful!")
            except Exception as e:
                logging.error(f"Failed to connect to MSSQL DB: {e}")
        return connection
    
    def execute_query(self,connection,query):  
    
        try:
            result_df = pd.read_sql(query, connection)
            return result_df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None # Placeholder return value
        
    def __init__(self):
        load_dotenv()  # Load environment variables from .env file
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    
        
    # def schedule_periodic_check(self, postgres_conn, migration_table):
    #     self.scheduler.add_job(self.check_and_schedule_jobs, 'interval', hours=12, args=[postgres_conn, migration_table])
        
    def main(self):
        print("Beginning migration")
        
        db_type = os.getenv('LOCAL_DB_TYPE')
        hostname = os.getenv('LOCAL_DB_HOST')
        port = os.getenv('LOCAL_DB_PORT')
        db_name = 'Migration_Test'
        user = os.getenv('LOCAL_DB_USER')
        password = os.getenv('LOCAL_DB_PASSWORD')
        migration_table = os.getenv('MIGRATION_TABLE')
        
        postgres_conn = self.create_connection(db_type, hostname, db_name, user, password, port)
        
        # If the connection is successful, schedule periodic checks for the migration table
        if postgres_conn:
            self.schedule_periodic_check(postgres_conn, migration_table)

    def schedule_periodic_check(self, postgres_conn, migration_table):
        """
            Schedules periodic checks and execution of migration jobs based on the data from the migration table.
            
            This function retrieves the details of migration jobs from the specified table and schedules them to run
            either immediately or at specified future times. The scheduling is based on the `start_time`, `days_to_run`, 
            and `time_flag` values for each job. If `time_flag` is not set, the job runs immediately. Jobs can be scheduled 
            to run once or repeatedly for a specified number of days.
            
            Args:
               - postgres_conn (psycopg2.connection): The connection object for the PostgreSQL database.
               - migration_table (str): The name of the table containing migration job details.
        """
        query = f"SELECT migration_name, start_time, days_to_run,time_flag FROM {migration_table}"
        rows = self.execute_query(postgres_conn, query)
        print(f"Rows fetched: {rows}")
        
        now = datetime.now()
        for index, row in rows.iterrows():
            job_name = row['migration_name']
            start_time = row['start_time']
            days_to_run = row['days_to_run']
            time_flag=row['time_flag']

            # If the time_flag is not set, run the main migration function immediately
            if not time_flag and not start_time:
                result=self.main_migration_func(job_name,postgres_conn)
                if result:
                    rows.drop(index, inplace=True)
                continue

            # If the job should run only once (days_to_run is 0)
            if days_to_run == 0:
                start_datetime = datetime.combine(now.date(), start_time)
                if start_datetime > now:  # Only schedule future jobs
                    start_time_12hr = start_datetime.strftime("%I:%M:%S %p")
                    print(f"Scheduled '{job_name}' at: {start_time_12hr} on {start_datetime.date()}")
                    self.scheduler.add_job(self.main_migration_func, 'date', run_date=start_datetime, args=[job_name, postgres_conn])
            else:
                # If the job should run for multiple days
                for day in range(1, days_to_run + 1):  # Corrected range to include the last day
                    start_datetime = datetime.combine(now + timedelta(days=day), start_time)
                    if start_datetime > now:  # Only schedule future jobs
                        start_time_12hr = start_datetime.strftime("%I:%M:%S %p")
                        print(f"Scheduled '{job_name}' at: {start_time_12hr} on {start_datetime.date()}")
                        self.scheduler.add_job(self.main_migration_func, 'date', run_date=start_datetime, args=[job_name, postgres_conn])


   

    def main_migration_func(self,job_name,postgres_conn):
        try:
            """
            Executes the main migration process for a given job.
            
            Parameters:
            - job_name: The name of the migration job to execute.
            - postgres_conn: Connection object to the PostgreSQL database.
            """
            
            print(f"@@@@@@@@@@@@@@@@@@@@@@@@ Executing job: {job_name}")
            load_dotenv()
            migration_table=os.getenv('MIGRATION_TABLE')
            migration_details_dict=self.get_migration_details(job_name,postgres_conn,migration_table)
            print(f"TYPE {type(migration_details_dict)}")

            table_flag=migration_details_dict['table_flag']
            db_config=self.get_db_config(migration_details_dict)
            
            from_host=db_config['hostname']
            from_port=db_config['port']
            from_user=db_config['user']
            from_pwd=db_config['password']
            from_db_type=db_config['from_db_type']
            from_driver=os.getenv('FROM_DB_DRIVER')
        
            from_driver=os.getenv('FROM_DB_DRIVER')

            from_database=migration_details_dict['from_database']
            from_connection=self.create_connection(from_db_type,from_host,from_database,from_user,from_pwd,from_port,from_driver)

            to_hostname = os.getenv('LOCAL_DB_HOST')
            to_port = os.getenv('LOCAL_DB_PORT')
            to_db_name =migration_details_dict['to_database']
            to_user = os.getenv('LOCAL_DB_USER')
            to_password = os.getenv('LOCAL_DB_PASSWORD')
            to_db_type = os.getenv('LOCAL_DB_TYPE')
            to_table=migration_details_dict['to_table']
            print(f"Target db and table {to_db_name},{to_table}")
            to_connection=self.create_connection(to_db_type,to_hostname,to_db_name,to_user,to_password,to_port)
            
            # migration_table='migrations_3'

            time_column_check=migration_details_dict['time_column_check']
            time_columns = [col.strip() for col in time_column_check.split(',')]
            print(f"time column check got is {time_columns}")
            primary_id_col=migration_details_dict['primary_id_column']
            last_id=migration_details_dict['last_id']
            
            if last_id:
                try:
                    if isinstance(last_id,str) or isinstance(last_id,int):
                        last_id=f'[{last_id}]'
                        last_id=json.loads(last_id)
                        print(f"json loads last_id {last_id}")
                except:
                    print(f"Error loading last_id list {e}")
                    last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                print(f"last_id after load_last_id is {last_id}")
            else:
                last_id=[]
            migration_status=migration_details_dict['status']

        
            if table_flag:
                # Table-to-table migration
                print(f"############### Table to Table migration")            
                from_table=migration_details_dict['from_table']
                if not self.is_valid_table_name(from_table):
                    full_from_table=f'[{from_database}].[dbo].[{from_table}]'
                    
                else:
                    full_from_table=from_table       
                

                if migration_status=='first':
                    # Migrate all records if it's the first migration
                    # print(f"no records are migrated since begining so migrating all")
                    df=self.get_all_records(from_connection,full_from_table,primary_id_col)
                    df = df.astype(object).mask(df.isna(), None)
                    print(f"Dataframe got is \n {df.head()}")
                    print(f"The number of rows in the DataFrame is: {df.shape[0]}")
                    print(f'the last 5 rows:{df.tail(5)}')
                    insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,to_table,primary_id_col,time_columns)
                    if last_id_time is None:
                        last_id_time=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                        
                    
                        
                    migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)

                    print(f"############################# Details to update in table \n Status:{migration_details_dict_updated['status']},Last_ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")
                    print(f"Updating table")
                    uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})
                
                else:
                    # Incremental migration based on last_id
                    
                    if not last_id:
                        last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                    
                    print(f"##################### The last id migrated is {last_id} from column {time_column_check}")
                    migrate_records=self.get_updated_records_query(time_columns,last_id,full_from_table)
                    
                    
                    migrate_records_df = self.execute_query(from_connection,migrate_records)
                    if migrate_records_df.empty:
                        print(f"no records feteched in query execution")
                    
                    migrate_records_df = migrate_records_df.astype(object).mask(migrate_records_df.isna(), None)
                    print(f"query result is \n {migrate_records_df.head()}")
                    print(f"The number of rows in the DataFrame is: {migrate_records_df.shape[0]}")
                    insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,migrate_records_df,to_table,primary_id_col)
                    
                    if last_id_time is None:
                        
                        last_id_time=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                        
                    migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)

                    print(f"######################### Details to update in table \n Status:{migration_details_dict_updated['status']},Last ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")

                    print(f"Updating table")
                    uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})    

            else:
                # Query-based migration
                print(f"@@@@@@@@@@@@@@@@@@ Executing {job_name}")
                from_query=migration_details_dict['from_query']
                print(f"######################## The query is {from_query}")
                print(f"########################## the result to be saved on {to_db_name},{to_table}")
            
                

                if migration_status=='first':
                    df=self.execute_query(from_connection,from_query)
                    df = df.astype(object).mask(df.isna(), None)
                    print(f"DataFrame is \n {df.head()}")
                    print(f"The number of rows in the DataFrame is: {df.shape[0]}")
                    
                    
                    insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,to_table,primary_id_col,time_columns)
                    if last_id_time is None:
                        
                        last_id_time=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                        
                    
                    if insert_records:
                        print(f"################################## Migration Successfull")
                    else:
                        print(f"Errors in Migration")

                    migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)               


                    print(f"###################### Details to update in table \n Status:{migration_details_dict_updated['status']},Last ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")
                    print(f"Updating table")
                    uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})  
                
                else:
                    if not last_id:
                        last_id=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)

                    print(f"##################### The last id migrated is {last_id} from column {time_column_check}")
                    df=self.execute_query(from_connection,from_query)
                    df = df.astype(object).mask(df.isna(), None)
                    print(f"query result is \n {df.head()}")
                    print(f"The number of rows in the DataFrame is: {df.shape[0]}")                
                    
                    insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,to_table,primary_id_col,time_columns)
                    if last_id_time is None:                    
                        last_id_time=self.update_last_id_list(to_db_name,to_table,time_columns,primary_id_col)
                    
                    if insert_records:
                        print(f"############################## Migration Successfull")
                    else:
                        print(f"Errors in Migration")
                    
                    migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)               
                    print(f"############################### Details to update in table \n Status:{migration_details_dict_updated['status']},Migration Update:{migration_details_dict_updated['migration_update']},Status:{migration_details_dict_updated['last_id']}")

                    print(f"Updating table")
                    uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})    
            return True
        except Exception as e:
            logging.eror(f"Exception while running migration - {e}")
            # print(f"Exception while running migration - {e}")
            return False
    def get_migration_details(self, job_name, conn,migration_table):
        """
            get the migration job details from migration table and return them as a dict item

            Args:
            - job_name : name of the migration job
            - conn : postgres_conn or connection where migration database is present
            - migration_table : name of the table        
        """
        try:
            query = f"SELECT * FROM {migration_table} WHERE migration_name = '{job_name}'"
            query_result = self.execute_query(conn, query)
            return query_result.to_dict(orient='records')[0]
        except Exception as e:
            print(f"Error while fetching the {job_name} details - {e}")
            return {}
        
    def get_db_config(self, migration_details_dict):
        db_config = migration_details_dict.get('from_db_config')
        if db_config and not isinstance(db_config, dict):
            db_config = json.loads(db_config)
        return db_config or {
            'hostname': os.getenv('FROM_DB_HOST'),
            'port': os.getenv('FROM_DB_PORT'),
            'user': os.getenv('FROM_DB_USER'),
            'password': os.getenv('FROM_DB_PASSWORD'),
            'from_db_type': os.getenv('FROM_DB_TYPE')
        }
    
    def create_postgres_connection(self,postgres_db_name):
        print(f"In create_postgres_connection {postgres_db_name}")
        
        load_dotenv()
        hostname = os.getenv('LOCAL_DB_HOST')
        port = os.getenv('LOCAL_DB_PORT')
        user = os.getenv('LOCAL_DB_USER')
        password = os.getenv('LOCAL_DB_PASSWORD')
        db_type = os.getenv('LOCAL_DB_TYPE')
        try:
            connection=self.create_connection(db_type,hostname,postgres_db_name,user,password,port)
            print(f"in create postgres connection {connection}")
        except Exception as e:
            logging.error(f"Error while establishing connection {e}")
        return connection
    
    def update_migration_details_dict(self,migration_details_dict,insert_records,success,failures,last_id_time,db_config):
        # print(f"last id got is {last_id_time}")
        # print(f"Migration Status is {insert_records}")
        
        migration_details_dict['status']=insert_records
        migration_details_dict['last_id']=last_id_time

        migration_update_list=migration_details_dict['migration_update']
        # print(f"Migration Status {migration_update_list}")
        migration_update_dict={'Success_Records':success,'failed_records':failures}
        # migration_update_list=[migration_update]
        migration_update_list.append(migration_update_dict)
        migration_details_dict['migration_update']=json.dumps(migration_update_list)
        
        migration_details_dict['from_db_config']=json.dumps(db_config)
        
        migrated_time=datetime.now()
        # print(f"Current_time is {migrated_time}")
        migration_details_dict['last_migrated_time']=migrated_time
        return migration_details_dict
    
    def get_all_records(self,connection,table_name,primary_id_col):
        """
            Retrieves all records from a specified table, ordered by the primary key column.

            Parameters:
            - connection: Connection object to the source database.
            - table_name: The name of the table from which to fetch records.
            - primary_id_col: The primary key column to order the records by.

            Returns:
            - df: A pandas DataFrame containing all records from the specified table.
                Returns None if an error occurs.
        """
        try:
            df_rows=f"select * from {table_name} order by {primary_id_col}"
            df = self.execute_query(connection,df_rows)
            df = df.astype(object).mask(df.isna(), None)
            return df
            
        except Exception as e:
            print(f"Error while fetching all records {e}")
            return None
        
    def get_updated_records_query(self,columns, last_id, full_from_table):
        """
            Constructs an SQL query to fetch updated records from a specified table based on the last processed values
            of specified columns.

            Parameters:
            - columns: A list of column names (usually timestamps or IDs) to check for updates.
            - last_id: A list of the last processed values for the corresponding columns.
            - full_from_table: The fully qualified name of the table from which to fetch updated records.

            Returns:
            - migrate_records: A string containing the constructed SQL query to fetch updated records.
        """
        # Constructing the WHERE clause dynamically
        where_conditions = []
        for col, val in zip(columns, last_id):
            if isinstance(val, str):
                where_conditions.append(f"{col} > '{val}'")
            else:
                where_conditions.append(f"{col} > {val}")

        where_clause = " AND ".join(where_conditions)

        # Constructing the SQL query
        migrate_records = f"SELECT * FROM {full_from_table} WHERE {where_clause}"
        return migrate_records
        
    
    
        
    def enclose_uppercase_words(self,input_string):
        # Split the input string by commas and strip any surrounding whitespace
        words = [word.strip() for word in input_string.split(',')]
        
        # Function to check if a word contains uppercase characters
        def needs_quotes(word):
            return any(char.isupper() for char in word)
        
        # Enclose words with uppercase characters in double quotes
        quoted_words = [f'"{word}"' if needs_quotes(word) else word for word in words]
        
        # Join the words back into a single string separated by commas
        return ', '.join(quoted_words)
        
    def insert_dict(self, conn, table_name, data_dict):
        """
        Insert a row into a PostgreSQL table using a dictionary.

        :param conn: psycopg2 connection object
        :param table_name: Name of the table to insert into
        :param data_dict: Dictionary containing column-value pairs to insert
        """
        try:
            # Convert dictionary values to JSON strings if necessary
            for key, value in data_dict.items():
                if isinstance(value, dict):
                    data_dict[key] = json.dumps(value)

            # Generate the column names and placeholders for the SQL query
            columns = ', '.join(data_dict.keys())
            placeholders = ', '.join(['%s'] * len(data_dict))

            # Create the SQL query
            sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            values = list(data_dict.values())

            # print(f"SQL Query: {sql_query}")
            # print(f"Values: {values}")

            # Execute the query
            with conn.cursor() as cursor:
                cursor.execute(sql_query, values)
                conn.commit()
            print("Insert successful")
            return True
        except Exception as e:
            conn.rollback()
            print(f"Error inserting into table: {e}")
            return False
        
    def is_valid_table_name(self,table_name):
        pattern = r'^\[\w+\]\.\[\w+\]\.\[\w+\]$'
        return re.match(pattern, table_name) is not None
    

    def check_db_connection(self,hostname, port, username, password,db_type):
        try:
            if db_type=='mssql':
                try:
                    
                    if not port:
                        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={hostname};DATABASE=master;UID={username};PWD={password}'
                    else:
                        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={hostname},{port};DATABASE=master;UID={username};PWD={password}'
                    connection = pyodbc.connect(connection_string)
                    connection.close()
                    print("Connection to MSSQL database established successfully!")
                    return True
                except pyodbc.Error as e:
                    print(f"Error: Could not connect to MSSQL database. {e}")
                    return False
            else:
                print(f"Enter valid db details")
                return False
        except Exception as e:
            print(f"Error while connecting to db {e}")


    def update_table(self,conn, table_name, data_dict, condition_dict):
        """
        Update a PostgreSQL table using a dictionary.

        :param conn: psycopg2 connection object
        :param table_name: Name of the table to update
        :param data_dict: Dictionary containing column-value pairs to update
        :param condition_dict: Dictionary containing column-value pairs for the WHERE condition
        """
        try:
            # Generate the SET part of the SQL query
            set_clause = ', '.join([f"{col} = %s" for col in data_dict.keys()])

            if condition_dict:

            # Generate the WHERE part of the SQL query
                where_clause = ' AND '.join([f"{col} = %s" for col in condition_dict.keys()])

            
            if condition_dict:
            # Complete SQL query
                sql_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                values = list(data_dict.values()) + list(condition_dict.values())

            else:
                sql_query = f"UPDATE {table_name} SET {set_clause}"
                values = list(data_dict.values())

            # Combine the values for the SET and WHERE parts
            

            # print(f"SQL Quey {sql_query} and values are {values}")

            ## Execute the query
            with conn.cursor() as cursor:
                
                cursor.execute(sql_query, values)
                conn.commit()
            print("################################## Update successful")
        except Exception as e:
            conn.rollback()
            print(f"Error updating table: {e}")

    def insert_records_postgresql(self,postgres_connection,df,insert_table_name,migration_track_col):
        """
            Inserts records from a DataFrame into a PostgreSQL table.

            Parameters:
            - postgres_connection: The connection object to the PostgreSQL database.
            - df: The DataFrame containing the records to be inserted.
            - insert_table_name: The name of the table where records will be inserted.
            - migration_track_col: The column used to track the last inserted record.

            Returns:
            - insert_flag: A boolean indicating if the insertion was successful.
            - inserted_records: The count of successfully inserted records.
            - failed_records: The count of records that failed to insert.
            - last_id_time: The value of the last inserted record's migration tracking column.
            Note:
            - Very Slow as each record is being inserted at a time
            - Not using this currently
        """
        inserted_records=0
        failed_records=0
        insert_flag=False
        batch_size=30000

        for index, row in df.iterrows():
            
            failed_log=[]

            
            try:
                row_dict = row.to_dict()
                columns = ', '.join(row_dict.keys())
                placeholders = ', '.join(['%s'] * len(row_dict))
                

                # Create the SQL query
                to_table=insert_table_name
                insert_query = f"INSERT INTO {to_table} ({columns}) VALUES ({placeholders})"
                
                # Extract the values from the dictionary
                values = tuple(row_dict.values())
                insert_query_=f"INSERT INTO {to_table} ({columns}) VALUES ({values})"

                # print(insert_query_)
                


                with postgres_connection.cursor() as cursor:
                    cursor.execute(insert_query, values)
                postgres_connection.commit()
                last_id_time=row_dict.get(migration_track_col,None)
                inserted_records += 1
                insert_flag=True


            except Exception as e:
                postgres_connection.rollback()
                failed_records += 1
                last_id_time=0
                failed_log.append({
                    'index': index,
                    'record': row_dict,
                    'error': str(e)
                })
                insert_flag=False
                print(f"Error inserting row {index}: {e}")
                with open('error_log.txt', 'a') as f:
                    f.write(f"Failed record details:\nIndex: {index}\nRecord: {row_dict}\nError: {str(e)}\n\n")

            
            

        # print(f"Inserted records: {inserted_records}")
        # print(f"Failed records: {failed_records}")
        return insert_flag,inserted_records,failed_records,last_id_time

    def insert_records_postgresql_batch_5_(self, postgres_connection, df, insert_table_name, migration_track_col):
        print(f"Inserting in table {insert_table_name},{migration_track_col}")
        # print(f"postgres conn {postgres_connection}")
        
        inserted_records = 0
        failed_records = 0
        insert_flag = True
        batch_size = 5000
        failed_log = []
        last_id_time = None
        num_batches = math.ceil(len(df) / batch_size)

        print(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")
        
        # Extract column names once
        columns = ', '.join(df.columns)
        
        # columns=self.enclose_uppercase_words(columns) #to handle columns with cap letter #notadvised
        print(f"Columns {columns}")

        update_clause = ', '.join([f"{col} = EXCLUDED.{col}"for col in df.columns])
        
        try:
            total_start_time = time.time()
            
            for batch_index in range(num_batches):
                batch_start_time = time.time()
                
                start_index = batch_index * batch_size
                end_index = min((batch_index + 1) * batch_size, len(df))
                
                batch_df = df.iloc[start_index:end_index]
                rows = [tuple(row) for row in batch_df.to_numpy()]  # Convert batch_df to list of tuples
                # print(f"type(rows):::{type(rows)}")
                batch_end_time = time.time()
                insert_start_time=time.time()

                
                
                #insert_query = f'INSERT INTO {insert_table_name} ({columns}) VALUES %s'
                # print(f"update_query {update_clause}")
                
                insert_query_update = f'''
                    INSERT INTO {insert_table_name} ({columns}) 
                    VALUES %s 
                    ON CONFLICT (id) DO UPDATE 
                    SET {update_clause}
                '''
                
                insert_query = f'INSERT INTO {insert_table_name} ({columns}) VALUES %s'
                # print(f"insert_query: {insert_query},{rows}")
                
                # with postgres_connection.cursor() as cursor:
                #     execute_values(cursor, insert_query, rows)
                #     # cursor.executemany(insert_query, rows)
                # postgres_connection.commit()
                try:
                    with postgres_connection.cursor() as cur:
                        execute_values(cur, insert_query, rows)
                    postgres_connection.commit()
                except psycopg2.Error as e:
                    print(f"Error inserting batch: {e}")
                    # Handle the error or retry the operation without ON CONFLICT clause if necessary
                    insert_query_no_conflict = f'INSERT INTO {insert_table_name} ({columns}) VALUES %s'
                    try:
                        with postgres_connection.cursor() as cur:
                            execute_values(cur, insert_query_no_conflict, rows)
                        postgres_connection.commit()
                    except psycopg2.Error as e2:
                        print(f"Error inserting batch without ON CONFLICT: {e2}")
                        postgres_connection.rollback()
                        failed_records += len(rows)
                        insert_flag = False
                        failed_log.append({
                            'batch_index': batch_index,
                            'error': str(e)
                        })
                        print(f"Error inserting batch {batch_index + 1} of {num_batches}: {e}")
                        with open('error_log.txt', 'a') as f:
                            f.write(f"Failed batch details:\nBatch index: {batch_index}\nError: {str(e)}\n\n")
                
                insert_end_time=time.time()
                
                insert_elaspsed_time=insert_end_time-insert_start_time
                batch_elapsed_time = batch_end_time - batch_start_time
                
                print(f"Inserted batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                # print(f"Batch Creation time: {batch_elapsed_time:.2f} seconds")
                # print(f"Batch insertion time: {insert_elaspsed_time:.2f} seconds")
                
                inserted_records += len(rows)
        
                # Update the last_id_time with the last record's migration_track_col value
                last_id_time = batch_df[migration_track_col].iloc[-1]
                if batch_index==4:
                    break;
            
            total_end_time = time.time()
            total_elapsed_time = total_end_time - total_start_time
            
            # print(f"Total insertion time: {total_elapsed_time:.2f} seconds")
            
        except Exception as e:
            
            postgres_connection.rollback()
            failed_records += len(rows)
            insert_flag = False
            failed_log.append({
                'batch_index': batch_index,
                'error': str(e)
            })
            print(f"Error inserting batch {batch_index + 1} of {num_batches}: {e}")
            with open('error_log.txt', 'a') as f:
                f.write(f"Failed batch details:\nBatch index: {batch_index}\nError: {str(e)}\n\n")

        if last_id_time:
            try:
                last_id_time_=self.jsonify_last_id(last_id_time)  
            except:
                last_id_time_=None

            print(f"last_id_time_ {last_id_time_}")
        else:
            last_id_time_ = None
        
        # print(f"Inserted records: {inserted_records}")
        # print(f"Failed records: {failed_records}")
        

        
        # last_id_time=json.dumps(last_id_time)
        return insert_flag, inserted_records, failed_records, last_id_time_
    
    def insert_records_postgresql_batch_5_without_retry(self, postgres_connection, df, insert_table_name, migration_track_col,time_columns):
        print(f"Inserting into table {insert_table_name}, column {migration_track_col}")
        print(f"time columns got are {time_columns}")

        inserted_records = 0
        failed_records = 0
        insert_flag = True
        batch_size = 5000
        failed_log = []
        last_id_time = None
        num_batches = math.ceil(len(df) / batch_size)

        print(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")

        # Extract column names once
        columns = ', '.join(df.columns)
        print(f"Columns: {columns}")

        # Prepare the update clause for ON CONFLICT
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns])

        total_start_time = time.time()

        for batch_index in range(num_batches):
            start_index = batch_index * batch_size
            end_index = min((batch_index + 1) * batch_size, len(df))

            batch_df = df.iloc[start_index:end_index]
            rows = [tuple(row) for row in batch_df.to_numpy()]

            insert_query_update = f'''
                INSERT INTO {insert_table_name} ({columns}) 
                VALUES %s 
                ON CONFLICT (id) DO UPDATE 
                SET {update_clause}
            '''
            
            insert_query_no_conflict = f'INSERT INTO {insert_table_name} ({columns}) VALUES %s'

            batch_start_time = time.time()

            try:
                with postgres_connection.cursor() as cur:
                    execute_values(cur, insert_query_update, rows)
                postgres_connection.commit()
            except psycopg2.Error as e:
                print(f"Error inserting batch {batch_index + 1} with ON CONFLICT: {e}")
                postgres_connection.rollback()  # Rollback after the first error
                try:
                    with postgres_connection.cursor() as cur:
                        execute_values(cur, insert_query_no_conflict, rows)
                    postgres_connection.commit()
                except psycopg2.Error as e2:
                    print(f"Error inserting batch {batch_index + 1} without ON CONFLICT: {e2}")
                    postgres_connection.rollback()  # Rollback after the second error
                    failed_records += len(rows)
                    insert_flag = False
                    failed_log.append({
                    'batch_index': batch_index,
                    'error': str(e2),
                    'failed_ids': batch_df['id'].tolist()  # Capture IDs of failed records
                    })
                    with open('error_log.txt', 'a') as f:
                        f.write(f"Failed batch details:\nBatch index: {batch_index}\nError: {str(e2)}\nFailed IDs: {batch_df['id'].tolist()}\n\n")

            batch_end_time = time.time()
            print(f"Inserted batch {batch_index + 1} of {num_batches} with {len(rows)} records")
            print(f"Batch insertion time: {batch_end_time - batch_start_time:.2f} seconds")

            inserted_records += len(rows)
            last_id_time = [batch_df[col].iloc[-1] for col in time_columns]
            if batch_index == 4:
                break

        total_end_time = time.time()
        print(f"Total insertion time: {total_end_time - total_start_time:.2f} seconds")
        last_id_time_=self.serialize_timestamps_and_jsonify(last_id_time)
        # last_id_time_ = self.jsonify_last_id(last_id_time) if last_id_time else None
        print(f"last_id_time_: {last_id_time}")

        return insert_flag, inserted_records, failed_records, last_id_time_

    
    def insert_records_postgresql_batch_5(self, postgres_connection, df, insert_table_name, migration_track_col, time_columns):
        """
        Inserts records into a PostgreSQL table in batches, with error handling for individual rows.

        Args:
            postgres_connection: The connection object to the PostgreSQL database.
            df: The DataFrame containing the data to be inserted.
            insert_table_name: The name of the table where data will be inserted.
            migration_track_col: The column used for tracking the migration progress.
            time_columns: List of columns that hold timestamp data.

        Returns:
            A tuple containing:
            - insert_flag (bool): Indicates whether the insertion was successful.
            - inserted_records (int): The number of successfully inserted records.
            - failed_records (int): The number of failed records.
            - last_id_time (str): JSON string of the last timestamp(s) from the successfully inserted records.

        Process:
        1. Calculates the number of batches needed based on the batch size (5000).
        2. Attempts to insert each batch:
            a. First, with the ON CONFLICT clause to handle conflicts.
            b. If the batch insertion fails, tries inserting the batch without the ON CONFLICT clause.
            c. If the batch insertion still fails, attempts to insert each row individually:
                i. First, with the ON CONFLICT clause.
                ii. If the individual row insertion fails, tries without the ON CONFLICT clause.
        3. Logs errors for any rows that fail to insert.
        4. Tracks the number of successfully inserted and failed records.
        5. Calculates and logs the total insertion time.
        6. Serializes and returns the last timestamp(s) and id(S) from the successfully inserted records.

        Notes:
        - Retries for 3 times when insert/update fails
        - Logs detailed error messages and failed record IDs to 'error_log.txt'.
        """
        # print(f"Inserting into table {insert_table_name}, column {migration_track_col}")
        # print(f"time columns got are {time_columns}")

        inserted_records = 0
        failed_records = 0
        insert_flag = True
        batch_size = 5000
        failed_log = []
        last_id_time = None
        num_batches = math.ceil(len(df) / batch_size)
        print(f"############# Records to insert: {df.shape[0]}")        
        print(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")

        # Extract column names once
        columns = ', '.join(df.columns)
        print(f"Columns: {columns}")

        # Prepare the update clause for ON CONFLICT
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns])

        total_start_time = time.time()

        # Define the insert block operation
        def insert_block_operation():
            nonlocal inserted_records, failed_records, last_id_time, insert_flag

            for batch_index in range(num_batches):
                start_index = batch_index * batch_size
                end_index = min((batch_index + 1) * batch_size, len(df))

                batch_df = df.iloc[start_index:end_index]
                rows = [tuple(row) for row in batch_df.to_numpy()]

                insert_query_update = f'''
                    INSERT INTO {insert_table_name} ({columns}) 
                    VALUES %s 
                    ON CONFLICT (id) DO UPDATE 
                    SET {update_clause}
                '''
                
                insert_query_no_conflict = f'INSERT INTO {insert_table_name} ({columns}) VALUES %s'

                batch_start_time = time.time()

                try:
                    with postgres_connection.cursor() as cur:
                        # Attempt to insert the batch with ON CONFLICT clause
                        execute_values(cur, insert_query_update, rows)
                    postgres_connection.commit()
                except psycopg2.Error as e:
                    print(f"Error inserting batch {batch_index + 1} with ON CONFLICT: {e}")
                    postgres_connection.rollback()  # Rollback after the first error

                    try:
                        with postgres_connection.cursor() as cur:
                            # Attempt to insert the batch without ON CONFLICT clause
                            execute_values(cur, insert_query_no_conflict, rows)
                        postgres_connection.commit()
                    except psycopg2.Error as e2:
                        print(f"Error inserting batch {batch_index + 1} without ON CONFLICT: {e2}")
                        postgres_connection.rollback()  # Rollback after the second error

                        # Try inserting rows individually
                        for row_index, row in batch_df.iterrows():
                            try:
                                with postgres_connection.cursor() as cur:
                                    # Attempt to insert individual row with ON CONFLICT clause
                                    cur.execute(insert_query_update, (row,))
                                postgres_connection.commit()
                                inserted_records += 1
                            except psycopg2.Error as e3:
                                print(f"Error inserting row {row_index + start_index} with ON CONFLICT: {e3}")
                                postgres_connection.rollback()  # Rollback after the third error
                                
                                try:
                                    with postgres_connection.cursor() as cur:
                                        # Attempt to insert individual row without ON CONFLICT clause
                                        cur.execute(insert_query_no_conflict, (row,))
                                    postgres_connection.commit()
                                    inserted_records += 1
                                except psycopg2.Error as e4:
                                    print(f"Error inserting row {row_index + start_index} without ON CONFLICT: {e4}")
                                    postgres_connection.rollback()  # Rollback after the fourth error
                                    failed_records += 1
                                    insert_flag = False
                                    # Log the failed row details
                                    failed_log.append({
                                        'batch_index': batch_index,
                                        'row_index': row_index + start_index,
                                        'error': str(e4),
                                        'failed_id': row['id']  # Capture ID of the failed record
                                    })
                                    with open('error_log.txt', 'a') as f:
                                        f.write(f"Failed row details:\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e4)}\nFailed ID: {row['id']}\n\n")
                else:
                    # If batch insertion is successful, count all rows as inserted
                    inserted_records += len(rows)

                batch_end_time = time.time()
                print(f"Inserted batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                print(f"Batch insertion time: {batch_end_time - batch_start_time:.2f} seconds")

                # Update last_id_time with the time columns from the last row of the current batch
                last_id_time = [batch_df[col].iloc[-1] for col in time_columns]

                if batch_index == 4:
                    break

        # Retry the entire block operation up to 3 times
        for attempt in range(3):
            try:
                insert_block_operation()
                break  # Break the retry loop if successful
            except Exception as e:
                print(f"Error in whole insert block operation on attempt {attempt + 1}: {e}")
                with open('error_log.txt', 'a') as f:
                    f.write(f"Error in whole insert block operation on attempt {attempt + 1}: {str(e)}\n")

        total_end_time = time.time()
        print(f"Total insertion time: {total_end_time - total_start_time:.2f} seconds")

        # Serialize and jsonify the last_id_time
        last_id_time_ = self.serialize_timestamps_and_jsonify(last_id_time)
        print(f"last_id_time_: {last_id_time_}")

        return insert_flag, inserted_records, failed_records, last_id_time_
    
    def update_records_postgresql_batch(self, postgres_connection, df, update_table_name, migration_track_col, time_columns):
        """
        Updates records in a PostgreSQL table in batches, with error handling for individual rows.

        Args:
            postgres_connection: The connection object to the PostgreSQL database.
            df: The DataFrame containing the data to be updated.
            update_table_name: The name of the table where data will be updated.
            migration_track_col: The column used for tracking the migration progress.
            time_columns: List of columns that hold timestamp data.

        Returns:
            A tuple containing:
            - update_flag (bool): Indicates whether the update was successful.
            - updated_records (int): The number of successfully updated records.
            - failed_records (int): The number of failed records.
            - last_id_time (str): JSON string of the last timestamp(s) from the successfully updated records.

        Process:
        1. Calculates the number of batches needed based on the batch size (5000).
        2. Attempts to update each batch:
            a. Updates the batch records.
            b. If the batch update fails, tries updating the batch one record at a time.
        3. Logs errors for any rows that fail to update.
        4. Tracks the number of successfully updated and failed records.
        5. Calculates and logs the total update time.
        6. Serializes and returns the last timestamp(s) and id(S) from the successfully updated records.

        Notes:
        - Retries for 3 times when update fails
        - Logs detailed error messages and failed record IDs to 'error_log.txt'.
        """

        updated_records = 0
        failed_records = 0
        update_flag = True
        batch_size = 5000
        failed_log = []
        last_id_time = None
        num_batches = math.ceil(len(df) / batch_size)
        print(f"############# Records to update: {df.shape[0]}")
        print(f"########### No of Batches Created: {num_batches} with batch size {batch_size}")

        # Extract column names once, excluding the primary key column
        columns = [col for col in df.columns if col != 'id']
        column_names = ', '.join(columns)
        print(f"Columns: {column_names}")

        # Prepare the set clause for the update statement
        set_clause = ', '.join([f"{col} = %s" for col in columns])
        update_query = f'''
            UPDATE {update_table_name}
            SET {set_clause}
            WHERE id = %s
        '''

        total_start_time = time.time()

        # Define the update block operation
        def update_block_operation():
            nonlocal updated_records, failed_records, last_id_time, update_flag

            for batch_index in range(num_batches):
                start_index = batch_index * batch_size
                end_index = min((batch_index + 1) * batch_size, len(df))

                batch_df = df.iloc[start_index:end_index]
                rows = [tuple(row) for row in batch_df.to_numpy()]

                batch_start_time = time.time()

                try:
                    with postgres_connection.cursor() as cur:
                        # Attempt to update the batch
                        print(f"Trying to update batch {batch_index + 1}")
                        for row in rows:
                            cur.execute(update_query, (*row[1:], row[0]))
                    postgres_connection.commit()
                except psycopg2.Error as e:
                    print(f"Error updating batch {batch_index + 1}: {e}")
                    postgres_connection.rollback()  # Rollback after the first error

                    # Try updating rows individually
                    for row_index, row in batch_df.iterrows():
                        try:
                            with postgres_connection.cursor() as cur:
                                # Attempt to update individual row
                                cur.execute(update_query, (tuple(row[1:]), row['id']))
                            postgres_connection.commit()
                            updated_records += 1
                        except psycopg2.Error as e3:
                            print(f"Error updating row {row_index + start_index}: {e3}")
                            postgres_connection.rollback()  # Rollback after the third error
                            failed_records += 1
                            update_flag = False
                            # Log the failed row details
                            failed_log.append({
                                'batch_index': batch_index,
                                'row_index': row_index + start_index,
                                'error': str(e3),
                                'failed_id': row['id']  # Capture ID of the failed record
                            })
                            with open('error_log.txt', 'a') as f:
                                f.write(f"Failed row details:\nBatch index: {batch_index}\nRow index: {row_index + start_index}\nError: {str(e3)}\nFailed ID: {row['id']}\n\n")
                else:
                    # If batch update is successful, count all rows as updated
                    updated_records += len(rows)

                batch_end_time = time.time()
                print(f"Updated batch {batch_index + 1} of {num_batches} with {len(rows)} records")
                print(f"Batch update time: {batch_end_time - batch_start_time:.2f} seconds")

                # Update last_id_time with the time columns from the last row of the current batch
                last_id_time = [batch_df[col].iloc[-1] for col in time_columns]

        # Retry the entire block operation up to 3 times
        for attempt in range(3):
            try:
                update_block_operation()
                break  # Break the retry loop if successful
            except Exception as e:
                print(f"Error in whole update block operation on attempt {attempt + 1}: {e}")
                with open('error_log.txt', 'a') as f:
                    f.write(f"Error in whole update block operation on attempt {attempt + 1}: {str(e)}\n")

        total_end_time = time.time()
        print(f"Total update time: {total_end_time - total_start_time:.2f} seconds")

        # Serialize and jsonify the last_id_time
        last_id_time_ = self.serialize_timestamps_and_jsonify(last_id_time)
        print(f"last_id_time_: {last_id_time_}")

        return update_flag, updated_records, failed_records, last_id_time_
    
    def log_failed_batch(self,batch_index, error):
        with open('error_log.txt', 'a') as f:
            f.write(f"Failed batch details:\nBatch index: {batch_index}\nError: {str(error)}\n\n")
    
    def serialize_timestamps_and_jsonify(self,input_list):
        """
            Serializes a list by converting Timestamp and datetime objects to ISO 8601 string format and JSON-encodes the list.

            Parameters:
            - input_list: A list containing various data types, including Timestamp and datetime objects.

            Returns:
            - A JSON-encoded string of the list with serialized timestamps, or None if an error occurs.

            Note:
            - The returned list is a json compatible list which can be loaded directly
        """
        try:
            serialized_list = []
            
            for item in input_list:
                if isinstance(item, Timestamp):
                    serialized_list.append(item.to_pydatetime().isoformat())
                elif isinstance(item, datetime):
                    serialized_list.append(item.isoformat())
                else:
                    serialized_list.append(item)
            
            return json.dumps(serialized_list)
        except Exception as e:
            print(f"Error serialize_timestamps_and_jsonify - {e}")
            return None

    def update_last_id_list(self,db_name, insert_table_name, time_check_cols, primary_id_col):
        """
            Retrieves the timestamp or primary key of the last inserted record from a PostgreSQL table.

            Parameters:
            - db_name: Name of the PostgreSQL database.
            - insert_table_name: Name of the table from which to fetch the last inserted record.
            - time_check_cols: List of columns to check for timestamp values.
            - primary_id_col: Column name used for ordering if timestamp columns are absent.

            Returns:
            - A list containing the last inserted record's timestamp or primary key values in ISO 8601 format,
            or None if an error occurs.

            Notes:
            - to get list of last_id's if needed in migration
            - this is to handle any error happened while inserting and if error happened in last_id storing
        """
        try:
            postgres_connection = self.create_postgres_connection(db_name)
            last_id_time = []

            with postgres_connection.cursor() as cursor:
                # Constructing the SELECT query dynamically for multiple columns
                select_cols = ', '.join(time_check_cols)
                if not primary_id_col:
                    query=f"SELECT {select_cols} FROM {insert_table_name} ORDER BY {select_cols} DESC LIMIT 1"
                else:
                    query = f"SELECT {select_cols} FROM {insert_table_name} ORDER BY {primary_id_col} DESC LIMIT 1"
                
                cursor.execute(query)
                result = cursor.fetchone()

                if result:
                    for value in result:
                        if isinstance(value, datetime):
                            last_id_time.append(value.isoformat())
                        else:
                            last_id_time.append(value)
                else:
                    last_id_time = None

        except Exception as fetch_error:
            last_id_time = None
            print(f"Error fetching last inserted record: {fetch_error}")

        return last_id_time
    
    

    


if __name__ == "__main__":
    manager = MigrationScheduler()
    manager.main()