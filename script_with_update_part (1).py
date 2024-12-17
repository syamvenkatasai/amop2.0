"""
This is a backup script after implementing the updates along with insert
does not hanlde last_id as list

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

import json
from datetime import datetime, timedelta
import threading
import time
from psycopg2.extras import execute_values
import math


logging.basicConfig(filename='db_migration.log', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s:%(message)s')

class MigrationScheduler:

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
            return None# Placeholder return value
    
    def create_new_migration(self,connection,table_name):
        try:
            load_dotenv()
            migration_details_dict={}
            print(f"Creating A New Migration please enter the respective details:")
            migration_job_name=input("Enter a unique name for migration:")
            print(f"Please Enter DB details (from db data is being migrated from)")
            from_host=input("Enter host name:")
            from_port=input("Enter port number (leave empty if not present):")
            from_user=input("Enter db username:")
            from_pwd=input("Enter db password:")
            from_db_type=input("Enter database type [mssql,postgresql]:")
            print(f"Checking db connection with {from_host},{from_port},{from_user},{from_pwd},{from_db_type}")
            check_db_conection=self.check_db_connection(from_host,from_port,from_user,from_pwd,from_db_type)
            if check_db_conection:
                db_config_dict={}
                db_config_dict['hostname']=from_host
                db_config_dict['port']=from_port
                db_config_dict['user']=from_user
                db_config_dict['password']=from_pwd
                db_config_dict['from_db_type']=from_db_type
            else:
                print(f"wrong details")
            
            migration_details_dict['migration_name']=migration_job_name
            migration_details_dict['from_db_config']=json.dumps(db_config_dict)
            migration_details_dict['status']='first'
            migration_details_dict['last_id']='[]'
            table_flag=input("Is it table-to-table migration or custom migration (table/custom):")
            if table_flag=='table':
                table_flag=True
                from_db_name=input("Enter the db_name (from where you want to migrate the data) :")
                from_table=input("Enter table_name (from where you want to migrate the data) :")
                primary_id_col=input("Enter primary id column name:")
                time_check_col=input("Enter the column which you will use to keep track of migrations (Ex. LastUpdatedDate, ID etc) :")
                from_db_driver=os.getenv('FROM_DB_DRIVER')
                print(f"db driver {from_db_driver}")

                if not self.is_valid_table_name(from_table):
                    full_from_table=f'[{from_db_name}].[dbo].[{from_table}]'
                    print(f"full table name {full_from_table}")

                from_db_connection=self.create_connection(from_db_type,from_host,from_db_name,from_user,from_pwd,from_port,from_db_driver)

                print(f"from connection {from_db_connection}")

                to_db_name=input(f"Enter Target DB (to which you want to migrate the data to) :")
                to_table_name=input("Enter Target table_name (to which you want to migrate the data to) :")
                hostname = os.getenv('LOCAL_DB_HOST')
                port = os.getenv('LOCAL_DB_PORT')
                user = os.getenv('LOCAL_DB_USER')
                password = os.getenv('LOCAL_DB_PASSWORD')
                db_type = os.getenv('LOCAL_DB_TYPE')
                
                to_db_connection=self.create_connection(db_type,hostname,to_db_name,user,password,port,driver='')
                print(f"to db connection {to_db_connection}")

                # table_present=self.table_exists_postgres(to_db_connection,to_table_name)
                # if table_present:
                #     print(f"table is already present")
                # else:
                #     print(f"table not present, creating table...")
                #     print
                #     create_table=self.create_pgsqltable_from_mssqlschema(from_db_connection,from_db_type,to_db_connection,from_table,to_table_name,primary_id_col)
                # if not create_table:
                #     print(f"Error Encountered while table creation")

                migration_details_dict['table_flag']=True
                migration_details_dict['from_database']=from_db_name
                migration_details_dict['from_table']=from_table
                migration_details_dict['time_column_check']=time_check_col
                migration_details_dict['to_database']=to_db_name
                migration_details_dict['to_table']=to_table_name
                
            elif table_flag=='custom':
                table_flag=False
                from_query=input("Enter query/script which you want to use to get data:")
                from_db_name=input("Enter the db_name (from where you want to migrate the data) :")
                to_query=input("Enter query to save data in db(leave empty, if you want to store data to a table):")
                migration_details_dict['table_flag']=False
                migration_details_dict['from_query']=from_query
                if not to_query:
                    to_db_name=input(f"Enter Target DB (to which you want to migrate the data to) :")
                    to_table_name=input("Enter Target table_name (to which you want to migrate the data to) :")
                    migration_details_dict['to_database']=to_db_name
                    migration_details_dict['to_table']=to_table_name
                else:
                    migration_details_dict['to_query']=to_query

            print(f"Inserting the migration job into database..........")
            insert_job=self.insert_dict(connection,table_name,migration_details_dict)       

            if insert_job:
                print(f"Insertion Succesfull Insert details are {migration_details_dict}")
            
        except Exception as e:
            print(f"Error occurred while creating a new migration job  {e}")
            return False
    
    def first_migration_insert_all(self,from_connection,to_connection,table_flag,migration_details_dict):
       pass
       
    
    def main(self):
        load_dotenv()  # Load environment variables from .env file
        migration_details_dict={}
        
        print("Beginning migration")
        
        hostname = os.getenv('LOCAL_DB_HOST')
        port = os.getenv('LOCAL_DB_PORT')
        db_name = 'Migration_Test'
        user = os.getenv('LOCAL_DB_USER')
        password = os.getenv('LOCAL_DB_PASSWORD')
        db_type = os.getenv('LOCAL_DB_TYPE')
        migration_table=os.getenv('MIGRATION_TABLE')
        
        postgres_conn = self.create_connection(db_type, hostname, db_name, user, password, port)
        query = f"SELECT migration_name FROM {migration_table}"
        rows = self.execute_query(postgres_conn, query)

        
        
        if rows.empty:
            print("No existing jobs found. Creating a new job.")
            new_job = self.create_new_migration(postgres_conn, migration_table)
        
        else:
            user_choice = input("Do you want to create a new migration job or select an existing one? (new/existing): ").strip().lower()
            
            if user_choice == 'new':
                print("Creating a new migration job.")
                new_job = self.create_new_migration(postgres_conn, migration_table)
            elif user_choice == 'existing':
                print("Existing jobs:")
                print(rows)
                choice = input("Enter 'single' to execute a single job immediately or 'scheduled' to schedule jobs: ").strip().lower()
    
                if choice == 'single':
                    job_name = input("Enter the job name you want to execute: ").strip()
                    self.main_migration_func(job_name, postgres_conn)
                elif choice == 'scheduled':
                    job_names = input("Enter the job names you want to select (comma-separated for multiple jobs): ").strip().split(',')
                    job_names = [job.strip() for job in job_names]
                    
                    start_times = {}
                    for job_name in job_names:
                        start_time_str = input(f"Enter the start time for the migration '{job_name}' (in HH:MM:SS format): ")
                        start_time = datetime.strptime(start_time_str, "%H:%M:%S").time()
                        start_times[job_name] = start_time
                    
                    days_to_run = int(input("Enter the number of days to run the job (including today): "))
                    
                    now = datetime.now()
                    for job_name, start_time in start_times.items():
                        for day in range(days_to_run):
                            start_datetime = datetime.combine(now + timedelta(days=day), start_time)
                            delay_seconds = (start_datetime - now).total_seconds()
                            start_time_12hr = start_datetime.strftime("%I:%M:%S %p")
                            print(f"Scheduled to start '{job_name}' at: {start_time_12hr} on {start_datetime.date()}")
                            threading.Timer(delay_seconds, self.main_migration_func, [job_name, postgres_conn]).start()

            else:
                print("Invalid choice. Please enter 'new' or 'existing'.")
                self.main()
    
    def get_migration_details(self, job_name, conn,migration_table):
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

    
    def main_migration_func(self,job_name,postgres_conn):
        # Implement the main migration function
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
        
        # migration_table='migrations_2'

        time_column_check=migration_details_dict['time_column_check']
        primary_id_col=migration_details_dict['primary_id_column']
        last_id=migration_details_dict['last_id']
        if last_id:
            last_id=self.load_last_id(last_id)
            print(f"last_id after load_last_id is {last_id}")
        migration_status=migration_details_dict['status']

        # print(f"from connection {from_connection} and to connection {to_connection}")
        # print(f"table_flag is {table_flag} and {type(table_flag)}")
        if table_flag:
            print(f"############### Table to Table migration")            
            from_table=migration_details_dict['from_table']
            if not self.is_valid_table_name(from_table):
                full_from_table=f'[{from_database}].[dbo].[{from_table}]'
                # print(f"full table name {full_from_table}")    
            else:
                full_from_table=from_table       
            

            if migration_status=='first':
                # print(f"no records are migrated since begining so migrating all")
                df=self.get_all_records(from_connection,full_from_table,primary_id_col)
                df = df.astype(object).mask(df.isna(), None)
                print(f"Dataframe got is \n {df.head()}")
                print(f"The number of rows in the DataFrame is: {df.shape[0]}")
                print(f'the last 5 rows:{df.tail(5)}')
                insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,to_table,primary_id_col)
                if last_id_time is None:
                    
                    last_id_time=self.update_last_id(to_db_name,to_table,primary_id_col)
                    
                migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)

                print(f"############################# Details to update in table \n Status:{migration_details_dict_updated['status']},Last_ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")
                print(f"Updating table")
                uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})
            
            else:
                # from_connection=self.create_connection(from_db_type,from_host,from_database,from_user,from_pwd,from_port,from_driver)
                # to_connection=self.create_connection(to_db_type,to_hostname,to_db_name,to_user,to_password,to_port)
                if not last_id:
                    last_id=self.get_last_record_id(to_connection,to_table,primary_id_col)
                
                print(f"##################### The last id migrated is {last_id} from column {time_column_check}")
                migrate_records=f"select * from {full_from_table} where {time_column_check} > '{last_id}'"
                
                migrate_records_df = self.execute_query(from_connection,migrate_records)
                if migrate_records_df.empty:
                    print(f"no records feteched in query execution")
                migrate_records_df = migrate_records_df.astype(object).mask(migrate_records_df.isna(), None)
                print(f"query result is \n {migrate_records_df.head()}")
                print(f"The number of rows in the DataFrame is: {migrate_records_df.shape[0]}")
                insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,migrate_records_df,to_table,primary_id_col)
                if last_id_time is None:
                    
                    last_id_time=self.update_last_id(to_db_name,to_table,primary_id_col)
                    
                migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)

                print(f"######################### Details to update in table \n Status:{migration_details_dict_updated['status']},Last ID:{migration_details_dict_updated['last_id']},Migration Update:{migration_details_dict_updated['migration_update']}")

                print(f"Updating table")
                uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})    

        else:
            print(f"@@@@@@@@@@@@@@@@@@ Executing {job_name}")
            from_query=migration_details_dict['from_query']
            print(f"######################## The query is {from_query}")
            print(f"########################## the result to be saved on {to_db_name},{to_table}")
            time_column_check=migration_details_dict['time_column_check']
            primary_id_col=time_column_check
            last_id=migration_details_dict['last_id']
            migration_status=migration_details_dict['status']

            if last_id==0 and migration_status=='first':
                df=self.execute_query(from_connection,from_query)
                df = df.astype(object).mask(df.isna(), None)
                print(f"DataFrame is \n {df.head()}")
                print(f"The number of rows in the DataFrame is: {df.shape[0]}")
                print(f'the last 5 rows:{df.tail(1)}')
                print(df.iloc[-1])
                
                insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,to_table,primary_id_col)
                if last_id_time is None:
                    print(f"last_id is {last_id_time}")
                    last_id_time=self.update_last_id(to_db_name,to_table,primary_id_col)
                    print(f"last_id after {last_id_time}")
                
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
                    last_id=self.get_last_record_id(to_connection,to_table,primary_id_col)

                print(f"##################### The last id migrated is {last_id} from column {time_column_check}")
                df=self.execute_query(from_connection,from_query)
                df = df.astype(object).mask(df.isna(), None)
                print(f"query result is \n {df.head()}")
                print(f"The number of rows in the DataFrame is: {df.shape[0]}")                
                
                insert_records,success,failures,last_id_time=self.insert_records_postgresql_batch_5(to_connection,df,to_table,primary_id_col)
                if last_id_time is None:                    
                    last_id_time=self.update_last_id(to_db_name,to_table,primary_id_col)
                   
                if insert_records:
                    print(f"############################## Migration Successfull")
                else:
                    print(f"Errors in Migration")
                
                migration_details_dict_updated=self.update_migration_details_dict(migration_details_dict,insert_records,success,failures,last_id_time,db_config)               
                print(f"############################### Details to update in table \n Status:{migration_details_dict_updated['status']},Migration Update:{migration_details_dict_updated['migration_update']},Status:{migration_details_dict_updated['last_id']}")

                print(f"Updating table")
                uu=self.update_table(postgres_conn,migration_table,migration_details_dict_updated,{'migration_name':job_name})    



    def get_all_records(self,connection,table_name,primary_id_col):
        try:
            df_rows=f"select * from {table_name} order by {primary_id_col}"
            df = self.execute_query(connection,df_rows)
            df = df.astype(object).mask(df.isna(), None)
            return df
            
        except Exception as e:
            print(f"Error while fetching all records {e}")
            return None
        
    def load_last_id(self,last_id_str):
        try:
            # Attempt to convert to integer
            last_id_int = int(last_id_str)
            return last_id_int  # Return as integer if successful
        except ValueError:
            try:
                # Attempt to convert to datetime
                last_id_timestamp = datetime.strptime(last_id_str, '%Y-%m-%d %H:%M:%S')
                return last_id_timestamp
            except ValueError:            
                try:
                    last_id_timestamp = datetime.fromisoformat(last_id_str)
                    return last_id_timestamp  # Return as datetime if successful
                except ValueError:
                    try:
                        last_id=json.loads(last_id_str)
                        return last_id
                    except ValueError:
                        print(f"Error while loading last_id from db")                
                        return None # Return None if conversion fails
        
    def get_last_record_id(self,to_conn, table_name,primary_id_col):
        try:
            with to_conn.cursor() as cursor:
                query = f"SELECT {primary_id_col} FROM {table_name} ORDER BY id DESC LIMIT 1;"
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    return result[0]
                else:
                    return None
        except Exception as e:
            print(f"Error retrieving last record ID: {e}")
            return None
        
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
        only good for shorter records
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
    
    def insert_records_postgresql_batch_5(self, postgres_connection, df, insert_table_name, migration_track_col):
        print(f"Inserting into table {insert_table_name}, column {migration_track_col}")

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
            last_id_time = batch_df[migration_track_col].iloc[-1]
            if batch_index == 4:
                break

        total_end_time = time.time()
        print(f"Total insertion time: {total_end_time - total_start_time:.2f} seconds")

        last_id_time_ = self.jsonify_last_id(last_id_time) if last_id_time else None
        print(f"last_id_time_: {last_id_time_}")

        return insert_flag, inserted_records, failed_records, last_id_time_

    def log_failed_batch(self,batch_index, error):
        with open('error_log.txt', 'a') as f:
            f.write(f"Failed batch details:\nBatch index: {batch_index}\nError: {str(error)}\n\n")
        
    def jsonify_last_id(self,last_id_time):
        try:
            if isinstance(last_id_time, datetime):
                print(f"last_id_time in insert recs {last_id_time} and type {type(last_id_time)}  ")
                json_datetime = last_id_time.isoformat()
                print("JSON-compatible datetime:", json_datetime)

                # Example JSON data containing the datetime string
                json_data = json.dumps({"timestamp": json_datetime})
                print("JSON data:", json_data)

                # Parse the JSON data back to a dictionary
                parsed_data = json.loads(json_data)
                print("Parsed JSON data:", parsed_data)

                dt_from_json = datetime.fromisoformat(parsed_data["timestamp"])
                print("Converted back to datetime:", dt_from_json)
                return dt_from_json
            else:
                # print(f"last_id_time is not datetime obj")
                last_id=json.dumps(last_id_time)
                print(f"returning last_id {last_id}")
                return last_id
        
        except Exception as e:
            print(f"Error in jsonify {e}")

    def update_last_id(self,db_name,insert_table_name,migration_track_col):
        try:
            postgres_connection=self.create_postgres_connection(db_name)           
            last_id_time=None           
            with postgres_connection.cursor() as cursor:
                query=f"SELECT {migration_track_col} FROM {insert_table_name} ORDER BY {migration_track_col} DESC LIMIT 1"                
                cursor.execute(query)
                result = cursor.fetchone()                
                if result:
                    last_id_time = result[0]
                    last_id_time=self.jsonify_last_id(last_id_time)
        except Exception as fetch_error:
            last_id_time=None
            print(f"Error fetching last inserted record: {fetch_error}")
        return last_id_time

if __name__ == "__main__":
    scheduler = MigrationScheduler()
    scheduler.main()
