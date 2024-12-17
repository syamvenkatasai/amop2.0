import psycopg2
from psycopg2 import sql
import re
from dotenv import load_dotenv
import os
from common_utils.logging_utils import Logging
logging = Logging()

def create_provider_commands(server_name,target):
    # logging.info(f"server_name is {server_name} \n target_db is {target}")
    load_dotenv()
    host = os.getenv('LOCAL_DB_HOST')
    port = os.getenv('LOCAL_DB_PORT')
    user = os.getenv('LOCAL_DB_USER')
    password = os.getenv('LOCAL_DB_PASSWORD')
    # postgres_conn=psycopg2.connect(dbname=target, user=user, password=password, host=host, port=port)
    commands=[]
    create_fdw=f"CREATE EXTENSION IF NOT EXISTS postgres_fdw;"    
    create_fdw_server=f"""CREATE SERVER provider_{server_name} 
    FOREIGN DATA WRAPPER postgres_fdw 
    OPTIONS (host '{host}', dbname '{server_name}');"""
    create_fdw_user=f"""CREATE USER MAPPING FOR current_user
    SERVER provider_{server_name}
    OPTIONS (user '{user}', password '{password}');
    """
    create_schema=f"CREATE SCHEMA provider_{server_name}_schema;"
    import_fdw_tables=f"""IMPORT FOREIGN SCHEMA public
    FROM SERVER provider_{server_name}
    INTO provider_{server_name}_schema;"""
    commands.append(create_fdw)
    commands.append(create_fdw_server)
    commands.append(create_fdw_user)
    commands.append(create_schema)
    commands.append(import_fdw_tables)
    return commands



def create_provider_main(ui_partner_name,ui_target_name):
    try:
        partner,target=get_tenant_names(ui_partner_name,ui_target_name)
        logging.info(f"Make the {partner} provider for {target}")
        if partner and target:
            commands_list=create_provider_commands(partner,target)
            # for command in commands_list:
            #     print(command)
            # Execute commands in the database connection
            conn = None
            try:
                conn = psycopg2.connect(dbname=target, user=os.getenv('LOCAL_DB_USER'), 
                                        password=os.getenv('LOCAL_DB_PASSWORD'), 
                                        host=os.getenv('LOCAL_DB_HOST'), 
                                        port=os.getenv('LOCAL_DB_PORT'))
                cursor = conn.cursor()

                for command in commands_list:
                    # print(f"Executing command: {command}")
                    cursor.execute(command)  # Ensure this line executes the command
                conn.commit()
            
                return_flag=True
                return_schema_name=f'provider_{partner}_schema'

            except Exception as e:
                logging.error(f"An error occurred: {e}")
                return_flag=False
                return_schema_name=f'provider_{partner}_schema'
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            
        else:
            return_flag=False
            return_schema_name=None
            raise ValueError(f"Error while getting tenant names")
    except Exception as e:
        logging.error(f"Error in making partner provider check log")
        return_flag=False
        return_schema_name=None
    return return_flag,return_schema_name
    
            

def get_tenant_names(provider_partner, target):
    load_dotenv()
    print(f"UI names: {provider_partner}, {target}")
    
    # Initialize variables
    source_server, target_db = None, None

    # Connect to the database
    try:
        with psycopg2.connect(dbname='common_utils', user=os.getenv('LOCAL_DB_USER'),
                              password=os.getenv('LOCAL_DB_PASSWORD'),
                              host=os.getenv('LOCAL_DB_HOST'),
                              port=os.getenv('LOCAL_DB_PORT')) as conn:
            
            with conn.cursor() as cursor:
                # Prepare and execute the query
                query = "SELECT tenant_name, db_name FROM tenant WHERE tenant_name IN (%s, %s)"
                # print(f"Executing query: {query}")
                
                cursor.execute(query, (provider_partner, target))
                results = cursor.fetchall()
                
                # Convert results to a dictionary
                tuple_dict = {key: value for key, value in results}
                # print(f"Results: {tuple_dict}")

                # Assign appropriate values to source_server and target_db
                source_server = tuple_dict.get(provider_partner)
                target_db = tuple_dict.get(target)

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"An error occurred: {error}")
        source_server, target_db = None, None
        # raise
    finally:
        logging.info("Database connection closed.")

    # Return the extracted values
    return source_server, target_db


# if __name__ == "__main__":
#     # Example input
#     provider='Go Technologies'
#     target='LiveU'    
#     result_flag,result_schema=create_provider_main(provider,target)
#     print(result_flag,result_schema)