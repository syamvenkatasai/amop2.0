from opensearchpy import OpenSearch, helpers
import psycopg2
from datetime import datetime
import uuid
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pandas as pd
import boto3
import psycopg2
import io
import concurrent.futures
import logging
import time
from io import BytesIO
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import threading  # For asynchronous execution
import time
from io import StringIO
import json

# Connect to OpenSearch
es = OpenSearch(
    ['https://search-amopsearchuat-dzxpse4exyj37kauestkbgeady.us-east-1.es.amazonaws.com'],
    http_auth=('admin', 'Amopteam@123'),
    use_ssl=True,
    verify_certs=True,
    timeout=60
)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="altaworx_central",
    user="root",
    password="AmopTeam123",
    host="amopuatpostgresoct23.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
    port="5432"
)

def get_table_schema(conn, table_name):
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        columns = cur.fetchall()
    
    return {column[0]: column[1] for column in columns}

def parse_datetime(value):
    if value in (None, 'null', 'NULL', ''):
        return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).isoformat()
        except ValueError:
            try:
                return datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f').isoformat()
            except ValueError:
                print(f"Warning: Unable to parse date time string: {value}\n")
                return value
    elif isinstance(value, datetime):
        return value.isoformat()
    return value

def convert_value(value, data_type):
    if value in (None, 'null', 'NULL', ''):
        return None
    if data_type == 'boolean':
        return str(value).lower() in ('true', '1')
    elif data_type == 'timestamp without time zone':
        return parse_datetime(value)
    elif data_type == ('character varying', 'text'):
        return str(value)
    elif data_type == 'integer':
        return int(value)
    elif data_type == 'real':
        return float(value)
    elif data_type == 'uuid':
        try:
            return str(uuid.UUID(value))
        except ValueError:
            print(f"Warning: Invalid UUID format: {value}\n")
            return value
    elif data_type == 'json':
        if isinstance(value, list):
            return value  # Directly return if the value is already a list
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            print(f"Warning: Invalid JSON format: {value}\n")
            return value
    return value

def bulk_index_data(batch_data):
    table_name, rows, schema = batch_data
    actions = []
    for item in rows:
        doc = {}
        for i, column_name in enumerate(schema.keys()):
            value = item[i]
            data_type = schema[column_name]
            converted_value = convert_value(value, data_type)
            doc[column_name] = converted_value
        
        action = {
            "_op_type": "index",  # Use "update" if you want to update existing documents
            "_index": table_name,
            "_source": doc,
            "_id": doc.get("id")
        }
        actions.append(action)
    
    try:
        response = helpers.bulk(es, actions)
        print(f"Successfully indexed {response[0]} documents for {table_name}")
    except Exception as e:
        print(f"Error indexing documents: {e}")

def fetch_and_bulk_index_data(table_name):
    schema = get_table_schema(conn, table_name)
    columns_list = ', '.join([f'"{col}"' for col in schema.keys()])
    query = f"SELECT {columns_list} FROM {table_name} "
    
    with conn.cursor() as cur:
        cur.execute(query)
        # print(rows)
        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            batch_count=batch_count+1
            if not rows:
                break
            yield (table_name, rows, schema)
        
def fetch_table_names():
    table_query = """
        select search_tables from open_search_index;
        """
    try:
        with conn.cursor() as cur:
            cur.execute(table_query)
            tables= cur.fetchall()
            # Extract table names from the query results
            print("tables",tables)
    except psycopg2.Error as e:
        print(f"Error fetching table names: {e}")
    
    index_tables = []
    for item in tables:
        # Check if the item is a tuple with a single string element
        if isinstance(item, tuple) and len(item) == 1:
            value = item[0]
            if value.startswith("{") and value.endswith("}"):
                # Remove curly braces and split by comma
                value = value.strip("{}").replace("'", "")
                tables = [table.strip() for table in value.split(",")]
                index_tables.extend(tables)
            else:
                index_tables.append(value)
    
    index_tables = list(set(index_tables))
    #index_tables=["optimization_group"]
    print("---------------",index_tables)
    return index_tables

def fetch_data_with_pagination(table_name, batch_size, start_row=0):
    schema = get_table_schema(conn, table_name)
    columns_list = ', '.join([f'"{col}"' for col in schema.keys()])
    query = f"SELECT {columns_list} FROM {table_name} LIMIT %s OFFSET %s"
    
    with conn.cursor() as cur:
        while True:
            cur.execute(query, (batch_size, start_row))
            rows = cur.fetchall()
            if not rows:
                break
            yield (table_name, rows, schema)
            start_row += batch_size

def process_table(table_name):
    batch_size = 25000
    for batch_data in fetch_data_with_pagination(table_name, batch_size):
        print(F"got data")
        # table_name, rows, schema = batch_data
    # batches=fetch_and_bulk_index_data(table_name)
    # for i,batch_data in enumerate(batches):
    #     print(f"got batch {i}")
        bulk_index_data(batch_data)

def reindex_all():
    table_names = fetch_table_names()
    NUM_THREADS=4

    # Use multiprocessing Pool to parallelize the indexing process
    # with Pool(processes=NUM_PROCESSES) as pool:
    #     pool.map(process_table, table_names)
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(process_table, table) for table in table_names]
    print("Tejaswi**************")
    #cursor.close()
    conn.close()
    
    return {'index_Status': 'Indexing for all Docs '}

# def elasticsearch_indexing():
#     index_tables=fetch_table_names()
#     print("---------------",index_tables)
#     for table in index_tables:
#         fetch_and_bulk_index_data(table)
        

#def search_data(query, search_all_columns,index_name,):
def search_data(query, search_all_columns, index_name, start, end):
    if is_field_present(index_name, "is_active"):
        # Include the filter for `is_active=true`
        filter_clause = {
            "term": {
                "is_active": True
            }
        }
    else:
        # No filter for `is_active`
        filter_clause = None
    if search_all_columns:
        fields = ["*"]  # Search across all columns

   
    search_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": query,
                            "type": "phrase",
                            "fields": ["*"]  # Search across all fields
                        }
                    }
                ]
            }
        },
        "from": start * end,  # Calculate the offset
        "size": end  # Number of results per page
    }
    
    # Add the filter clause if `is_active` is present
    if filter_clause:
        search_body["query"]["bool"]["filter"] = [filter_clause]
    try:
        response = es.search(index=index_name, body=search_body)
        return response
    except Exception as e:
        print(f"An error occurred while searching: {e}")
        return None
        
def is_field_present(index_name, field_name):
    try:
        # Get the mapping for the index
        mapping = es.indices.get_mapping(index=index_name)
        # Check if the field exists in the mapping
        fields = mapping[index_name]['mappings']['properties']
        return field_name in fields
    except Exception as e:
        print(f"An error occurred while checking field presence: {e}")
        return False
        
def advance_search_data(filters,index_list,start, end):
    search_body = {
        "query": {
            "bool": {
                "must": []
            }
        },
        "from": start * end ,  # Start from the specified offset
        "size": end ,
         "track_total_hits": True # Number of results to return
    }

    # Add filters to the search body
    if filters:
        for field, values in filters.items():
            if values:  # Check if values is not empty
                if len(values) == 1:
                    # Single value, use term query
                    search_body["query"]["bool"]["must"].append({
                        "term": {
                            f"{field}.keyword": values[0]
                        }
                    })
                else:
                    # Multiple values, use terms query
                    search_body["query"]["bool"]["must"].append({
                        "terms": {
                             f"{field}.keyword": values
                        }
                    })
    print("Search body:", search_body)  
    
    # Add is_active filter if it exists in the index
    

    print("Search body:", search_body)
    
    try:
        response = es.search(index=index_list, body=search_body)
        print("Response:", response)  # Debugging line
        return response
    except Exception as e:
        print(f"An error occurred while searching: {e}")
        return None
        
    
    

    print("Search body:", search_body)  # Debugging line
    try:
        response = es.search(index=index_list, body=search_body)
        print("Response:", response)  # Debugging line
        return response
    except Exception as e:
        print(f"An error occurred while searching: {e}")
        return None




#used to check through multiple index but currently not using        
def search_across_indexes(query, index_list):
    data_set = ast.literal_eval(index_list)
    index_list = list(data_set)
    search_body = {
        "query": {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["*"],  # Fields from index1
                            "boost": 1.0,
                            "type":"most_fields"
                        }
                    },
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["*"],  # Fields from index2
                            "boost": 1.0,
                            "type":"most_fields"
                        }
                    },
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["*"],  # Fields from index3
                            "boost": 1.0,
                            "type":"most_fields"
                        }
                    }
                ]
            }
        }
    }
    
    try:
        response = es.search(index=index_list, body=search_body)
        return response
    except Exception as e:
        print(f"An error occurred while searching: {e}")
        return None

# AWS S3 client
s3_client = boto3.client('s3')


def export_inventory(data):
    S3_BUCKET_NAME = 'searchexcelssandbox'
    try:
        print("data--------", data)
        search_all_columns = data.get("search_all_columns", "true")
        index_name = data.get('index_name')
        cols = data.get("cols")  # Columns to include in the export
        search = data.get("search")
        print("@@@@@@@@@@@", search)
        
        try:
            pages = data.get("pages")
            start = int(pages.get("start", None))
            end = int(pages.get("end", None))
            print("^^^^^^^^^^", start, end)
        except:
            pass
        
        results = None
        sources = []
        pquery = f"select search_tables,index_search_type from open_search_index where search_module={index_name}"
        print("########", pquery)
        
        with psycopg2.connect(
            dbname="altaworx_central",
            user="root",
            password="AmopTeam123",
            host="amopuatpostgresoct23.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
            port="5432"
        ) as conn:
            with conn.cursor() as cur:
                pquery = f"SELECT search_tables, index_search_type FROM open_search_index WHERE search_module='{index_name}'"
                cur.execute(pquery)
                data1 = cur.fetchall()
    
        print("---------data1", data1)
        index_list = data1[0][0]
        search_type = data1[0][1]
    
        print("-----------", index_list, search_type)
        
        if search == "advanced":
            filters = data.get('filters')
            filters = {k: v for k, v in filters.items() if v}
            filters.pop('bulk_change_id', None)
            print("$$$$$$$$", filters)
            results = advance_search_data(filters, index_list, start, end)
        elif search == "whole":
            query = data.get("search_word")
            print("********", query)
            results = search_data(query, search_all_columns, index_list, start, end)
        else:
            print("incorrect type")
    
        total_rows = 0
        if results:
            hits = results.get('hits', {}).get('hits', [])
            total_rows = results['hits']['total']['value']
            for hit in hits:
                source = hit.get('_source', {})
                sources.append(source)
           # return {"flag":True,"results":sources}
            # Filter the columns if 'cols' are provided
            if cols:
                sources = [{key: value for key, value in source.items() if key in cols} for source in sources]
            
            # Create a DataFrame for the filtered data
            df = pd.DataFrame(sources)
            required_columns = [
                "service_provider_display_name", "imei", "rate_plan", "data_usage_mb", "sms_count", 
                "cost_center", "account_number", "customer_name", "date_added", "iccid", 
                "msisdn", "eid", "customer_name", "username", "data_usage_bytes", 
                "customer_rate_pool_name", "customer_rate_plan_name", "status_display_name", 
                "date_activated", "ip_address", "billing_account_number", "foundation_account_number", 
                "modified_by", "modified_date"
            ]
            
            # Filter the DataFrame to include only the required columns
            df = df[required_columns]
            df.columns = [col.replace('_', ' ').capitalize() for col in df.columns]
            df['S.No'] = range(1, len(df) + 1)
            columns = ['S.No'] + [col for col in df.columns if col != 'S.No']
            df = df[columns]
            # Convert to CSV (you can convert to Excel if you prefer)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Upload the CSV file to S3
            file_name = f"exports/Inventory_export.csv"
            csv_buffer.seek(0)  # Move to the start of the StringIO buffer

            # Upload to S3 (public or private based on your needs)
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=file_name,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )

            # Generate URL (public URL or pre-signed URL)
            download_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{file_name}"
            search_result = {
                'flag': True,
                'download_url': download_url  # Return the URL where the file is stored in S3
            }
            return search_result
        else:
            search_result = {
                "flag": True,
                "data": {
                    "table": sources
                },
                "pages": {
                    "start": start,
                    "end": end,
                    "total": total_rows
                }
            }
    
        return search_result
    except Exception as e:
        print(f"Exception is {e}")
        search_result = {
                "flag": False,
                "message": "No Records found"
            }
        return search_result
    

 


# def export_inventory(data):
#     print("data--------",data)
#     search_all_columns = data.get("search_all_columns", "true")
#     index_name=data.get('index_name')
#     cols=data.get("cols")
#     search=data.get("search")
#     print("@@@@@@@@@@@",search)
#     try:
#         pages=data.get("pages")
#         start=int(pages.get("start",None))
#         end=int(pages.get("end",None))
#         print("^^^^^^^^^^",start,end)
#     except:
#         pass
#     results=None
#     sources=[]
#     pquery=f"select search_tables,index_search_type from open_search_index where search_module={index_name}"
#     print("########",pquery)
#     with psycopg2.connect(
#         dbname="altaworx_central",
#         user="root",
#         password="AmopTeam123",
#         host="amopuatpostgresoct23.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
#         port="5432"
#     ) as conn:
#         with conn.cursor() as cur:
#             pquery = f"SELECT search_tables, index_search_type FROM open_search_index WHERE search_module='{index_name}'"
#             cur.execute(pquery)
#             data1 = cur.fetchall()
#     print("---------data1",data1)
#     index_list=data1[0][0]
#     search_type=data1[0][1]
#     #search_type="whole"

#     print("-----------",index_list,search_type)
#     if search=="advanced":
#         filters=data.get('filters')
#         filters = {k: v for k, v in filters.items() if v}
#         filters.pop('bulk_change_id', None)
#         #filters = {key: value[0] for key, value in filters.items()}
#         #filters = {f"{key}.keyword": value for key, value in filters.items()}
#         print("$$$$$$$$",filters)
#         results = advance_search_data(filters,index_list,start, end)
#         #results = advance_search_data(filters,index_list)
#     elif search=="whole":
#         query=data.get("search_word")
#         print("********",query)
#         results = search_data(query, search_all_columns,index_list,start,end)
#     else:
#         print("incorrect type")
#     total_rows=0
#     if results:
#         hits = results.get('hits', {}).get('hits', [])
#         total_rows = results['hits']['total']['value']
#         for hit in hits:
#             source = hit.get('_source', {})
#             sources.append(source)
#         #total_rows = len(sources)
#         search_result = {
#             "flag": True,
#             "data": {
#                 "table": sources
#             },
#             "pages":{
                
#                 "start":start,
#                 "end":end,
#                 "total": total_rows
#             }
#         }
        
#     else:
#         search_result = {
#             "flag": True,
#             "data": {
#                 "table": sources
#             },
#             "pages":{
#                 "start":start,
#                 "end":end,
#                 "total": total_rows
#             }
#         }
#     return search_result


def perform_search(data):
    print("data--------",data)
    search_all_columns = data.get("search_all_columns", "true")
    index_name=data.get('index_name')
    cols=data.get("cols")
    search=data.get("search")
    print("@@@@@@@@@@@",search)
    try:
        pages=data.get("pages")
        start=int(pages.get("start",None))
        end=int(pages.get("end",None))
        print("^^^^^^^^^^",start,end)
    except:
        pass
    results=None
    sources=[]
    pquery=f"select search_tables,index_search_type from open_search_index where search_module={index_name}"
    print("########",pquery)
    with psycopg2.connect(
        dbname="altaworx_central",
        user="root",
        password="AmopTeam123",
        host="amopuatpostgresoct23.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
        port="5432"
    ) as conn:
        with conn.cursor() as cur:
            pquery = f"SELECT search_tables, index_search_type FROM open_search_index WHERE search_module='{index_name}'"
            cur.execute(pquery)
            data1 = cur.fetchall()
    print("---------data1",data1)
    index_list=data1[0][0]
    search_type=data1[0][1]
    #search_type="whole"

    print("-----------",index_list,search_type)
    if search=="advanced":
        filters=data.get('filters')
        filters = {k: v for k, v in filters.items() if v}
        filters.pop('bulk_change_id', None)
        #filters = {key: value[0] for key, value in filters.items()}
        #filters = {f"{key}.keyword": value for key, value in filters.items()}
        print("$$$$$$$$",filters)
        results = advance_search_data(filters,index_list,start, end)
        #results = advance_search_data(filters,index_list)
    elif search=="whole":
        query=data.get("search_word")
        print("********",query)
        results = search_data(query, search_all_columns,index_list,start,end)
        
    else:
        print("incorrect type")
    total_rows=0
    if results:
        hits = results.get('hits', {}).get('hits', [])
        total_rows = results['hits']['total']['value']
        for hit in hits:
            source = hit.get('_source', {})
            sources.append(source)
        
        #total_rows = len(sources)
        search_result = {
            "flag": True,
            "data": {
                "table": sources
            },
            "pages":{
                
                "start":start,
                "end":end,
                "total": total_rows
            }
        }
        
    else:
        search_result = {
            "flag": True,
            "data": {
                "table": sources
            },
            "pages":{
                "start":start,
                "end":end,
                "total": total_rows
            }
        }
    return search_result


def fetch_dropdown(data):
    try:
        column_name=data.get("drop_down",None)
        table=data.get("table",None)
        flag=data.get("flag",None)
        conn = psycopg2.connect(
            dbname="altaworx_central",
            user="root",
            password="AmopTeam123",
            host="amopuatpostgresoct23.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
            port="5432"
        )
        cursor = conn.cursor()
        print("-------------",column_name)
        
        table_query= f"SELECT search_tables, index_search_type FROM open_search_index WHERE search_module='{table}'"
        cursor.execute(table_query)
        table_data=cursor.fetchall()
        table_name=table_data[0][0]
    
        if  flag == "status_history":
            iccid=data.get("iccid",None)
            iccid=iccid[0]
            query = f"SELECT DISTINCT {column_name} FROM {table_name} where iccid='{iccid}'"
            
        else:
            query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL AND {column_name} != 'None'"

        # Execute the query
        cursor.execute(query)
        # Fetch all unique values and return them as a list
        unique_values = cursor.fetchall()
        col_data=[value[0] for value in unique_values]
        return {"flag": "true" , column_name:col_data}

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()



