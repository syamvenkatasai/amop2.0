
import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
import logging
import csv
from io import StringIO
import psycopg2
from datetime import datetime

# Initialize clients
s3_client = boto3.client('s3')

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# OpenSearch client configuration
def opensearch_client():
    host = 'https://search-amopsearch-df66xuwugs7f6b43ihav5yd5zm.us-east-1.es.amazonaws.com'
    auth = ('admin', 'Amopteam@123')  # Use your OpenSearch credentials

    logging.debug(f"Creating OpenSearch client with host: {host}")
    
    return OpenSearch(
        [host],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        timeout=60
    )

# PostgreSQL connection
def get_db_connection():
    return psycopg2.connect(
        dbname="AmopDB",
        user="root",
        password="AmopTeam123",
        host="amoppostgres.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
        port="5432"
    )


def parse_datetime(value):
    if value in (None, 'null', 'NULL', ''):
        return None
    if isinstance(value,str):
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
            "_source": doc
        }
        actions.append(action)
        print(action)
    
    try:
        response = helpers.bulk(es, actions)
        print(f"Successfully indexed {response[0]} documents for {table_name}")
    except Exception as e:
        print(f"Error indexing documents: {e}")


# Function to get table schema
def get_table_schema(conn, table_name):
    query = """
        SELECT column_name,data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """
    print("********************")
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        columns = cur.fetchall()
    
    return {column[0]: column[1] for column in columns}

# Lambda handler
def lambda_handler(event, context):
    logging.debug(f"Received event: {json.dumps(event)}")
    es = opensearch_client()
    conn = get_db_connection()  # Create a database connection
    
    # Process each record in the event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        # Log the received event details
        logging.debug(f"Processing file: {key} from bucket: {bucket}")
        
        try:
            # Get the object from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read().decode('utf-8')  # Read the CSV file content
            logging.debug(f"Successfully fetched object {key} from bucket {bucket}.")
            logging.debug(f"Contents of the CSV file: {data}")  # Log the contents of the CSV
        except Exception as e:
            logging.error(f"Error fetching object {key} from bucket {bucket}: {str(e)}")
            continue

        try:
            csv_reader = csv.reader(StringIO(data))  # Read the CSV as a list
            
            key_parts = key.split('/')  # Split the key, e.g., 'DMS/public/amop_apis/20241011-095606472.csv'
    
            # Extract the part that represents the index name (3rd part in this example)
            index_name = key_parts[2] if len(key_parts) > 2 else ''  # Fetch 'amop_apis'
            
            print(f"Derived index name from key: {index_name}")
                    
            rows = []
            for row in csv_reader:
                if not row:
                    continue
        
                operation = row[0] # First column indicates the operation (Insert/Update/Delete)
                #index_name = "amop_apis"  # Deriving index name from the file name (use your actual index name)
                print(f"Index name derived from file: {index_name}")
                
                # Fetch actual column names from the PostgreSQL database
                column_names = get_table_schema(conn, index_name)
                print(f"Column names for index '{index_name}': {column_names}")
                print(f"Row data: {row}", column_names)
                
                schema = get_table_schema(conn, index_name)
                logging.debug(f"Schema for index '{index_name}': {schema}")
                # Collect rows for bulk indexing
                rows.append((operation, row[1:]))  # Include operation type with row data
                print("============",rows)
        
            # Prepare the batch of data for bulk indexing
            actions = []
            for operation, row in rows:
                json_data = {}
                doc_id = row[0]
                for i, column_name in enumerate(schema.keys()):
                    if i == 0:  # Skip the operation type column
                        continue
                    
                    value = row[i] if i < len(row) else None
                    # value = row[i + 1] if i + 1 < len(row) else None
                    print("---------------",value)
                    data_type = schema[column_name]
                    json_data[column_name] = convert_value(value, data_type)
        
                # Determine the action type (Insert, Update, Delete)
                action = {}
               # Assuming the first column is the unique ID
                print("%%%%%%%%%%",doc_id)
                if operation == 'I':  # Insert operation
                    action = {
                        "_op_type": "index",  # Use "index" to insert or overwrite documents
                        "_index": index_name,
                        "_id": doc_id,  # Assuming ID is in the first column
                        "_source": json_data
                    }
                    print(f"Prepared insert action for document with ID {doc_id}")
                elif operation == 'U':  # Update operation
                    action = {
                        "_op_type": "update",  # Using "index" again for update, can use "update" if preferred
                        "_index": index_name,
                        "_id": doc_id,
                        "doc": json_data
                    }
                    print(f"Prepared update action for document with ID {doc_id}")
                elif operation == 'D':  # Delete operation
                    action = {
                        "_op_type": "delete",
                        "_index": index_name,
                        "_id": doc_id
                    }
                    print(f"Prepared delete action for document with ID {doc_id}")
                else:
                    print(f"Unknown operation '{operation}' for document ID {doc_id}")
                    continue

                actions.append(action)
                print(f"Prepared action: {action}")

            # Perform bulk indexing
            if actions:
                try:
                    from opensearchpy import helpers  # Importing helpers for bulk indexing
                    
                    response = helpers.bulk(es, actions)
                    print(f"Successfully processed bulk operation. {response[0]} documents affected.")
                except Exception as e:
                    print(f"Error during bulk operation: {str(e)}")
        
        except Exception as e:
            print(f"Error processing CSV data from S3 object {key}: {str(e)}")
        finally:
            # Clean up database connection
            conn.close()




    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed S3 event')
    }
