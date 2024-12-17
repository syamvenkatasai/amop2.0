import copy
import os
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
from pandas import Timestamp
from common_utils.daily_migration_management.migration_api import MigrationScheduler
# from migration_management.migration_api import MigrationScheduler
import boto3
from botocore.exceptions import ClientError

import logging



def find_jobs():
        scheduler=MigrationScheduler()
        """
            main : this is responsible for fetching the jobs that are scheduled to be run from data base and starting a thread
            for each jon that is scheduled
        """
        load_dotenv()
#         LOCAL_DB_HOST=amoppostgres.c3qae66ke1lg.us-east-1.rds.amazonaws.com
# LOCAL_DB_PORT=5432
# MIGRATION_TABLE=migrations
# LOCAL_DB_USER=root
# LOCAL_DB_PASSWORD=AmopTeam123
# LOCAL_DB_TYPE=postgresql
# DF_SIZE=50000
 
# FROM_DB_HOST=awx-central.cnikycxqaajm.us-east-1.rds.amazonaws.com
# FROM_DB_PORT=1433
# FROM_DB_USER=ALGONOX-Vyshnavi
# FROM_DB_PASSWORD=cs!Vtqe49gM32FDi
# FROM_DB_TYPE=mssql
# FROM_DB_DRIVER={ODBC Driver 17 for SQL Server}
        
        print(f"Begining Migration")
        hostname = "amoppostoct19.c3qae66ke1lg.us-east-1.rds.amazonaws.com" #"amoppostgres.c3qae66ke1lg.us-east-1.rds.amazonaws.com"
        port = "5432"
        db_name = 'Migration_Test'
        user = "root"
        password = "AmopTeam123"
        db_type = "postgresql"
        postgres_conn = scheduler.create_connection(db_type, hostname, db_name, user, password, port)
        migration_table="migrations"
        job_scheduled_query=f"select migration_name from {migration_table} where schedule_flag is True order by migration_order asc"
        print(f"job query {job_scheduled_query}")
        rows = scheduler.execute_query(postgres_conn, job_scheduled_query)
        # print(rows)
        job_names_list=[]
        threads = []
        for index, row in rows.iterrows():
            job_name = row['migration_name']
            job_names_list.append(job_name)
        print("@@@########################",job_names_list)
        
        send_jobs_to_sqs(job_names_list)

        return job_names_list

def send_jobs_to_sqs(job_names_list):
    # Initialize SQS client
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/008971638399/Migration-JOBs.fifo'

    for job_name in job_names_list:
        try:
            # Send message to SQS queue
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=job_name,
                MessageGroupId='migration_jobs_group',  # Required for FIFO queues
                MessageDeduplicationId=job_name  # Deduplication based on the job name
            )
            print(f"Sent job {job_name} to SQS. MessageId: {response['MessageId']}")
        except ClientError as e:
            print(f"Failed to send job {job_name} to SQS. Error: {str(e)}")
