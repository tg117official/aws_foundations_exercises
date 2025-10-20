from pyspark.sql import SparkSession
import boto3
import pandas as pd
import os

# Set Up AWS Access Keys Using ENV vars

# AWS credentials and configuration (Make sure your AWS credentials are set up)
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')  # Change the region if needed
table = dynamodb.Table('cdc_tracking_01')

def get_data_from_dynamodb():
    try:
        # Scan the entire table to retrieve all items
        response = table.scan()
        items = response.get('Items', [])

        # Print the data
        for item in items:
            record_id = item.get('record_id', 'N/A')
            lastmodified = item.get('lastmodified', 'N/A')
            print(f"Record ID: {record_id}, Last Modified: {lastmodified}")

        return items
    except Exception as e:
        print(f"Error fetching data from DynamoDB: {e}")
        return []


# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb', region_name='us-east-2')  # Change region_name if necessary

# Function to update the lastmodified value
def update_lastmodified_value(record_id, lastmodified_value):
    try:
        # Update the item in DynamoDB
        dynamodb.put_item(
            TableName='cdc_tracking_01',
            Item={
                'record_id': {'S': record_id},
                'lastmodified': {'N': str(lastmodified_value)}  # Ensure the numeric value is passed as a string
            }
        )
        print(f"Successfully updated 'lastmodified' to {lastmodified_value} for record_id '{record_id}'.")
    except Exception as e:
        print(f"Error updating DynamoDB: {e}")


# Call the function to update the value
update_lastmodified_value('orders', 0)
update_lastmodified_value('products', 0)
update_lastmodified_value('customers', 0)


# Fetch data from the DynamoDB table
dynamodb_data = get_data_from_dynamodb()