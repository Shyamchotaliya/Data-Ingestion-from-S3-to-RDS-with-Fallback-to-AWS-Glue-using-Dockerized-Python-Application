import os
import boto3
import pandas as pd
from sqlalchemy import create_engine
import logging

# --- Configuration ---
S3_BUCKET = 'project6-data-bucket-shyam'
S3_KEY = 'students.csv'
RDS_HOST = 'student-database.cq5uge448u0v.us-east-1.rds.amazonaws.com'
RDS_USER = 'admin'
RDS_PASSWORD = 'Shyam22062003'  
RDS_DB_NAME = 'studentdb'
GLUE_DB_NAME = 'student_data_fallback'
GLUE_TABLE_NAME = 'students_fallback'
AWS_REGION = 'us-east-1'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Main ETL function."""
    logging.info("Starting ETL process...")
    
    # --- 1. Extract data from S3 ---
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        df = pd.read_csv(response.get("Body"))
        logging.info("Successfully extracted data from S3.")
        logging.info(f"Data has {len(df)} rows.")
    except Exception as e:
        logging.error(f"Error extracting from S3: {e}")
        return  # Stop execution if S3 fails

    # --- 2. Load data to RDS (Primary Target) ---
    try:
        logging.info("Attempting to connect to RDS...")
        connection_str = f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}/{RDS_DB_NAME}"
        engine = create_engine(connection_str)
        
        with engine.connect() as connection:
            df.to_sql('students', con=connection, if_exists='append', index=False)
            logging.info("Successfully loaded data into RDS.")
            
    except Exception as rds_error:
        logging.warning(f"Could not connect to RDS. Reason: {rds_error}")
        logging.info("Initiating fallback to AWS Glue.")
        
        # --- 3. Fallback: Load metadata to AWS Glue ---
        try:
            glue_client = boto3.client('glue', region_name=AWS_REGION)
            
            # Convert pandas dtype to Glue-compatible type
            type_mapping = {'object': 'string', 'int64': 'int', 'float64': 'double', 'bool': 'boolean'}
            columns = [{'Name': col, 'Type': type_mapping.get(str(dtype), 'string')} for col, dtype in df.dtypes.items()]

            glue_client.create_table(
                DatabaseName=GLUE_DB_NAME,
                TableInput={
                    'Name': GLUE_TABLE_NAME,
                    'Description': 'Fallback table for student data when RDS is down.',
                    'StorageDescriptor': {
                        'Columns': columns,
                        'Location': f's3://{S3_BUCKET}/',
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                            'Parameters': {'field.delim': ','}
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {'has_encrypted_data': 'false'}
                }
            )
            logging.info(f"Successfully created fallback table '{GLUE_TABLE_NAME}' in AWS Glue.")
            
        except Exception as glue_error:
            logging.error(f"Fallback to Glue also failed. Reason: {glue_error}")

if __name__ == "__main__":
    main()
