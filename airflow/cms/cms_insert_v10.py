import json
import gzip
import pandas as pd
import os
import psycopg2
from io import StringIO
import glob
import logging
import time
import sys
from datetime import datetime
# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'cms_insert_{datetime.now().isoformat(sep=" ")[:10]}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_multiple_files_to_postgres(config_file_path):
    logger.info("Starting the load process")
    try:
        # Read configurations from the JSON file
        try:
            with open(config_file_path, 'r') as config_file:
                config = json.load(config_file)
                logger.info("Configuration file loaded successfully")
        except FileNotFoundError:
            logger.error("Configuration file not found")
            return
        except json.JSONDecodeError:
            logger.error("Error decoding JSON configuration file")
            return

        # Extract parameters from the configuration
        gz_files_pattern = config.get('gz_files_pattern')
        #gz_files_pattern = sys.argv[1]
        db_params = config.get('db_params')

        if not gz_files_pattern or not db_params:
            logger.error("Invalid configuration: 'gz_files_pattern' or 'db_params' missing")
            return

        # Connect to PostgreSQL
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            logger.info("Connected to PostgreSQL database")
        except psycopg2.Error as e:
            logger.error("Error connecting to the PostgreSQL database", exc_info=True)
            return

        # Find all files matching the pattern
        gz_files = glob.glob(gz_files_pattern)
        logger.info(f"Found {len(gz_files)} files matching the pattern")

        for gz_file_path in gz_files:
            logger.info(f"Processing file: {gz_file_path}")
            try:
                # List to hold DataFrames for each file
                df_list = []

                # Extract the filename from the full path
                gz_filename = os.path.basename(gz_file_path)

                # Read the gz file
                with open(gz_file_path, 'rt') as gz_file:
                    # Load the CSV data into a pandas DataFrame
                    df = pd.read_csv(gz_file, sep=';', header=None, 
                                     names=['trigger_time', 'msisdn', 'action_name', 'logic_id', 'content_id', 'pay_type'])
                    # Convert triggertime column to datetime format
                    df['trigger_time'] = pd.to_datetime(df['trigger_time'], format='%Y%m%d%H%M%S')
                    # Add the source_file column with the filename
                    df['source_file'] = gz_filename
                    # Append the DataFrame to the list
                    df_list.append(df)

                # Concatenate all DataFrames
                combined_df = pd.concat(df_list, ignore_index=True)
                #logger.info(f"File {gz_filename} loaded into DataFrame")

                # Convert DataFrame to list of tuples
                data = [tuple(row) for row in combined_df.values]

                # Create a placeholder string for the VALUES clause
                placeholders = ','.join(['%s'] * len(combined_df.columns))

                # Construct the full INSERT statement using executemany
                insert_query = '''
                    INSERT INTO cms.send_report (trigger_time, msisdn, action_name, logic_id, content_id, pay_type, source_file)
                    VALUES ({})
                    '''.format(placeholders)

                # Execute the INSERT statement with executemany
                try:
                    cur.executemany(insert_query, data)
                    logger.info(f"Data from file {gz_filename} inserted into the database")

                    # Commit changes
                    conn.commit()
                    logger.info(f"Changes committed to the database for file {gz_filename}")
                except psycopg2.Error as e:
                    logger.error(f"Error inserting data from file {gz_filename}", exc_info=True)
                    conn.rollback()
                    continue

                # Remove gz file
                try:
                    os.remove(gz_file_path)
                    logger.info(f"File {gz_filename} removed after processing")
                except OSError as e:
                    logger.error(f"Error removing file {gz_filename}", exc_info=True)

            except Exception as e:
                logger.error(f"An error occurred while processing file {gz_filename}", exc_info=True)
                continue

        # Close the connection
        try:
            cur.close()
            conn.close()
            logger.info("Database connection closed")
        except psycopg2.Error as e:
            logger.error("Error closing the database connection", exc_info=True)
    except Exception as e:
        logger.error("An unexpected error occurred", exc_info=True)

# Path to the configuration file
config_file_path = '/comptel/ccacp/bonyan/scripts/CmsReport/config.json'

# Load data to PostgreSQL
load_multiple_files_to_postgres(config_file_path)
