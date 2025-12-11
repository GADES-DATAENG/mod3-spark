import argparse
import logging
import requests
import csv
from io import StringIO
from pyspark.sql import SparkSession

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main():
    # Argument parser to get file location and BigQuery details
    parser = argparse.ArgumentParser(description="Read a file and write to BigQuery")
    parser.add_argument("--input_url", required=True, help="URL to input file (e.g., from GitHub or GCS)")
    parser.add_argument("--bq_project", required=True, help="GCP Project ID")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery Dataset Name")
    parser.add_argument("--bq_table", required=True, help="BigQuery Table Name")
    parser.add_argument("--gcp_keyfile", required=True, help="Path to GCP Service Account JSON key")

    args = parser.parse_args()

    input_url = args.input_url
    bq_project = args.bq_project
    bq_dataset = args.bq_dataset
    bq_table = args.bq_table
    gcp_keyfile = args.gcp_keyfile

    logger.info(f'Start extracting data for table {bq_table}')

    # Initialize Spark session with BigQuery support
    spark = SparkSession.builder \
        .appName(f"staging_{bq_table}_to_bigquery") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.project.id", bq_project) \
        .config("parentProject", bq_project) \
        .getOrCreate()
    
    #spark.sparkContext.addFile(gcp_keyfile)
    #key_path = SparkFiles.get("gcp_keyfile.json")
    logger.info('Spark Session built with success')

    try:
        # Check if input is a URL or local file path
        if input_url.startswith(('http://', 'https://')):
            logger.info("Downloading content from URL and distributing via Spark...")
            # Download the CSV content
            response = requests.get(input_url)
            
            if response.status_code != 200:
                raise Exception(f"Failed to download CSV. Status code: {response.status_code}")
            
            # Get the CSV content
            csv_content = response.text
            csv_reader = csv.reader(StringIO(csv_content))
            
            # Convert the rows to a list of lists (data)
            data_rows = list(csv_reader)
            # Extract header (first row)
            columns = data_rows[0]

            # Create an RDD from the data rows (excluding header)
            rdd = spark.sparkContext.parallelize(data_rows[1:])

            # Create DataFrame with the column names
            df = rdd.toDF(columns)
        else:
            logger.info(f"Reading local CSV file: {input_url}")
            # Read local CSV file directly with Spark
            df = spark.read.csv(input_url, header=True, inferSchema=True)
        
        df.count()
        logger.info(f"Successfully loaded {df.count()} rows")
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

    logger.info("Start saving data in BQ...")
    # Write to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"{bq_project}.{bq_dataset}.{bq_table}") \
        .option("writeMethod", "direct") \
        .option("credentialsFile", gcp_keyfile) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("datasetLocation", "europe-west4") \
        .mode("append") \
        .save()

    logger.info(f"Data successfully written to {bq_project}.{bq_dataset}.{bq_table}")

    spark.stop()

if __name__ == "__main__":
    main()
