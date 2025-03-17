import argparse
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main():
    # Argument parser to get file location and BigQuery details
    parser = argparse.ArgumentParser(description="Read a file and write to BigQuery")
    parser.add_argument("--input_path", required=True, help="Path to input file (GCS or local)")
    parser.add_argument("--bq_project", required=True, help="GCP Project ID")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery Dataset Name")
    parser.add_argument("--bq_table", required=True, help="BigQuery Table Name")
    parser.add_argument("--gcp_keyfile", required=True, help="Path to GCP Service Account JSON key")

    args = parser.parse_args()

    input_path = args.input_path
    bq_project = args.bq_project
    bq_dataset = args.bq_dataset
    bq_table = args.bq_table
    gcp_keyfile = args.gcp_keyfile

    logger.info(f'Start extracting data for table {bq_table}')

    # Initialize Spark session with BigQuery support
    spark = SparkSession.builder \
        .appName("CSV to BigQuery") \
        .master("local[*]") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_keyfile) \
        .getOrCreate()
    logger.info('Spark Session built with sucess')
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    logger.info(f"All files from {input_path} loaded into DF.")

    logger.info("Start saving data in BQ...")
    # Write to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"{bq_project}.{bq_dataset}.{bq_table}") \
        .option("writeMethod", "direct") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("datasetLocation", "europe-west4") \
        .mode("append") \
        .save()

    logger.info(f"Data successfully written to {bq_project}.{bq_dataset}.{bq_table}")

    spark.stop()

if __name__ == "__main__":
    main()
