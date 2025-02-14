from pyspark.sql import SparkSession
import argparse

def main():
    # Argument parser to get file location and BigQuery details
    parser = argparse.ArgumentParser(description="Read a file and write to BigQuery")
    parser.add_argument("--input_path", required=True, help="Path to input file (GCS or local)")
    parser.add_argument("--bq_project", required=True, help="GCP Project ID")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery Dataset Name")
    parser.add_argument("--bq_table", required=True, help="BigQuery Table Name")
    parser.add_argument("--gcp_keyfile", required=True, help="Path to GCP Service Account JSON key")

    args = parser.parse_args()

    # Initialize Spark session with BigQuery support
    spark = SparkSession.builder \
        .appName("FileToBigQuery") \
        .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest.jar") \
        .config("temporaryGcsBucket", "your-temp-bucket") \
        .getOrCreate()

    # Set GCP credentials
    spark.conf.set("google.cloud.auth.service.account.json.keyfile", args.gcp_keyfile)

    # Read the file (Auto-detect format based on extension)
    if args.input_path.endswith(".csv"):
        df = spark.read.option("header", "true").csv(args.input_path)
    elif args.input_path.endswith(".json"):
        df = spark.read.json(args.input_path)
    elif args.input_path.endswith(".parquet"):
        df = spark.read.parquet(args.input_path)
    else:
        raise ValueError("Unsupported file format. Use CSV, JSON, or Parquet.")

    # Write to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"{args.bq_project}.{args.bq_dataset}.{args.bq_table}") \
        .option("temporaryGcsBucket", "your-temp-bucket") \
        .mode("append") \
        .save()

    print(f"Data successfully written to {args.bq_project}.{args.bq_dataset}.{args.bq_table}")

if __name__ == "__main__":
    main()
