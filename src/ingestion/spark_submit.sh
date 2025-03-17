# Get the Spark Master IP address from Docker Container
#docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark-master-1

# The Spark Master URL should be like
# spark://<container-ip>:7077

# The spark-submit command will be like this
spark-submit \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0 \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  file_to_bq.py \
  --input_path ../../spark_data/jaffle_data/ \
  --bq_project lively-hall-447909-i7 \
  --bq_dataset staging \
  --bq_table customers \
  --gcp_keyfile ../../notebooks/keys/gcp_keyfile.json