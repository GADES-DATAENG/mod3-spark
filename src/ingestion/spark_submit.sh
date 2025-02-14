# Get the Spark Master IP address from Docker Container
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark-master

# The Spark Master URL should be like
# spark://<container-ip>:7077

# The spark-submit command will be like this
spark-submit \
  --master spark://172.17.0.2:7077 \
  --deploy-mode client \
  --jars gs://spark-lib/bigquery/spark-3.2-bigquery-0.27.1.jar \
  my_script.py gs://my-bucket/my-file.csv