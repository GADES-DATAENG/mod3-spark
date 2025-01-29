FROM python:3.11-alpine

USER root

# Install system dependencies
RUN apk add --no-cache \
    openjdk11 \
    wget \
    procps \
    bash \
    git \
    build-base \
    libffi-dev \
    openssl-dev

# Install Jupyter and Python packages
RUN pip install --no-cache-dir \
    jupyter \
    notebook \
    pyspark==3.4.1 \
    pandas \
    numpy \
    matplotlib \
    findspark

# Download and install Spark 3.4.1
RUN wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz && \
    tar -xvzf spark-3.4.1-bin-hadoop3.tgz && \
    mv spark-3.4.1-bin-hadoop3 /usr/local/spark && \
    rm spark-3.4.1-bin-hadoop3.tgz

# Set up environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV SPARK_HOME=/usr/local/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_LOCAL_IP=jupyter

# Create user and set up work directory
RUN adduser -D jupyter && \
    mkdir -p /home/jupyter/work && \
    chown -R jupyter:jupyter /home/jupyter

# Switch to jupyter user
USER jupyter
WORKDIR /home/jupyter/work

# Expose Jupyter port
EXPOSE 8888

# Start Jupyter notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]