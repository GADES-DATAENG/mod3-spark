# mod3-spark

This folder contains Jupyter notebooks designed to explore the usage of **PySpark**, a powerful tool for distributed data processing.

## Getting Started

To run these notebooks, ensure you have the necessary dependencies installed and configured on your system.

### Prerequisites

1. **Install Docker**

2. **Download required data**:
   Run the next command to get the data needed for this exercise. If you don't have `wget` installed, follow the instructions below.
   ```bash
   wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2023-01.parquet -P spark_data/
   ```

   ### **Install `wget` if needed**
   **macOS**<br>
   You can install `wget` using Homebrew:
   ```bash
   brew install wget
   ```

   **Linux**<br>
   On Debian-based distributions (like Ubuntu), install `wget` using:
   ```bash
   sudo apt-get update
   sudo apt-get install wget
   ```
   **Red Hat-based systems (like Fedora)**<br>
   ```bash
   sudo dnf install wget
   ```

   **Windows**<br>
   On Windows, install `wget` as part of Git Bash or through a third-party package manager like Chocolatey:
   ```bash
   choco install wget
   ```

### Running the Notebooks

1. Build the Docker container:
   ```bash
   docker-compose build --no-cache
    ```
2. Run the Docker container:
   ```bash
   docker-compose up -d
    ```
3. Open the following links to ensure everything is working properly<br>
[Jupyter Notebook](http://localhost:8888)<br>
[Spark Cluster UI](http://localhost:8080)