# mod3-spark

This folder contains Jupyter notebooks designed to explore the usage of **PySpark**, a powerful tool for distributed data processing.

## Getting Started

To run these notebooks, ensure you have the necessary dependencies installed and configured on your system.

### Prerequisites

1. **Python Environment**:
   - Install Python 3.7 or later.
   - Set up a virtual environment (recommended).
     ```bash
     python -m venv .venv
     source .venv/bin/activate  # macOS/Linux
     .venv\Scripts\activate     # Windows
     ```

2. **Install PySpark**:
   - Install PySpark using pip:
     ```bash
     pip install pyspark
     ```

3. **Install Jupyter Notebook**:
   - Install Jupyter:
     ```bash
     pip install notebook
     ```

4. **Install Java**:
   - PySpark requires Java. Ensure Java is installed and JAVA_HOME is correctly set.
   - On macOS, you can install Java using Homebrew:
     ```bash
     brew install openjdk@17
     ```
     Then, configure your environment:
     ```bash
     export JAVA_HOME=$(/usr/libexec/java_home)
     export PATH=$JAVA_HOME/bin:$PATH
     ```

5. **Verify Setup**:
   - Check that PySpark works by starting a Python shell and creating a Spark session:
     ```python
     from pyspark.sql import SparkSession
     spark = SparkSession.builder.appName("Test").getOrCreate()
     print(spark.version)
     ```

### Running the Notebooks

1. Start the Jupyter Notebook server:
   ```bash
   jupyter notebook
    ```