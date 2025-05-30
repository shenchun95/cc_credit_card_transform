# Base on an official Apache Airflow image
FROM apache/airflow:2.8.1-python3.9 # You can choose a specific Airflow/Python version

USER root
# Install sudo and git (git might be needed if your DAGs pull from a repo, not strictly for this case)
RUN apt-get update && \
    apt-get install -y sudo git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install OpenJDK for Spark (PySpark needs Java)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install PySpark and other Python dependencies
# You might want to create a requirements_airflow.txt if it gets complex
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyspark==3.5.0 # Use a PySpark version compatible with your Spark setup

# Copy DAGs, PySpark scripts, data, and key into the image
# These paths assume the Docker build context is the project root.
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY pyspark_scripts/ ${AIRFLOW_HOME}/pyspark_scripts/
COPY raw_cc_credit_card/ ${AIRFLOW_HOME}/raw_cc_credit_card/
COPY data/raw_additional_data/ ${AIRFLOW_HOME}/data/raw_additional_data/
COPY decryption.key ${AIRFLOW_HOME}/decryption.key
COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt # If you have one and it's relevant for Spark/Airflow worker

# Set the working directory for the Spark job if necessary,
# though spark-submit usually handles paths relative to where it's called
# or the application path.
# The PySpark script uses relative paths like '../raw_cc_credit_card',
# so AIRFLOW_HOME (usually /opt/airflow) becomes the base for these.

# Expose Airflow webserver and flower ports (optional, for reference)
# EXPOSE 8080
# EXPOSE 5555

# The entrypoint and CMD will be inherited from the base Airflow image,
# which will start Airflow services (webserver, scheduler, etc.).
