# Use Apache Airflow base image with Python 3.9
FROM apache/airflow:2.7.3-python3.9

USER root

# Install dependencies (including Java for Spark)
RUN apt-get update && apt-get install -y \
    curl \
    python3-pip \
    gnupg \
    openjdk-11-jdk  # Java is required for Spark

# Install Google Cloud SDK
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt-get update && apt-get install -y google-cloud-sdk

# Install Apache Spark
ENV SPARK_VERSION=3.4.1
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Switch back to airflow user
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
