FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Spark Kafka + S3A JARs
RUN mkdir -p /app/jars && \
    curl -sL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar -o /app/jars/spark-sql-kafka.jar && \
    curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /app/jars/hadoop-aws.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /app/jars/aws-sdk.jar && \
    curl -sL https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar -o /app/jars/clickhouse-jdbc.jar

ENV PYSPARK_SUBMIT_ARGS="--jars /app/jars/spark-sql-kafka.jar,/app/jars/hadoop-aws.jar,/app/jars/aws-sdk.jar,/app/jars/clickhouse-jdbc.jar pyspark-shell"

COPY src/ .

CMD ["python", "spark_job.py"]
