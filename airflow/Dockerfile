FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu AS spark-runtime

FROM apache/airflow:2.10.5-python3.11

USER root
COPY --from=spark-runtime /opt/spark /opt/spark
COPY --from=spark-runtime /opt/java/openjdk /opt/java/openjdk
RUN apt-get update \
    && apt-get install -y --no-install-recommends gettext-base \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark \
    JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:${PATH} \
    PYTHONPATH=/opt/project/src:/opt/project

USER airflow
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
