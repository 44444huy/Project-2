FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

COPY src /opt/project/src
COPY spark/conf/spark-defaults.conf.template /opt/project/spark-defaults.conf.template
COPY spark/scripts/spark-entrypoint.sh /opt/project/spark-entrypoint.sh

RUN apt-get update \
    && apt-get install -y --no-install-recommends gettext-base \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /tmp/.ivy2 /tmp/spark-conf \
    && chmod +x /opt/project/spark-entrypoint.sh \
    && chown -R 185:185 /opt/project /tmp/.ivy2 /tmp/spark-conf

ENV PYTHONPATH=/opt/project/src \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

USER 185:185
WORKDIR /opt/project
ENTRYPOINT ["/opt/project/spark-entrypoint.sh"]
