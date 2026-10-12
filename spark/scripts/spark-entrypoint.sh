#!/bin/sh
set -eu
: "${POLARIS_CLIENT_ID:?POLARIS_CLIENT_ID must be set}"
: "${POLARIS_CLIENT_SECRET:?POLARIS_CLIENT_SECRET must be set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER must be set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD must be set}"
mkdir -p /tmp/spark-conf
envsubst '${POLARIS_CLIENT_ID} ${POLARIS_CLIENT_SECRET} ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}' \
  < /opt/project/spark-defaults.conf.template \
  > /tmp/spark-conf/spark-defaults.conf
export SPARK_CONF_DIR=/tmp/spark-conf
exec "$@"
