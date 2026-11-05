# Debezium Connect plus the Confluent Avro converter, so Debezium registers
# schemas in Schema Registry (needed to reproduce schema-compatibility failures).
# The Debezium version is a build arg (dev/docker-compose.yml passes it through
# from DEBEZIUM_VERSION, defaulting to the value here), so the stack's Connect
# image can be swapped without editing this file.
ARG DEBEZIUM_VERSION=3.2.0.Final
FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}

ARG AVRO_CONVERTER_VERSION=8.0.0
RUN cd /tmp \
 && curl -fsSL -o avro.zip \
      "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${AVRO_CONVERTER_VERSION}/confluentinc-kafka-connect-avro-converter-${AVRO_CONVERTER_VERSION}.zip" \
 && mkdir -p /tmp/avro && cd /tmp/avro \
 && (unzip -q /tmp/avro.zip || jar xf /tmp/avro.zip) \
 && mkdir -p /kafka/connect/confluent-avro-converter \
 && cp /tmp/avro/*/lib/*.jar /kafka/connect/confluent-avro-converter/ \
 && rm -rf /tmp/avro /tmp/avro.zip

# Prometheus JMX exporter (Q3d): a Java agent the connect service loads through
# KAFKA_OPTS (dev/docker-compose.yml). It serves :9404/metrics with the rules in
# jmx-exporter.yml: debezium_metrics_* and kafka_connect_*. Pinned, checksum-verified.
ARG JMX_EXPORTER_VERSION=1.0.1
ARG JMX_EXPORTER_SHA256=7d61f737fd661610ccc14aea79764faa1ea94a340cbc8f0029b3d2edea3d80c1
RUN mkdir -p /kafka/etc \
 && curl -fsSL -o /kafka/etc/jmx_prometheus_javaagent.jar \
      "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar" \
 && echo "${JMX_EXPORTER_SHA256}  /kafka/etc/jmx_prometheus_javaagent.jar" | sha256sum -c -
COPY jmx-exporter.yml /kafka/etc/jmx-exporter.yml
