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
