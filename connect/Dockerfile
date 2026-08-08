# Kafka Connect with both halves of what this stack needs.
#
# The obvious base, quay.io/debezium/connect, does not work here: as of 3.x it
# ships the Apicurio converters only, so a worker configured with
# io.confluent.connect.avro.AvroConverter dies at startup with a
# ClassNotFoundException and never binds its REST port.
#
# So start from the Confluent image, where that converter is built in, and add
# the Debezium PostgreSQL connector. The plugin tarball is self-contained — it
# carries debezium-core, which is where the outbox EventRouter SMT lives.
FROM confluentinc/cp-kafka-connect:7.9.0

# Matches the version Debezium's own connect image installs.
ARG DEBEZIUM_VERSION=3.3.2.Final

USER root
RUN curl -fsSL \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
      | tar -xz -C /usr/share/confluent-hub-components \
 && chown -R appuser:appuser /usr/share/confluent-hub-components/debezium-connector-postgres
USER appuser
