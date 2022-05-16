ARG CASSANDRA_VERSION

FROM cassandra:${CASSANDRA_VERSION}

ARG CASSANDRA_VERSION

COPY ./cassandra_encrypted/cassandra${CASSANDRA_VERSION}.yaml /etc/cassandra/cassandra.yaml
COPY ./cassandra_encrypted/.keystore /conf/.keystore
COPY ./cassandra_encrypted/.truststore /conf/.truststore
