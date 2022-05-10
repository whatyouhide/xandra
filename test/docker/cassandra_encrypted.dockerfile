ARG CASSANDRA_VERSION

FROM cassandra:${CASSANDRA_VERSION}

COPY ./cassandra_encrypted/cassandra.yaml /etc/cassandra/cassandra.yaml
COPY ./cassandra_encrypted/.keystore /conf/.keystore
