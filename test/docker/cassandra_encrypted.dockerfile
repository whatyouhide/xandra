FROM cassandra:latest

COPY ./cassandra_encrypted/cassandra.yaml /etc/cassandra/cassandra.yaml
COPY ./cassandra_encrypted/.keystore /conf/.keystore
