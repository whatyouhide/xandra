ARG CASSANDRA_VERSION

FROM cassandra:$CASSANDRA_VERSION

# Enable user-defined functions.
RUN sed -i -e "s/\(enable_user_defined_functions: \)false/\1true/" /etc/cassandra/cassandra.yaml

# Disable auto bootstrapping.
RUN echo "auto_bootstrap: false" >> /etc/cassandra/cassandra.yaml
