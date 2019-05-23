ARG CASSANDRA_VERSION=latest

FROM cassandra:$CASSANDRA_VERSION

ARG AUTHENTICATION=false

RUN sed -i -e "s/\(enable_user_defined_functions: \)false/\1true/" /etc/cassandra/cassandra.yaml

RUN if [ "$AUTHENTICATION" = true ]; then \
      sed -i -e "s/\(authenticator: \)AllowAllAuthenticator/\1PasswordAuthenticator/" /etc/cassandra/cassandra.yaml; \
    fi
