ARG CASSANDRA_VERSION

FROM cassandra:${CASSANDRA_VERSION}

ARG AUTHENTICATION=false

RUN if [ "$AUTHENTICATION" = true ]; then \
    sed -i -e "s/\(authenticator: \)AllowAllAuthenticator/\1PasswordAuthenticator/" /etc/cassandra/cassandra.yaml; \
    fi

COPY ./cassandra.yaml /etc/cassandra/cassandra.yaml
COPY ./.keystore /conf/.keystore
