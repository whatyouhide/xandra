ARG CASSANDRA_VERSION

FROM cassandra:$CASSANDRA_VERSION

ARG AUTHENTICATION=false

# Enable user-defined functions.
RUN sed -i -e "s/\(enable_user_defined_functions: \)false/\1true/" /etc/cassandra/cassandra.yaml
RUN sed -i -e "s/\(user_defined_functions_enabled: \)false/\1true/" /etc/cassandra/cassandra.yaml

RUN if [ "$AUTHENTICATION" = true ]; then \
  sed -i -e "s/\(authenticator: \)AllowAllAuthenticator/\1PasswordAuthenticator/" /etc/cassandra/cassandra.yaml; \
  fi
