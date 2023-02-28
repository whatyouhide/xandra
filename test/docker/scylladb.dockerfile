ARG SCYLLA_VERSION=5.1.5

FROM scylladb/scylla:$SCYLLA_VERSION

ARG AUTHENTICATION=false

RUN if [ "$AUTHENTICATION" = true ]; then \
  sed -i -e "s/\(authenticator: \)AllowAllAuthenticator/\1PasswordAuthenticator/" /etc/scylla/scylla.yaml; \
  fi

RUN sed -i -e "s/enable_user_defined_functions: false/enable_user_defined_functions: true/" /etc/scylla/scylla.yaml
