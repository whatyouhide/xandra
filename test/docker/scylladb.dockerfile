ARG SCYLLA_VERSION=latest

FROM scylladb/scylla:$SCYLLA_VERSION

ARG AUTHENTICATION=false

RUN if [ "$AUTHENTICATION" = true ]; then \
      sed -i -e "s/\(authenticator: \)AllowAllAuthenticator/\1PasswordAuthenticator/" /etc/scylla/scylla.yaml; \
    fi