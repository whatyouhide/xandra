ARG SCYLLA_VERSION

FROM scylladb/scylla:$SCYLLA_VERSION

ARG AUTHENTICATION=false

RUN if [ "$AUTHENTICATION" = true ]; then \
  sed -i -e "s/\(authenticator: \)AllowAllAuthenticator/\1PasswordAuthenticator/" /etc/scylla/scylla.yaml; \
  fi

RUN echo "experimental_features:\n    - udf" >> /etc/scylla/scylla.yaml && \
  echo "enable_user_defined_functions: true" >> /etc/scylla/scylla.yaml
