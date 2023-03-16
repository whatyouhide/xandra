#!/bin/bash -e

PASSWORD=cassandra
CLUSTER_NAME=test_cluster
ORGANISATION="O=Imixs, L=MUC, ST=BAY, C=DE"

KEY_STORE_PATH="./certs"
KEY_STORE="$KEY_STORE_PATH/cassandra.keystore"
PKS_KEY_STORE="$KEY_STORE_PATH/cassandra.keystore.pks12"
TRUST_STORE="$KEY_STORE_PATH/cassandra.truststore"
CLIENT_PUBLIC_CERT="$KEY_STORE_PATH/${CLUSTER_NAME}_client_public.pem"

mkdir -pv "$KEY_STORE_PATH"

echo "**************************************************"
echo " Generating keystore and certificates for cluster $CLUSTER_NAME"
echo "**************************************************"


### Cluster key setup.

echo "==> Creating client key"
keytool -genkey \
  -keyalg RSA \
  -alias "${CLUSTER_NAME}_client" \
  -keystore "$KEY_STORE" \
  -storepass "$PASSWORD" \
  -keypass "$PASSWORD" \
  -dname "CN=$CLUSTER_NAME client, $ORGANISATION" \
  -validity 36500

echo "==> Creating the public key for the client to identify itself"
keytool -export \
  -alias "${CLUSTER_NAME}_client" \
  -file "$CLIENT_PUBLIC_CERT" \
  -keystore "$KEY_STORE" \
  -storepass "$PASSWORD" \
  -keypass "$PASSWORD" \
  -noprompt

echo "==> Importing the identity of the client pub  key into the trust store so nodes can identify this client"
keytool -importcert \
  -v \
  -trustcacerts \
  -alias "${CLUSTER_NAME}_client" \
  -file "$CLIENT_PUBLIC_CERT" \
  -keystore "$TRUST_STORE" \
  -storepass "$PASSWORD" \
  -keypass "$PASSWORD" \
  -noprompt


echo "==> Creating a pks12 keystore file"
keytool -importkeystore \
  -srckeystore "$KEY_STORE" \
  -destkeystore "$PKS_KEY_STORE" \
  -deststoretype PKCS12 \
  -srcstorepass "$PASSWORD" \
  -deststorepass "$PASSWORD"

echo "==> Creating OpenSSL cer.pem and key.pem files"
openssl pkcs12 \
  -in "$PKS_KEY_STORE" \
  -nokeys \
  -out "${KEY_STORE_PATH}/${CLUSTER_NAME}.cer.pem" \
  -passin pass:${PASSWORD}
openssl pkcs12 \
  -in "$PKS_KEY_STORE" \
  -nodes \
  -nocerts \
  -out "${KEY_STORE_PATH}/${CLUSTER_NAME}.key.pem" \
  -passin pass:${PASSWORD}

echo "**************************************************"
echo "✅ Done"
echo "**************************************************"
