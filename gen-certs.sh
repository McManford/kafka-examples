#!/bin/bash

base_dir=$(dirname $0)

if [ $# -ne 1 ]; then
    echo "Usage gen-certs.sh output-dir"
    exit 1
fi

out_dir=$1
if [ ! -d ${out_dir} ]; then
    echo "output-dir ${out_dir} does not exist"
    exit 1
fi

echo "##### Creating keystores and trustores in ${out_dir}"

# FIPS extra args
# extra_args="-storetype PKCS12 -keyalg RSA -providerClass com.rsa.jsafe.provider.JsafeJCE -keysize 2048 -sigalg SHA256withRSA"
password="test1234"
validity="365"
server_keystore="${out_dir}/kafka.server.keystore.jks"
server_truststore="${out_dir}/kafka.server.truststore.jks"
client_keystore="${out_dir}/kafka.client.keystore.jks"
client_truststore="${out_dir}/kafka.client.truststore.jks"
ou=""
o="Intel"
l="Santa Clara"
st="CA"
c="US"
server_cn="kafka-1"
client_cn="kafka-1-cli"
keytool_alias_server="kafka-1"
keytool_alias_client="kafka-1"

# Generate CA key and certificate
openssl req -new -x509 -keyout ${out_dir}/ca-key -out ${out_dir}/ca-cert -days $validity -passout pass:$password -subj "/CN=$server_cn/O=$o/L=$l/ST=$st/C=$c"

# Import CA cert into server and client trust stores.
# Servers and clients will trust any certificate signed by this CA 
keytool -keystore ${server_truststore} -importcert -alias CARoot -file ${out_dir}/ca-cert -noprompt -storepass $password ${extra_args}
keytool -keystore ${client_truststore} -importcert -alias CARoot -file ${out_dir}/ca-cert -noprompt -storepass $password ${extra_args}


### Build server key store ###
# Generates server key pair.
# Wraps the public key into an X.509 v3 self-signed certificate, which is stored as a single-element certificate chain.
# dname specifies the X.500 Distinguished Name to be associated with alias, and is used as the issuer and subject fields in the self-signed certificate.
keytool -keystore ${server_keystore} -genkeypair -alias $keytool_alias_server -dname "CN=$server_cn, OU=$ou, O=$o, L=$l, ST=$st, C=$c" -noprompt -keypass ${password} -storepass ${password} ${extra_args}

# Generates server Certificate Signing Request (CSR), using the PKCS#10 format.
# The private key and X.500 Distinguished Name associated with alias are used to create the PKCS#10 certificate request.
keytool -keystore ${server_keystore} -certreq -alias $keytool_alias_server -file ${out_dir}/server-cert.csr -noprompt -storepass $password ${extra_args}

# Sign server certificate using CA certificate/key
openssl x509 -req -CA ${out_dir}/ca-cert -CAkey ${out_dir}/ca-key -in ${out_dir}/server-cert.csr -out ${out_dir}/server-cert-signed -days $validity -CAcreateserial -passin pass:$password

# Import CA certificate and server certificate signed by CA into server key store
keytool -keystore ${server_keystore} -importcert -alias CARoot -file ${out_dir}/ca-cert -noprompt -storepass $password ${extra_args}
keytool -keystore ${server_keystore} -importcert -alias $keytool_alias_server -file ${out_dir}/server-cert-signed -noprompt -storepass $password ${extra_args}


### Build client key store ###
# Generates client key pair.
# Wraps the public key into an X.509 v3 self-signed certificate, which is stored as a single-element certificate chain.
# dname specifies the X.500 Distinguished Name to be associated with alias, and is used as the issuer and subject fields in the self-signed certificate.
keytool -keystore ${client_keystore} -genkeypair -alias $keytool_alias_client -dname "CN=$client_cn, OU=$ou, O=$o, L=$l, ST=$st, C=$c" -validity $validity -noprompt -storepass $password -keypass $password ${extra_args}

# Generates client Certificate Signing Request (CSR), using the PKCS#10 format.
# The private key and X.500 Distinguished Name associated with alias are used to create the PKCS#10 certificate request.
keytool -keystore ${client_keystore} -certreq -alias $keytool_alias_client -file ${out_dir}/client-cert.csr -noprompt -storepass $password ${extra_args}

# Sign client certificate using CA certificate/key
openssl x509 -req -CA ${out_dir}/ca-cert -CAkey ${out_dir}/ca-key -in ${out_dir}/client-cert.csr -out ${out_dir}/client-cert-signed -days $validity -CAcreateserial -passin pass:$password

# Import CA certificate and client certificate signed by CA into client key store
keytool -keystore ${client_keystore} -importcert -alias CARoot -file ${out_dir}/ca-cert -noprompt -storepass $password ${extra_args}
keytool -keystore ${client_keystore} -importcert -alias $keytool_alias_client -file ${out_dir}/client-cert-signed -noprompt -storepass $password ${extra_args}
