# This script can be run to regenerate the certificates for the IDS-Testbed in a specified folder

if [ "$#" != "2" ] ; then
	echo "Usage: ./setup_CA.sh <pki_dir> <new component>"
	exit 1
fi

PKIINPUT="$(dirname "$0")/pkiInput"
PKIDIR="$1"
COMPONENT_NEW="$2"

printf "PKIINPUT is %s\n" "$PKIINPUT"
printf "PKIDIR is %s\n" "$PKIDIR"

if [ -d "$PKIDIR" ]; then
	echo "$PKIDIR already exists. OK."
else
	echo "$PKIDIR already does not exist. Create it first!"
	exit 1
fi

CADIR="$PKIDIR/ca"
SUBCADIR="$PKIDIR/subca"
OCSPDIR="$PKIDIR/ocsp"
COMPDIR="$PKIDIR/certs"
shift

# delete old files
rm $COMPDIR/$COMPONENT_NEW*

# IMPORTANT need to edit ./data-cfssl/ocsp/sqlite_db_components.json
# and CertificateAuthority/data-cfssl/ocsp/sqlite_db_subcas.json change to your local path

# 1. Generate and sign certificates for the new component in the testbed
printf "1. Generate and sign certificates for new component $COMPONENT_NEW"
echo " => cfssl genkey \"$PKIINPUT/$COMPONENT_NEW.json\" | cfssljson -bare \"$COMPDIR/$COMPONENT_NEW\"\n"
cfssl genkey "$PKIINPUT/$COMPONENT_NEW.json" | cfssljson -bare "$COMPDIR/$COMPONENT_NEW"
echo " => cfssl sign -ca \"$SUBCADIR/subca.pem\" -ca-key \"$SUBCADIR/subca-key.pem\"  -db-config \"$PKIDIR/ocsp/sqlite_db_components.json\" --config \"$PKIINPUT/ca-config.json\"  -profile \"component\" \"$COMPDIR/$COMPONENT_NEW.csr\" | cfssljson -bare \"$COMPDIR/$COMPONENT_NEW\"\n"
cfssl sign -ca "$SUBCADIR/subca.pem" -ca-key "$SUBCADIR/subca-key.pem"  -db-config "$PKIDIR/ocsp/sqlite_db_components.json" --config "$PKIINPUT/ca-config.json"  -profile "component" "$COMPDIR/$COMPONENT_NEW.csr" | cfssljson -bare "$COMPDIR/$COMPONENT_NEW"

# 2. Prepare the OCSP provider for components
printf "2. Prepare the OCSP provider for components\n"
cfssl ocsprefresh -db-config "$OCSPDIR/sqlite_db_components.json" -ca "$SUBCADIR/subca.pem" -responder "$OCSPDIR/ocsp_components.pem" -responder-key "$OCSPDIR/ocsp_components-key.pem"
cfssl ocspdump -db-config "$OCSPDIR/sqlite_db_components.json" >"$OCSPDIR/ocspdump_components.txt"

# 3. Prepare the OCSP provider for subCA
printf "3. Prepare the OCSP provider for subCA\n"
cfssl ocsprefresh -db-config "$OCSPDIR/sqlite_db_subcas.json" -ca "$CADIR/ca.pem" -responder "$OCSPDIR/ocsp_subcas.pem" -responder-key "$OCSPDIR/ocsp_subcas-key.pem"
cfssl ocspdump -db-config "$OCSPDIR/sqlite_db_subcas.json" >"$OCSPDIR/subcas_components.txt"


printf "\n * You can run the OCSP provider with: cfssl ocspserve -port=8887 -responses=\"$OCSPDIR/ocspdump_components.txt\" -loglevel=0"
printf "check certificate status with: $ openssl ocsp -issuer data-cfssl/ocsp/ocsp_components.pem -issuer data-cfssl/subca/subca.pem -no_nonce -cert data-cfssl/certs/$COMPONENT_NEW.pem -CAfile data-cfssl/ca/ca.pem -text -url http://localhost:8887\n"

# Other commands
echo "\n 4. Other commands in $COMPDIR:\n"
openssl pkcs12 -export -out $COMPDIR/$COMPONENT_NEW.p12 -in $COMPDIR/$COMPONENT_NEW.pem -inkey $COMPDIR/$COMPONENT_NEW-key.pem -passout pass:password
openssl pkcs12 -in $COMPDIR/$COMPONENT_NEW.p12 -clcerts -nokeys -out $COMPDIR/$COMPONENT_NEW.crt -passin pass:password
openssl pkcs12 -in $COMPDIR/$COMPONENT_NEW.p12 -out $COMPDIR/$COMPONENT_NEW.cert -nokeys -nodes -passin pass:password
cp $COMPDIR/$COMPONENT_NEW-key.pem $COMPDIR/$COMPONENT_NEW.key

chmod 664 $COMPDIR/$COMPONENT_NEW.cert
chmod 664 $COMPDIR/$COMPONENT_NEW.crt
chmod 664 $COMPDIR/$COMPONENT_NEW.key
chmod 664 $COMPDIR/$COMPONENT_NEW.p12

# Extra commands for CA certificate
echo "\n 5. Other commands in $CADIR:\n"
openssl pkcs12 -export -out $CADIR/ca.p12 -in $CADIR/ca.pem -inkey $CADIR/ca-key.pem -passout pass:password
openssl pkcs12 -in $CADIR/ca.p12 -clcerts -nokeys -out $CADIR/ca.crt -passin pass:password
openssl pkcs12 -in $CADIR/ca.p12 -out $CADIR/ca.cert -nokeys -nodes -passin pass:password
cp $CADIR/ca-key.pem $CADIR/ca.key
chmod 664 $CADIR/ca.cert
chmod 664 $CADIR/ca.crt
chmod 664 $CADIR/ca.key
chmod 664 $CADIR/ca.p12

### Extra commands for subCA certificate
echo "\n 6. Other commands in $SUBCADIR:\n"
openssl pkcs12 -export -out $SUBCADIR/subca.p12 -in $SUBCADIR/subca.pem -inkey $SUBCADIR/subca-key.pem -passout pass:password
openssl pkcs12 -in $SUBCADIR/subca.p12 -clcerts -nokeys -out $SUBCADIR/subca.crt -passin pass:password
openssl pkcs12 -in $SUBCADIR/subca.p12 -out $SUBCADIR/subca.cert -nokeys -nodes -passin pass:password
cp $SUBCADIR/subca-key.pem $SUBCADIR/subca.key
chmod 664 $SUBCADIR/subca.cert
chmod 664 $SUBCADIR/subca.crt
chmod 664 $SUBCADIR/subca.key
chmod 664 $SUBCADIR/subca.p12

echo "\n... DONE!\n"
