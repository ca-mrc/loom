#!/bin/bash
# Prove that the negative certificate control has a real usable key and PIN.
set -euo pipefail
umask 077
openssl rand 32 > /root/auth-fixture/challenge
pkcs11-tool --module /usr/lib/softhsm/libsofthsm2.so --token-label untrusted \
    --login --pin "$(cat /root/auth-fixture/pin)" --sign --id 01 --mechanism SHA256-RSA-PKCS \
    --input-file /root/auth-fixture/challenge --output-file /root/auth-fixture/signature >/dev/null 2>&1
openssl x509 -inform DER -in /root/auth-fixture/untrusted.der -pubkey -noout > /root/auth-fixture/public-key
openssl dgst -sha256 -verify /root/auth-fixture/public-key -signature /root/auth-fixture/signature \
    /root/auth-fixture/challenge
