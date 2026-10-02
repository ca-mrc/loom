#!/bin/bash
# Generic authentication inputs; deliberately does not run p11-kit or sudo.
set -euo pipefail
umask 077
test ! -e /root/auth-fixture
mkdir -p /root/auth-fixture/tokens /root/.ssh /home/authuser/.ssh
mkdir -p /run/sshd /run/user/0 /run/user/1100/p11-kit /etc/pam_pkcs11/cacerts
chmod 755 /run/user /run/sshd
chmod 700 /run/user/0 /run/user/1100 /run/user/1100/p11-kit
chown -R authuser:authuser /run/user/1100 /home/authuser/.ssh
printf 'directories.tokendir = /root/auth-fixture/tokens\nobjectstore.backend = file\n' > /etc/softhsm/softhsm2.conf
openssl rand -hex 12 > /root/auth-fixture/pin
pin=$(cat /root/auth-fixture/pin)
for label in trusted untrusted; do
    softhsm2-util --init-token --free --label "$label" --pin "$pin" \
        --so-pin "$(openssl rand -hex 12)" >/dev/null
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=$label-fixture-ca" \
        -keyout "/root/auth-fixture/$label-ca.key" -out "/root/auth-fixture/$label-ca.pem" >/dev/null 2>&1
    openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
        -out "/root/auth-fixture/$label.key" >/dev/null 2>&1
    openssl req -new -key "/root/auth-fixture/$label.key" -subj /CN=authuser \
        -out "/root/auth-fixture/$label.csr" >/dev/null 2>&1
    openssl x509 -req -in "/root/auth-fixture/$label.csr" -days 2 -set_serial 1 \
        -CA "/root/auth-fixture/$label-ca.pem" -CAkey "/root/auth-fixture/$label-ca.key" \
        -outform DER -out "/root/auth-fixture/$label.der" >/dev/null 2>&1
    softhsm2-util --import "/root/auth-fixture/$label.key" --token "$label" \
        --id 01 --label authentication --pin "$pin" >/dev/null
    pkcs11-tool --module /usr/lib/softhsm/libsofthsm2.so --token-label "$label" \
        --login --pin "$pin" --write-object "/root/auth-fixture/$label.der" \
        --type cert --id 01 >/dev/null 2>&1
    rm "/root/auth-fixture/$label.key" "/root/auth-fixture/$label-ca.key" "/root/auth-fixture/$label.csr"
done
cp /root/auth-fixture/trusted-ca.pem /etc/pam_pkcs11/cacerts/fixture.pem
chmod 755 /etc/pam_pkcs11/cacerts
chmod 644 /etc/pam_pkcs11/cacerts/fixture.pem
openssl rehash /etc/pam_pkcs11/cacerts
ssh-keygen -q -t ed25519 -N '' -f /run/auth-host-key
ssh-keygen -q -t ed25519 -N '' -f /root/.ssh/id_ed25519
cp /root/.ssh/id_ed25519.pub /home/authuser/.ssh/authorized_keys
chown authuser:authuser /home/authuser/.ssh/authorized_keys
{ printf '[localhost]:2222 '; cut -d ' ' -f 1,2 /run/auth-host-key.pub; } > /root/.ssh/known_hosts
# Daemons must not retain a noninteractive runtime RPC's stdout/stderr pipes.
/usr/sbin/sshd -E /run/auth-sshd.log </dev/null >/run/auth-sshd-start.log 2>&1
