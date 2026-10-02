FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive XDG_RUNTIME_DIR=/run/user/0
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-server openssh-client p11-kit softhsm2 opensc libpam-pkcs11 openssl sudo \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /etc/ssh/ssh_host_* \
    && useradd --create-home --uid 1100 --shell /bin/bash authuser \
    && usermod --password '*' authuser
COPY prepare.sh /usr/local/bin/prepare-auth-fixture
COPY check-signature.sh /usr/local/bin/check-untrusted-signature
COPY pam.conf /etc/pam_pkcs11/pam_pkcs11.conf
COPY sshd.conf /etc/ssh/sshd_config
RUN chmod 755 /usr/local/bin/prepare-auth-fixture /usr/local/bin/check-untrusted-signature \
    && printf '%s\n' 'Defaults:authuser timestamp_timeout=0,passwd_tries=1' \
      'authuser ALL=(root) PASSWD: /usr/bin/id -u' > /etc/sudoers.d/auth-fixture \
    && chmod 440 /etc/sudoers.d/auth-fixture \
    && printf '%s\n' 'auth requisite pam_pkcs11.so' 'account required pam_unix.so' \
      'session required pam_unix.so' > /etc/pam.d/sudo \
    && visudo -c
WORKDIR /root
