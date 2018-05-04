#!/usr/bin/env bash

echo "Generating SSH key pair..."

PRIVATE_KEY_FILE="/tmp/ssh.key"
PUBLIC_KEY_FILE="/tmp/ssh.key.pub"
rm -f ${PRIVATE_KEY_FILE}
rm -f ${PUBLIC_KEY_FILE}

ssh-keygen -b 4096 -t rsa -f ${PRIVATE_KEY_FILE} -q -N '' && echo "Done"

export SSH_KEY_PRIVATE=$(cat ${PRIVATE_KEY_FILE}) && echo "Private key exported to SSH_KEY_PRIVATE"

export SSH_KEY_PUBLIC=$(cat ${PUBLIC_KEY_FILE}) && echo "Private key exported to SSH_KEY_PUBLIC"
