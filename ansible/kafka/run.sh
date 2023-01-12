#!/bin/bash

MODE=$1
DEPLOY="present"
DELETE="absent"

if [[ "$MODE" = "$DEPLOY" ]]; then
    printf "\n##### Deploy Kafka Operator...\n"
    sh ${WORKDIR}/ansible/kafka/deploy.sh
elif [ "$MODE" = "$DELETE" ]; then
    printf "\n##### Delete Kafka Operator...\n"
    sh ${WORKDIR}/ansible/kafka/delete.sh
fi
