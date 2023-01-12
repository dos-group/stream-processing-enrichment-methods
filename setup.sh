#!/bin/bash

export GCP_PROJECT_ID=<gcp-project-id>
export SERVICE_ACCOUNT_EMAIL=<service-account-email>
export GKE_NETWORK=<gke-network>
export GKE_SUBNET=<gke-subnet>

envsubst \
    < ansible/playbook-cluster.template.yml \
    > ansible/playbook-cluster.yml

envsubst \
    < gcloud-docker/startup.template.sh \
    > gcloud-docker/startup.sh
