#!/bin/bash

# Set the gcloud project name
gcloud config set project ${GCP_PROJECT_ID}

# Activate the gcloud service account
gcloud auth activate-service-account ${SERVICE_ACCOUNT_EMAIL} --key-file=/workdir/my-sa.json
