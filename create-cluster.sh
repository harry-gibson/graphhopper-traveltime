#!/bin/bash
gcloud --project=map-visualization-dev beta dataproc clusters create \
   --num-workers=30 \
   --scopes=cloud-platform \
   --worker-machine-type=n1-standard-1 \
   --worker-boot-disk-type=pd-ssd \
   --master-machine-type=n1-standard-1 \
   --region=europe-west1 \
   --zone=europe-west1-b \
   --max-idle=600 \
   --initialization-actions="gs://map-graphhopper/install-on-cluster.sh" \
   isochrone-cluster