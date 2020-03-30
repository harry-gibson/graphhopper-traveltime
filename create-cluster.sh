#!/bin/bash
gcloud dataproc clusters create \
   --project=map-visualization-dev \
   --num-workers=2 \
   --num-secondary-workers=38 \
   --scopes=cloud-platform \
   --worker-machine-type=n1-highmem-2 \
   --worker-boot-disk-type=pd-ssd \
   --master-machine-type=n1-standard-2 \
   --region=europe-west1 \
   --zone=europe-west1-b \
   --max-idle=600 \
   --initialization-actions="gs://map-graphhopper/install-on-cluster.sh" \
   isochrone-cluster-1