#!/bin/bash
apt-get update -y
mkdir -p /tmp
gsutil -m cp -r gs://map-graphhopper/africa/car/network /tmp
chmod -R 0777 /tmp/network