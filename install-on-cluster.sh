#!/bin/bash
apt-get update -y
mkdir -p /srv/data
gsutil -m cp -r gs://map-graphhopper/africa/car/network /srv/data