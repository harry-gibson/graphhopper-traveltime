#!/bin/bash
apt-get update -y
mkdir /tmp
gsutil -m cp -r gs://map-graphhopper/africa/car/network /tmp