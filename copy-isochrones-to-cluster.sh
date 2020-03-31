#!/bin/bash
apt-get update -y
mkdir -p /tmp
gsutil -m cp -r gs://map-graphhopper/isochrones/zwe/car.csv /tmp/isochrones.csv
chmod -R 0777 /tmp