```bash
   gcloud dataproc jobs submit hadoop \
       --cluster isochrones-cluster --region europe-west1 \
       --class uk.ac.ox.map.hadoop.IsochroneDriver \
       --jars gs://map-graphhopper/graphhopper-builder-1.0-jar-with-dependencies.jar -- gs://map-graphhopper/health_facilities gs://map-graphhopper/isochrones
```