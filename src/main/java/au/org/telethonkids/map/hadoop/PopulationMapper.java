package uk.ac.ox.map.hadoop;

import com.google.common.collect.Lists;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

public class PopulationMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text outputKey= new Text();
    private DoubleWritable outputValue = new DoubleWritable();
    private final static WKBReader wkbReader = new WKBReader();
    private STRtree isochrones = new STRtree();
    private final static GeometryFactory geometryFactory = new GeometryFactory();
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String targetTravelTimeBand = conf.get("travel.time.band");
        try {
            Reader in = new FileReader("/tmp/isochrones.csv");
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

            for (CSVRecord record : records) {
                String travelTimeBand = record.get(record.size() - 2);
                if(travelTimeBand.equals(targetTravelTimeBand)){
                    String geom = record.get(record.size() - 1);
                    byte[] bytes = WKBReader.hexToBytes(geom);
                    Geometry geometry = wkbReader.read(bytes);
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(record.get(2)).append(",").append(record.get(record.size() - 5)).append(",").append(record.get(record.size() - 4));
                    geometry.setUserData(stringBuilder.toString());
                    isochrones.insert(geometry.getEnvelopeInternal(), geometry);
                }
            }
            isochrones.build();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ",");
        Double lat = Double.parseDouble(strings[0]);
        Double lon = Double.parseDouble(strings[1]);
        Double pop = Double.parseDouble(strings[2]);

        Coordinate coordinate = new Coordinate(lon,lat);
        Point point = geometryFactory.createPoint(coordinate);
        Envelope search = new Envelope(coordinate);
        search.expandBy(0.001);
        List<Geometry> candidates = isochrones.query(search);
        List<String> facilities = Lists.newArrayList();
        for (Geometry candidate: candidates) {
            if(candidate.intersects(point)){
                facilities.add((String) candidate.getUserData());
            }
        }

        for (String facility: facilities
             ) {
            outputKey.set(facility + "," + facilities.size());
            outputValue.set(pop);
            context.write(outputKey, outputValue);
        }

    }
}
