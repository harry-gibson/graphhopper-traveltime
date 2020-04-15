package au.org.telethonkids.map.hadoop;

import au.org.telethonkids.map.IsochroneGenerator;
import com.graphhopper.GraphHopper;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;

import java.io.IOException;
import java.util.List;

public class OriginMapper extends Mapper<Object, Text, NullWritable, Text> {

    private final GraphHopper hopper = new GraphHopperOSM().setOSMFile("/tmp/network.osm.pbf").
            setStoreOnFlush(true).
            setCHEnabled(true).
            setGraphHopperLocation("/tmp/network").
            setEncodingManager(EncodingManager.create("car")).
            importOrLoad();
    protected FlagEncoder encoder;


    private static final int timeLimit = 8100;
    private static final int numberOfBuckets = 9;

    private final static GeometryFactory geometryFactory = new GeometryFactory();
    private final static WKBWriter wkbWriter = new WKBWriter();

    private NullWritable outputKey = NullWritable.get();
    private Text outputValue = new Text();


    @Override
    protected void setup(Context context) {
        EncodingManager encodingManager = hopper.getEncodingManager();
        encoder = encodingManager.getEncoder("car");
    }

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ",");
        String latString = strings[strings.length - 3];
        String lonString = strings[strings.length - 2];
        if(!latString.isEmpty() && !lonString.isEmpty()){
            Double lat = Double.parseDouble(latString);
            Double lon = Double.parseDouble(lonString);
            List<List<Coordinate>> isochrone = IsochroneGenerator.buildIsochrone(timeLimit, numberOfBuckets, hopper, encoder, lat, lon);
            if(isochrone != null){
                List<Coordinate[]> polygonShells = IsochroneGenerator.buildIsochronePolygons(lat, lon, isochrone);
                if(polygonShells != null) {
                    Polygon previousPolygon = geometryFactory.createPolygon(polygonShells.get(0));
                    int interval = timeLimit / numberOfBuckets;
                    outputValue.set(value + "," + interval + "," + WKBWriter.toHex(wkbWriter.write(previousPolygon)));
                    context.write(outputKey, outputValue);
                    for (int j = 1; j < polygonShells.size() - 1; j++) {
                        Polygon polygon = geometryFactory.createPolygon(polygonShells.get(j));
                        Geometry difference = polygon.difference(previousPolygon);
                        outputValue.set(value + "," + ((j+1) * interval) + "," +  WKBWriter.toHex(wkbWriter.write(difference)));
                        context.write(outputKey, outputValue);
                        previousPolygon = polygon;

                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        hopper.close();
    }
}
