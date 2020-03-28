package uk.ac.ox.map.hadoop;

import com.graphhopper.GraphHopper;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTWriter;
import uk.ac.ox.map.IsochroneGenerator;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

public class OriginMapper extends Mapper<Object, Text, NullWritable, Text> {

    protected GraphHopper hopper;
    protected FlagEncoder encoder;

    private static final int timeLimit = 8100;
    private static final int numberOfBuckets = 9;

    private final static GeometryFactory geometryFactory = new GeometryFactory();
    private final static WKTWriter wktWriter = new WKTWriter();

    private NullWritable outputKey = NullWritable.get();
    private Text outputValue = new Text();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String string = generateRandomString(1)[0];
        String source = "/tmp/network/";
        File srcDir = new File(source);
        String destination = "/tmp/" + string + "/";
        File destDir = new File(destination);
        try {
            FileUtils.copyDirectory(srcDir, destDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

        hopper = new GraphHopperOSM().setOSMFile("/tmp/network.osm.pbf").
                setStoreOnFlush(true).
                setCHEnabled(true).
                setGraphHopperLocation(destination).
                setEncodingManager(EncodingManager.create("car")).
                importOrLoad();

        EncodingManager encodingManager = hopper.getEncodingManager();
        encoder = encodingManager.getEncoder("car");
    }

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ",");
        String latString = strings[5];
        String lonString = strings[6];
        if(!latString.isEmpty() && !lonString.isEmpty()){
            Double lat = Double.parseDouble(latString);
            Double lon = Double.parseDouble(lonString);
            List<List<Coordinate>> isochrone = IsochroneGenerator.buildIsochrone(timeLimit, numberOfBuckets, hopper, encoder, lat, lon);
            if(isochrone != null){
                List<Coordinate[]> polygonShells = IsochroneGenerator.buildIsochronePolygons(lat, lon, isochrone);
                Polygon previousPolygon = geometryFactory.createPolygon(polygonShells.get(0));
                int interval = timeLimit / numberOfBuckets;
                outputValue.set(value + "," + interval + ",\"" + wktWriter.write(previousPolygon) + "\"");
                context.write(outputKey, outputValue);
                for (int j = 1; j < polygonShells.size() - 1; j++) {
                    Polygon polygon = geometryFactory.createPolygon(polygonShells.get(j));
                    outputValue.set(value + "," + ((j+1) * interval) + ",\"" +  wktWriter.write(polygon.difference(previousPolygon)) + "\"");
                    context.write(outputKey, outputValue);
                    previousPolygon = polygon;

                }
            }
        }
    }

    private String[] generateRandomString(int numberOfWords)
    {
        String[] randomStrings = new String[numberOfWords];
        Random random = new Random();
        for(int i = 0; i < numberOfWords; i++)
        {
            char[] word = new char[random.nextInt(8) + 3];
            for(int j = 0; j < word.length; j++)
            {
                word[j] = (char)('a' + random.nextInt(26));
            }
            randomStrings[i] = new String(word);
        }
        return randomStrings;
    }

}
