package uk.ac.ox.map;

import com.google.common.collect.Lists;
import com.graphhopper.GraphHopper;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.EncodingManager;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, ParseException {
        WKBReader wkbReader = new WKBReader();
        Reader in = new FileReader("/home/cavargasru/Downloads/zwe_isochrones_900.csv");
        Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
        List<CSVRecord> csvRecords = Lists.newArrayList();
        int i = 0;
        System.out.println(csvRecords.size());




        FileWriter out = new FileWriter("/home/cavargasru/Downloads/zwe_isochrones_wkt_900.csv");
        CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT);
        printer.printRecord("tid", "geom");



        for (CSVRecord record : records) {

            String geom = record.get(record.size() - 1);

            byte[] bytes = WKBReader.hexToBytes(geom);

            printer.printRecord(i , wkbReader.read(bytes).toText());
            i++;
        }
        printer.close();
        System.exit(0);

        String osmFile = args[0];
        String graphLocation = args[1];
        String mode = args [2];
        GraphHopper hopper = new GraphHopperOSM().setOSMFile(osmFile).
                setStoreOnFlush(true).
                setCHEnabled(true).
                setGraphHopperLocation(graphLocation).
                setEncodingManager(EncodingManager.create(mode)).
                importOrLoad();
    }
}
