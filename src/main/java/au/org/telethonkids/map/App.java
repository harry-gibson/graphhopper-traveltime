package au.org.telethonkids.map;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.Set;

/**
 * Hello world!
 *
 */
public class App
{
    private static final long MEGABYTE = 1024L * 1024L;

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


    }

    public static GraphHopper getGraph(String osmFile, String graphLocation, String mode) {
        return new GraphHopperOSM().setOSMFile(osmFile).
                setStoreOnFlush(true).
                setCHEnabled(true).
                setGraphHopperLocation(graphLocation).
                setEncodingManager(EncodingManager.create(mode)).
                importOrLoad();
    }

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    public static void printMemoryUsage() {
        // Get the Java runtime
        Runtime runtime = Runtime.getRuntime();
        // Run the garbage collector
        runtime.gc();
        // Calculate the used memory
        long memory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Used memory is bytes: " + memory);
        System.out.println("Used memory is megabytes: "
                + bytesToMegabytes(memory));
    }

    public static Set<CSVRecord> getLocations(String originsFile) throws IOException {
        Reader in = new FileReader(originsFile);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        Set<CSVRecord> recordsSet = Sets.newHashSet();
        for (CSVRecord record : records) {
            recordsSet.add(record);
        }
        in.close();
        return recordsSet;
    }
}
