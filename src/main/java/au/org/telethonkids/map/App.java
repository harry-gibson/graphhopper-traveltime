package au.org.telethonkids.map;

import com.google.common.collect.Sets;
import com.graphhopper.GraphHopper;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.EncodingManager;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Set;

public class App
{
    private static final long MEGABYTE = 1024L * 1024L;


    public static GraphHopper getOSMGraph(String osmFile, String graphLocation, String mode) {
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

    public static Set<CSVRecord> getCSVRecords(String csvFile) throws IOException {
        Reader in = new FileReader(csvFile);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        Set<CSVRecord> recordsSet = Sets.newHashSet();
        for (CSVRecord record : records) {
            recordsSet.add(record);
        }
        in.close();
        return recordsSet;
    }
}
