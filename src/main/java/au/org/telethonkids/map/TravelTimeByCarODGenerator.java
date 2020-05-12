package au.org.telethonkids.map;


import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

public class TravelTimeByCarODGenerator {

    private final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    private static final long MEGABYTE = 1024L * 1024L;
    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    public static void main( String[] args ) throws IOException {

        System.out.println("Start: " + dtf.format(LocalDateTime.now()));
        App.printMemoryUsage();
        String osmFile = args[0];
        String graphLocation = args[1];
        String originsFile = args[2];
        String destinationsFile = args [3];
        String mode = "car";

        GraphHopper hopper = App.getOSMGraph(osmFile, graphLocation, mode);
        EncodingManager encodingManager = hopper.getEncodingManager();
        FlagEncoder encoder = encodingManager.getEncoder(mode);
        App.printMemoryUsage();
        System.out.println("Network loaded: " + dtf.format(LocalDateTime.now()));

        Set<CSVRecord> origins = App.getLocations(originsFile);
        Set<CSVRecord> destinations = App.getLocations(destinationsFile);

        FileWriter out = new FileWriter(args[4]);
        CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT);
        printer.printRecord("sa1_origin", "dzn_destination","car");
        origins.parallelStream().forEach(
                origin -> {
                    String start = origin.get("uid");
                    Double startLon = Double.parseDouble(origin.get("lon"));
                    Double startLat = Double.parseDouble(origin.get("lat"));
                    for (CSVRecord destination: destinations
                         ) {
                        String end = destination.get("uid");
                        Double endLon = Double.parseDouble(destination.get("lon"));
                        Double endLat = Double.parseDouble(destination.get("lat"));
                        GHRequest req = new GHRequest(startLat, startLon, endLat, endLon);
                        GHResponse rsp = hopper.route(req);
                        if(!rsp.hasErrors()){
                            if(!rsp.getAll().isEmpty()) {
                                long time = rsp.getBest().getTime();
                                try {
                                    printer.printRecord(start, end, time);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
        );
        System.out.println("End: " + dtf.format(LocalDateTime.now()));
        App.printMemoryUsage();
        printer.close();
        hopper.close();
    }
}
