package au.org.telethonkids.map;


import com.graphhopper.GHResponse;
import com.graphhopper.PathWrapper;
import com.graphhopper.reader.gtfs.GraphHopperGtfs;
import com.graphhopper.reader.gtfs.GtfsStorage;
import com.graphhopper.reader.gtfs.PtFlagEncoder;
import com.graphhopper.reader.gtfs.Request;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FootFlagEncoder;
import com.graphhopper.storage.GHDirectory;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.storage.index.LocationIndex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TravelTimeByPTODGenerator {

    private final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    private static final long MEGABYTE = 1024L * 1024L;
    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }
    private final static double MAX_DISTANCE_KM = 150;

    public static void main( String[] args ) throws IOException {

        System.out.println("Start: " + dtf.format(LocalDateTime.now()));
        App.printMemoryUsage();
        String osmFile = args[0];
        String gtfsFile = args[1];
        String graphLocation = args[2];
        String originsFile = args[3];
        String destinationsFile = args [4];
        String outFile = args[5];
        String outErrorsFile = outFile.replace(".csv", ".errors.csv");
        // no convenience hopper.close() method on GraphHopperGtfs, so in order to properly close the
        // storage and locationindex at the end we need to have a reference to them here so don't use a static
        // method in App to create the hopper
        final PtFlagEncoder ptFlagEncoder = new PtFlagEncoder();

        EncodingManager encodingManager = EncodingManager.create(Arrays.asList(ptFlagEncoder, new FootFlagEncoder()), 8);
        GHDirectory directory = GraphHopperGtfs.createGHDirectory(graphLocation);
        GtfsStorage gtfsStorage = GraphHopperGtfs.createGtfsStorage();
        GraphHopperStorage graphHopperStorage = GraphHopperGtfs.createOrLoad(directory, encodingManager, ptFlagEncoder,
                    gtfsStorage, Collections.singleton(gtfsFile), Collections.singleton(osmFile));
        LocationIndex locationIndex = GraphHopperGtfs.createOrLoadIndex(directory, graphHopperStorage);
        // nb GraphHopperGtfs does not extend GraphHopper like GraphHopperOSM does
        GraphHopperGtfs hopper = GraphHopperGtfs.createFactory(ptFlagEncoder, GraphHopperGtfs.createTranslationMap(),
                    graphHopperStorage, locationIndex, gtfsStorage)
                    .createWithoutRealtimeFeed();

        App.printMemoryUsage();
        System.out.println("Network loaded: " + dtf.format(LocalDateTime.now()));

        Set<CSVRecord> origins = App.getLocations(originsFile);
        Set<CSVRecord> destinations = App.getLocations(destinationsFile);

        FileWriter out = new FileWriter(outFile);
        CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT);
        FileWriter outErrors = new FileWriter(outErrorsFile);
        CSVPrinter errorPrinter = new CSVPrinter(outErrors, CSVFormat.DEFAULT);
        printer.printRecord("sa1_origin", "dzn_destination","transit","debug");
        errorPrinter.printRecord("sa1_origin", "origin_lat", "origin_lon",
                "dzn_destination", "dest_lat", "dest_lon");

        // GTFS queries need a time, to match to timetables.
        // pick a random thursday that isn't near bank holidays
        Instant depTime = LocalDateTime.of(2016,10,13,6,30,0)
                .atZone(ZoneId.of("Australia/Perth"))
                .toInstant();
        try {
            origins.parallelStream().limit(100).forEach(
                    origin -> {
            //for (CSVRecord origin : origins){
                        String start = origin.get("uid");
                        Double startLon = Double.parseDouble(origin.get("lon"));
                        Double startLat = Double.parseDouble(origin.get("lat"));
                        for (CSVRecord destination : destinations
                        ) {
                            String end = destination.get("uid");
                            Double endLon = Double.parseDouble(destination.get("lon"));
                            Double endLat = Double.parseDouble(destination.get("lat"));
                            Double dist_km = HaversineDistance.HaversineDistance(startLat, startLon, endLat, endLon);
                            if(dist_km > MAX_DISTANCE_KM){
                                continue;
                            }
                            Request req = new Request(startLat, startLon, endLat, endLon);
                            //https://github.com/graphhopper/graphhopper/issues/1396
                            req.setEarliestDepartureTime(depTime);
                            req.setMaxWalkDistancePerLeg(1500);
                            req.setProfileQuery(false);
                            req.setIgnoreTransfers(true);
                            req.setBlockedRouteTypes(2); // train?

                            try {
                                GHResponse rsp = hopper.route(req);
                                if (!rsp.hasErrors()) {
                                    // filter to only routes that aren't walk-only
                                    // avoid calling getAll multiple times as this is presumably a bit expensive
                                    List<PathWrapper> responses = rsp.getAll();
                                    Supplier<Stream<PathWrapper>> streamSupplier = () -> responses.stream().filter(p -> p.getLegs().size() > 1);
                                    Stream<PathWrapper> includingTransit = streamSupplier.get();
                                    if (includingTransit.findAny().isPresent()) {
                                        long bestNonWalkingTime = streamSupplier.get()
                                                .min(Comparator.comparing(PathWrapper::getTime))
                                                .orElseThrow(NoSuchElementException::new)
                                                .getTime();
                                        try {
                                            printer.printRecord(start, end, bestNonWalkingTime, rsp.getDebugInfo());
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            } catch (com.graphhopper.util.exceptions.PointNotFoundException e) {
                                try {
                                    errorPrinter.printRecord(start, startLat, startLon, end, endLat, endLon);
                                } catch (IOException ioException) {
                                    ioException.printStackTrace();
                                }
                            }
                        }
                    }
            );
        }
        finally {
            // ensure that the graph (and csvs, but that's not so important) are properly closed whilst troubleshooting
            // otherwise it gets corrupted every time we get an exception in the routing and has to be re-made
            printer.close();
            errorPrinter.close();
            graphHopperStorage.close();
            locationIndex.close();
        }
        System.out.println("End: " + dtf.format(LocalDateTime.now()));
        App.printMemoryUsage();
    }
}
