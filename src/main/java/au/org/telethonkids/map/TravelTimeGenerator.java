package au.org.telethonkids.map;


import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.PathWrapper;
import com.graphhopper.reader.gtfs.GraphHopperGtfs;
import com.graphhopper.reader.gtfs.GtfsStorage;
import com.graphhopper.reader.gtfs.PtFlagEncoder;
import com.graphhopper.reader.gtfs.Request;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FootFlagEncoder;
import com.graphhopper.storage.GHDirectory;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.storage.index.LocationIndex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;



public class TravelTimeGenerator {

    private final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    private static void runGTFSSearch(TravelTimeRunConfig config) throws IOException {

        // get the actual points, do this first so the user doesn't spend ages waiting for the
        // graph to build only to find that the CSV needs fixing
        List<FromTo> fromToPoints = config.LoadFromToPoints();

        // Initialise the GraphHopper, generating the graph if not already done
        // no convenience hopper.close() method on GraphHopperGtfs, so in order to properly close the
        // storage and locationindex at the end we need to keep a reference to them, as well as to the hopper itself
        final PtFlagEncoder ptFlagEncoder = new PtFlagEncoder();
        EncodingManager encodingManager = EncodingManager.create(
                Arrays.asList(ptFlagEncoder, new FootFlagEncoder()), 8);
        GHDirectory directory = GraphHopperGtfs.createGHDirectory(config.getGraphFolder());
        GtfsStorage gtfsStorage = GraphHopperGtfs.createGtfsStorage();
        GraphHopperStorage graphHopperStorage = GraphHopperGtfs.createOrLoad(directory, encodingManager, ptFlagEncoder,
                gtfsStorage, Collections.singleton(config.getGTFSFile()), Collections.singleton(config.getOSMFile()));
        LocationIndex locationIndex = GraphHopperGtfs.createOrLoadIndex(directory, graphHopperStorage);
        // nb GraphHopperGtfs does not extend GraphHopper like GraphHopperOSM does
        GraphHopperGtfs hopper = GraphHopperGtfs.createFactory(ptFlagEncoder, GraphHopperGtfs.createTranslationMap(),
                graphHopperStorage, locationIndex, gtfsStorage)
                .createWithoutRealtimeFeed();

        // setup the output files and write headings
        FileWriter out = new FileWriter(config.getOutputFile());
        CSVPrinter outPrinter = new CSVPrinter(out, CSVFormat.DEFAULT);
        FileWriter outErrors = new FileWriter(config.getOutputErrorsFile());
        CSVPrinter errorPrinter = new CSVPrinter(outErrors, CSVFormat.DEFAULT);
        outPrinter.printRecord(config.getOriginsData().getIdCol() + "_origin",
                config.getDestinationsData().getIdCol() + "_destination"
                ,"total_time","walk_distance_m","straight_line_dist_km", "transit_legs","debug");
        errorPrinter.printRecord("origin_id", "origin_lat", "origin_lon",
                "dest_id", "dest_lat", "dest_lon", "error_type");

        // configure optional parts of the search
        final double max_corvid_endurance;
        if (config.getMaxCrowFliesDistanceKM() == null){
            max_corvid_endurance = Integer.MAX_VALUE;
            System.out.println("*** Maximum distance between points is unrestricted ***");
        }
        else{
            max_corvid_endurance = config.getMaxCrowFliesDistanceKM();
            System.out.println("*** Maximum distance between points is restricted to "
                    + max_corvid_endurance + "km ***");
        }
        GTFSSearchOptions gtfsSearchOptions = config.getTransitOptions();
        if(gtfsSearchOptions == null){
            throw new InvalidObjectException("Config file did not contain a TransitOptions section");
        }
        // GTFS queries need a time, to match to timetables. If the string has been formatted right this
        // should cope with timezones
        final Instant depTime;
        try {
            depTime = OffsetDateTime.parse(gtfsSearchOptions.getEarliestDepartureTime()).toInstant();
        }
        catch (NullPointerException | DateTimeParseException e){
            throw new InvalidObjectException(
                    "TransitOptions section did not contain a valid entry for EarliestDepartureTime");
        }
        final Double maxWalkDistPerLeg = gtfsSearchOptions.getMaxWalkDistancePerLeg(); // may be null
        if (maxWalkDistPerLeg == null){
            System.out.println("*** Maximum walk distance per leg not restricted ***" );
        }
        else{
            System.out.println("*** Maximum walk distance per leg of " + maxWalkDistPerLeg.toString() + "m ***");
        }
        final Integer blockedRouteType;
        switch (gtfsSearchOptions.getExcludeType()){
            case "train":
                blockedRouteType = 2;
                System.out.println("*** Excluding train routes ***");
                break;
            case "bus":
                blockedRouteType = 3;
                System.out.println("*** Excluding bus routes ***");
                break;
            case "ferry":
                blockedRouteType = 4;
                System.out.println("*** Excluding ferry routes ***");
                break;
            case "tram":
                blockedRouteType = 0;
                System.out.println("*** Excluding tram routes ***");
                break;
            default:
                blockedRouteType = null;
                System.out.println("*** All transit types available ***");
        }
        System.out.println("Points loaded: beginning transit routing search for " + fromToPoints.size() + " route pairs");
        try {
            fromToPoints.parallelStream().forEach(
                    fromTo -> {
                        //for (CSVRecord origin : origins){
                        LatLonPair origin = fromTo.getFrom();
                        int originID = origin.getId();
                        double originLon = origin.getLon();
                        double originLat = origin.getLat();
                        LatLonPair dest = fromTo.getTo();
                        int destID = dest.getId();
                        double destLon = dest.getLon();
                        double destLat = dest.getLat();
                        double crowFlies = fromTo.HaversineDistance();
                        if (crowFlies > max_corvid_endurance){
                            try {
                                synchronized (errorPrinter) {
                                    errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                            "Points too far apart");
                                }
                            } catch (IOException ioException) {
                                System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                        originID + " / "+ destID);
                                System.out.println(ioException.getStackTrace()[0].getLineNumber());
                            }
                            return;
                        }
                        if (fromTo.isZeroLength() || crowFlies < 0.02){
                            // try to prevent IndexOutOfBoundsException that seems to occur when routing with
                            // near-identical points
                            try {
                                synchronized (errorPrinter) {
                                    errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                            "Points identical or within 20m");
                                }
                            } catch (IOException ioException) {
                                System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                        originID + " / "+ destID);
                                System.out.println(ioException.getStackTrace()[0].getLineNumber());
                            }
                            return;
                        }

                        Request req = new Request(originLat, originLon, destLat, destLon);
                        //https://github.com/graphhopper/graphhopper/issues/1396
                        req.setEarliestDepartureTime(depTime);
                        if(maxWalkDistPerLeg != null) {
                            req.setMaxWalkDistancePerLeg(maxWalkDistPerLeg);
                        }
                        req.setProfileQuery(false);
                        req.setIgnoreTransfers(true);
                        req.setMaxVisitedNodes(25000); // an arbitrary number i made up, default was 1M
                        if(blockedRouteType != null){
                            req.setBlockedRouteTypes(blockedRouteType); // train
                        }
                        try {
                            GHResponse rsp = hopper.route(req);
                            if (!rsp.hasErrors()) {
                                // filter to only routes that aren't walk-only
                                // avoid calling getAll multiple times as this is presumably a bit expensive
                                List<PathWrapper> responses = rsp.getAll();
                                Supplier<Stream<PathWrapper>> streamSupplier = () -> responses.stream().filter(p -> p.getLegs().size() > 1);
                                Stream<PathWrapper> includingTransit = streamSupplier.get();
                                if (includingTransit.findAny().isPresent()) {
                                    PathWrapper bestRoute = streamSupplier.get()
                                            .min(Comparator.comparing(PathWrapper::getTime))
                                            .orElseThrow(NoSuchElementException::new);
                                    long bestNonWalkingTime = bestRoute.getTime();
                                    double walkDistance = bestRoute.getDistance();
                                    int busLegs = bestRoute.getNumChanges() + 1;
                                    try {
                                        synchronized (outPrinter) {
                                            outPrinter.printRecord(originID, destID, bestNonWalkingTime, walkDistance, crowFlies,
                                                    busLegs, rsp.getDebugInfo());
                                        }
                                    } catch (IOException ioException) {
                                        System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING RESULT WITH POINTS "
                                                + originID + " / "+ destID);
                                        System.out.println(ioException.getStackTrace()[0].getLineNumber());
                                    }
                                }
                                else{
                                    try {
                                        synchronized (errorPrinter) {
                                            errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                                    "No transit route found");
                                        }
                                    } catch (IOException ioException) {
                                        System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                                originID + " / "+ destID);
                                        System.out.println(ioException.getStackTrace()[0].getLineNumber());
                                    }
                                }
                            }
                            else{
                                try {
                                    synchronized (errorPrinter) {
                                        errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                                "Routing error: " + rsp.toString());
                                    }
                                } catch (IOException ioException) {
                                    System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                            originID + " / "+ destID);
                                    System.out.println(ioException.getStackTrace()[0].getLineNumber());
                                }
                            }
                        } catch (com.graphhopper.util.exceptions.PointNotFoundException e) {
                            try {
                                synchronized (errorPrinter) {
                                    errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                            "Point not found");
                                }
                            } catch (IOException ioException) {
                                System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                        originID + " / "+ destID);
                                System.out.println(ioException.getStackTrace()[0].getLineNumber());
                            }
                        }
                        catch (java.lang.IndexOutOfBoundsException e){
                            // This seems to occur when start and end points are identical or nearly so. Not certain
                            // how dissimilar they can be without it happening so hard to filter out bad data
                            // (115.89468, -31.83413) / (115.89467, -31.83451) fails
                            System.out.println("*** WARNING - IndexOutOfBoundsException occurred routing with points " +
                                    originID + " / " + destID);
                            e.printStackTrace();
                        }
                    }
                    //}
            );
        }
        finally {
            // ensure that the graph (and csvs, but that's not so important) are properly closed whilst troubleshooting
            // otherwise it gets corrupted every time we get an exception in the routing and has to be re-made
            outPrinter.close();
            errorPrinter.close();
            graphHopperStorage.close();
            locationIndex.close();
        }

    }

    private static void runCarSearch(TravelTimeRunConfig config) throws IOException{

        // get the actual points, do this first so the user doesn't spend ages waiting for the
        // graph to build only to find that the CSV needs fixing
        List<FromTo> fromToPoints = config.LoadFromToPoints();

        // Initialise the GraphHopper, generating the graph if not already done
        // GraphHopperOSM will handle closing storage and locationindex when it itself is closed,
        // unlike GraphHopperGtfs
        GraphHopper hopper = new GraphHopperOSM().setOSMFile(config.getOSMFile()).
                setStoreOnFlush(true).
                setCHEnabled(true).
                setGraphHopperLocation(config.getGraphFolder()).
                setEncodingManager(EncodingManager.create("car")).
                importOrLoad();

        // setup the output files and write headings
        FileWriter out = new FileWriter(config.getOutputFile());
        CSVPrinter outPrinter = new CSVPrinter(out, CSVFormat.DEFAULT);
        FileWriter outErrors = new FileWriter(config.getOutputErrorsFile());
        CSVPrinter errorPrinter = new CSVPrinter(outErrors, CSVFormat.DEFAULT);
        outPrinter.printRecord(config.getOriginsData().getIdCol() + "_origin",
                config.getDestinationsData().getIdCol() + "_destination"
                ,"total_time","total_dist","straight_line_dist","debug");
        errorPrinter.printRecord("origin_id", "origin_lat", "origin_lon",
                "dest_id", "dest_lat", "dest_lon", "error_type");

        // configure optional parts of the search
        // configure optional parts of the search
        final double max_corvid_endurance;
        if (config.getMaxCrowFliesDistanceKM() == null){
            max_corvid_endurance = Integer.MAX_VALUE;
            System.out.println("*** Maximum distance between points is unrestricted ***");
        }
        else{
            max_corvid_endurance = config.getMaxCrowFliesDistanceKM();
            System.out.println("*** Maximum distance between points is restricted to "
                    + max_corvid_endurance + "km ***");
        }
        System.out.println("Points loaded: beginning car routing search for " + fromToPoints.size() + " route pairs");
        try {
            fromToPoints.parallelStream().forEach(
                    fromTo -> {
                        LatLonPair origin = fromTo.getFrom();
                        int originID = origin.getId();
                        double originLon = origin.getLon();
                        double originLat = origin.getLat();
                        LatLonPair dest = fromTo.getTo();
                        int destID = dest.getId();
                        double destLon = dest.getLon();
                        double destLat = dest.getLat();
                        double crowFlies = fromTo.HaversineDistance();
                        if (crowFlies > max_corvid_endurance){
                            try{
                                synchronized (errorPrinter){
                                    errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                            "Points too far apart");
                                }
                            } catch (IOException ioException){
                                System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                        originID + " / "+ destID);
                                System.out.println(ioException.getStackTrace()[0].getLineNumber());
                            }
                            return;
                        }

                        GHRequest req = new GHRequest(originLat, originLon, destLat, destLon);
                        try {
                            GHResponse rsp = hopper.route(req);
                            if (!rsp.hasErrors()) {
                                if (!rsp.getAll().isEmpty()) {
                                    PathWrapper bestRoute = rsp.getBest();
                                    long bestTime = bestRoute.getTime();
                                    double bestRouteDistance = bestRoute.getDistance();
                                    try {
                                        synchronized (outPrinter) {
                                            outPrinter.printRecord(originID, destID, bestTime, bestRouteDistance,
                                                    crowFlies, rsp.getDebugInfo());
                                        }
                                    } catch (IOException ioException) {
                                        System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING RESULT WITH POINTS "
                                                + originID + " / "+ destID);
                                        System.out.println(ioException.getStackTrace()[0].getLineNumber());
                                    }
                                }
                                else{
                                    try{
                                        synchronized (errorPrinter){
                                            errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                                    "No matching car route found");
                                        }
                                    } catch (IOException ioException){
                                        System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                                originID + " / "+ destID);
                                        System.out.println(ioException.getStackTrace()[0].getLineNumber());
                                    }
                                }
                            }
                            else{
                                try {
                                    synchronized (errorPrinter) {
                                        errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                                "Routing error: " + rsp.toString());
                                    }
                                } catch (IOException ioException) {
                                    System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                            originID + " / "+ destID);
                                    System.out.println(ioException.getStackTrace()[0].getLineNumber());
                                }
                            }
                        } catch (com.graphhopper.util.exceptions.PointNotFoundException e) {
                            try {
                                synchronized (errorPrinter) {
                                    errorPrinter.printRecord(originID, originLat, originLon, destID, destLat, destLon,
                                            "Point not found");
                                }
                            } catch (IOException ioException) {
                                System.out.println("*** WARNING - IOEXCEPTION OCCURRED WRITING ERROR MSG WITH POINTS " +
                                        originID + " / "+ destID);
                                System.out.println(ioException.getStackTrace()[0].getLineNumber());
                            }
                        }
                        catch (java.lang.IndexOutOfBoundsException e){
                            System.out.println("*** WARNING - IndexOutOfBoundsException occurred routing with points " +
                                    originID + " / " + destID);
                            e.printStackTrace();
                        }
                    }
            );
        }
        finally {
            // ensure that the graph (and csvs, but that's not so important) are properly closed whilst troubleshooting
            // otherwise it gets corrupted every time we get an exception in the routing and has to be re-made
            outPrinter.close();
            errorPrinter.close();
            hopper.close();
        }
    }

    public static void main( String[] args ) throws IOException {

        String yamlFile = args[0];
        TravelTimeRunConfig config = Utils.ParseConfig(yamlFile);

        System.out.println("Start: " + dtf.format(LocalDateTime.now()));
        App.printMemoryUsage();

        if(config.getGTFSFile() != null) {
            System.out.println("Beginning public transport search");
            runGTFSSearch(config);
        }
        else{
            System.out.println("Beginning car routing search");
            runCarSearch(config);
        }

        System.out.println("End: " + dtf.format(LocalDateTime.now()));
        App.printMemoryUsage();
    }
}
