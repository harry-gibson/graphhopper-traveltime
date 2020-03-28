package uk.ac.ox.map;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.graphhopper.GraphHopper;
import com.graphhopper.isochrone.algorithm.ContourBuilder;
import com.graphhopper.isochrone.algorithm.Isochrone;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.QueryGraph;
import com.graphhopper.routing.util.DefaultEdgeFilter;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.index.QueryResult;

import com.graphhopper.util.PMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.triangulate.ConformingDelaunayTriangulator;
import org.locationtech.jts.triangulate.ConstraintVertex;
import org.locationtech.jts.triangulate.quadedge.QuadEdgeSubdivision;
import org.locationtech.jts.triangulate.quadedge.Vertex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.IntStream;

public class IsochroneGenerator {

    private final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    private final static GeometryFactory geometryFactory = new GeometryFactory();
    private final static WKTWriter wktWriter = new WKTWriter();

    private static final long MEGABYTE = 1024L * 1024L;

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }


    public static void main( String[] args ) throws IOException {

        LocalDateTime start = LocalDateTime.now();
        System.out.println(dtf.format(start));

        printMemoryUsage();
        int timeLimit = 8100;
        int numberOfBuckets = 9;
        // Network
        String osmFile = args[0];
        String graphLocation = args[1];
        String mode = args [2];
        GraphHopper hopper = getGraph(osmFile, graphLocation, mode);
        EncodingManager encodingManager = hopper.getEncodingManager();
        FlagEncoder encoder = encodingManager.getEncoder(mode);
        // Cache
        Reader inCache = new FileReader(args[4]);
        Iterable<CSVRecord> cache = CSVFormat.RFC4180.withFirstRecordAsHeader().withTrim().parse(inCache);
        Set<Coordinate> caches = Sets.newHashSet();
        for (CSVRecord record : cache) {
            Double lat = Double.parseDouble(record.get("lat"));
            Double lon = Double.parseDouble(record.get("lon"));
            Coordinate coordinate = new Coordinate(lon,lat);
            caches.add(coordinate);
        }
        // Origins
        Reader in = new FileReader(args[3]);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
        List<CSVRecord> csvRecords = Lists.newArrayList();
        for (CSVRecord record : records) {
            String latString = record.get("Lat");
            String lonString = record.get("Long");
            if(!latString.isEmpty() && !lonString.isEmpty()){
                csvRecords.add(record);
            }
        }
//        FileWriter out = new FileWriter(args[4]);
        BufferedWriter out = Files.newBufferedWriter(
                Paths.get(args[4]),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE);
        CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT);
        printer.printRecord("time", "isochrone","lat","lon");
        IntStream.range(0, csvRecords.size()).parallel().forEach(
                i -> {
                    CSVRecord record = csvRecords.get(i);
                    Double lat = Double.parseDouble(record.get("Lat"));
                    Double lon = Double.parseDouble(record.get("Long"));
                    Coordinate coordinate = new Coordinate(lon,lat);
                    if(!caches.contains(coordinate)){
                        List<List<Coordinate>> isochrone = buildIsochrone(timeLimit, numberOfBuckets, hopper, encoder, lat, lon);
                        printMemoryUsage();
                        if(isochrone != null){
                            List<Coordinate[]> polygonShells = buildIsochronePolygons(lat, lon, isochrone);
                            Polygon previousPolygon = geometryFactory.createPolygon(polygonShells.get(0));
                            int interval = timeLimit / numberOfBuckets;
                            try {
                                printer.printRecord( interval , wktWriter.write(previousPolygon), record.get("Lat"), record.get("Long"));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            for (int j = 1; j < polygonShells.size() - 1; j++) {
                                Polygon polygon = geometryFactory.createPolygon(polygonShells.get(j));
                                try {
                                    printer.printRecord( ((j+1) * interval) , wktWriter.write(polygon.difference(previousPolygon)), record.get("Lat"), record.get("Long"));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                previousPolygon = polygon;

                            }
                        }
                    }
                }
        );
        printMemoryUsage();
        printer.close();

        LocalDateTime end = LocalDateTime.now();
        System.out.println(dtf.format(end));
    }

    private static ArrayList<Coordinate[]> buildIsochronePolygons(Double lat, Double lon, List<List<Coordinate>> isochrone) {
        Collection<ConstraintVertex> sites = new ArrayList<>();
        for (int j = 0; j < isochrone.size(); j++) {
            List<Coordinate> level = isochrone.get(j);
            for (Coordinate coord : level) {
                ConstraintVertex site = new ConstraintVertex(coord);
                site.setZ(j);
                sites.add(site);
            }
        }

        ConformingDelaunayTriangulator conformingDelaunayTriangulator = new ConformingDelaunayTriangulator(sites, 0.0);
        conformingDelaunayTriangulator.setConstraints(new ArrayList<>(), new ArrayList<>());
        conformingDelaunayTriangulator.formInitialDelaunay();
        conformingDelaunayTriangulator.enforceConstraints();

        Geometry convexHull = conformingDelaunayTriangulator.getConvexHull();
        if (!(convexHull instanceof Polygon)) {
            throw new IllegalArgumentException("Too few points found. "
                    + "Please try a different 'point' or a larger 'time_limit'.");
        }
        QuadEdgeSubdivision tin = conformingDelaunayTriangulator.getSubdivision();
        for (Vertex vertex : (Collection<Vertex>) tin.getVertices(true)) {
            if (tin.isFrameVertex(vertex)) {
                vertex.setZ(Double.MAX_VALUE);
            }
        }
        ArrayList<Coordinate[]> polygonShells = new ArrayList<>();
        ContourBuilder contourBuilder = new ContourBuilder(tin);
        for (int j = 0; j < isochrone.size() - 1; j++) {
            MultiPolygon multiPolygon = contourBuilder.computeIsoline((double) j + 0.5);
            Polygon maxPolygon = heuristicallyFindMainConnectedComponent(multiPolygon, geometryFactory.createPoint(new Coordinate(lon, lat)));
            polygonShells.add(maxPolygon.getExteriorRing().getCoordinates());
        }
        return  polygonShells;
    }

    private static List<List<Coordinate>> buildIsochrone(int timeLimit, int numberOfBuckets, GraphHopper hopper, FlagEncoder encoder, Double lat, Double lon) {
        QueryResult qr = hopper.getLocationIndex().findClosest(lat, lon, DefaultEdgeFilter.allEdges(encoder));
        Graph graph = hopper.getGraphHopperStorage();
        QueryGraph queryGraph = new QueryGraph(graph);
        try {
            queryGraph.lookup(Collections.singletonList(qr));
            PMap pMap = new PMap();
            Isochrone isochrone = new Isochrone(queryGraph, new FastestWeighting(encoder, pMap), false);
            isochrone.setTimeLimit(timeLimit);
            return isochrone.searchGPS(qr.getClosestNode(), numberOfBuckets);
        } catch (IllegalStateException exception) {
            return null;
        }


    }


    private static GraphHopper getGraph(String osmFile, String graphLocation, String mode) {
        return new GraphHopperOSM().setOSMFile(osmFile).
                setStoreOnFlush(true).
                setCHEnabled(true).
                setGraphHopperLocation(graphLocation).
                setEncodingManager(EncodingManager.create(mode)).
                importOrLoad();
    }

    private static Polygon heuristicallyFindMainConnectedComponent(MultiPolygon multiPolygon, Point point) {
        int maxPoints = 0;
        Polygon maxPolygon = null;
        for (int j = 0; j < multiPolygon.getNumGeometries(); j++) {
            Polygon polygon = (Polygon) multiPolygon.getGeometryN(j);
            if (polygon.contains(point)) {
                return polygon;
            }
            if (polygon.getNumPoints() > maxPoints) {
                maxPoints = polygon.getNumPoints();
                maxPolygon = polygon;
            }
        }
        return maxPolygon;
    }

    private static void printMemoryUsage() {
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
}
