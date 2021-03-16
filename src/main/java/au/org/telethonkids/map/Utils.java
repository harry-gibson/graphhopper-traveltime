package au.org.telethonkids.map;
import com.fasterxml.jackson.databind.MapperFeature;
import com.google.common.collect.Lists;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

enum SearchType{
    CAR_SEARCH,
    TRANSIT_SEARCH,
    ISOCHRONE_GENERATION
}

class LatLonPair{
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LatLonPair point = (LatLonPair) o;
        return Double.compare(point.lat, lat) == 0 &&
                Double.compare(point.lon, lon) == 0 &&
                id == point.id;
    }

    public boolean positionEquals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LatLonPair point = (LatLonPair) o;
        return Double.compare(point.lat, lat) == 0 &&
                Double.compare(point.lon, lon) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lat, lon, id);
    }

    private double lat;
    private double lon;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    private int id;

    public LatLonPair(double lat, double lon, int id) {
        this.lat = lat;
        this.lon = lon;
        this.id = id;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }


}

class FromTo{
    public FromTo(LatLonPair from, LatLonPair to) {
        this.from = from;
        this.to = to;
    }
    public FromTo(double originLat, double originLon, int originID, double destLat, double destLon, int destId){
        this.from = new LatLonPair(originLat, originLon, originID);
        this.to = new LatLonPair(destLat, destLon, destId);
    }

    public LatLonPair getFrom() {
        return from;
    }

    public void setFrom(LatLonPair from) {
        this.from = from;
    }

    public LatLonPair getTo() {
        return to;
    }

    public void setTo(LatLonPair to) {
        this.to = to;
    }

    private LatLonPair from;
    private LatLonPair to;

    private static Double toRad(Double value) {
        return value * Math.PI / 180;
    }

    /**
     Calculates the _approximate_ distance in km between two lat/lon pairs on Earth using the Haversine formula
     * from https://gist.github.com/vananth22/888ed9a22105670e7a4092bdcf0d72e4
     * R = earth’s radius (mean radius = 6,371km)
     * Δlat = lat2− lat1
     * Δlong = long2− long1
     * a = sin²(Δlat/2) + cos(lat1).cos(lat2).sin²(Δlong/2)
     * c = 2.atan2(√a, √(1−a))
     * d = R.c
     **/
    public double HaversineDistance(){

        final int R = 6371; // Radius of the earth
        double lat1 = from.getLat();
        double lat2 = to.getLat();
        double lon1 = from.getLon();
        double lon2 = to.getLon();

        double latDistance = toRad(lat2-lat1);
        double lonDistance = toRad(lon2-lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
                        Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c; // dist in km
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FromTo fromTo = (FromTo) o;
        return from.equals(fromTo.from) &&
                to.equals(fromTo.to);
    }

    public boolean isZeroLength(){
        return from.positionEquals(to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }
}

class GTFSSearchOptions{
    public GTFSSearchOptions(String earliestDepartureTime, String excludeType, double maxWalkDistancePerLeg) {
        EarliestDepartureTime = earliestDepartureTime;
        ExcludeType = excludeType;
        MaxWalkDistancePerLeg = maxWalkDistancePerLeg;
    }

    public GTFSSearchOptions() {
    }

    public String getEarliestDepartureTime() {
        return EarliestDepartureTime;
    }

    public void setEarliestDepartureTime(String earliestDepartureTime) {
        EarliestDepartureTime = earliestDepartureTime;
    }

    public String getExcludeType() {
        return ExcludeType;
    }

    public void setExcludeType(String excludeType) {
        ExcludeType = excludeType;
    }

    public Double getMaxWalkDistancePerLeg() {
        return MaxWalkDistancePerLeg;
    }

    public void setMaxWalkDistancePerLeg(double maxWalkDistancePerLeg) {
        MaxWalkDistancePerLeg = maxWalkDistancePerLeg;
    }

    private String EarliestDepartureTime;
    private String ExcludeType;
    private Double MaxWalkDistancePerLeg;

}

class PointSourceConfig {
    public PointSourceConfig(String filePath, String latCol, String lonCol, String idCol) {
        this.filePath = filePath;
        this.latCol = latCol;
        this.lonCol = lonCol;
        this.idCol = idCol;
    }

    private String filePath;
    private String latCol;
    private String lonCol;
    private String idCol;

    public PointSourceConfig() {
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getLatCol() {
        return latCol;
    }

    public void setLatCol(String latCol) {
        this.latCol = latCol;
    }

    public String getLonCol() {
        return lonCol;
    }

    public void setLonCol(String lonCol) {
        this.lonCol = lonCol;
    }

    public String getIdCol() {
        return idCol;
    }

    public void setIdCol(String idCol) {
        this.idCol = idCol;
    }
}

/**
 * Class that will be instantiated by jackson yaml parser, matching the fields in the .yml config file
 */
class TravelTimeRunConfig{
    private String OSMFile;
    private String GTFSFile;
    private String GraphFolder;
    private PointSourceConfig OriginsData;
    private PointSourceConfig DestinationsData;
    private GTFSSearchOptions TransitOptions;
    private Double MaxCrowFliesDistanceKM;
 
    private String IsochroneTimes;

    private String OutputFile;
    private String OutputErrorsFile;

    public GTFSSearchOptions getTransitOptions() {
        return TransitOptions;
    }

    public List<Integer> getIsochroneTimes(){
        if (this.IsochroneTimes == null){
            return null;
        }
        return Arrays.stream(this.IsochroneTimes.split(" +")).map(Integer::parseInt).collect(Collectors.toList());
    }

    public void setTransitOptions(GTFSSearchOptions transitOptions) {
        TransitOptions = transitOptions;
    }

    public Double getMaxCrowFliesDistanceKM() {
        return MaxCrowFliesDistanceKM;
    }

    public void setMaxCrowFliesDistanceKM(double maxCrowFliesDistanceKM) {
        MaxCrowFliesDistanceKM = maxCrowFliesDistanceKM;
    }

    public String getOutputFile() {
        return OutputFile;
    }

    public void setOutputFile(String outputFile) {
        OutputFile = outputFile;
    }

    public String getOutputErrorsFile() {
        return OutputErrorsFile;
    }

    public void setOutputErrorsFile(String outputErrorsFile) {
        OutputErrorsFile = outputErrorsFile;
    }
    public String getOSMFile() {
        return OSMFile;
    }

    public void setOSMFile(String OSMFile) {
        this.OSMFile = OSMFile;
    }

    public String getGTFSFile() {
        return GTFSFile;
    }

    public void setGTFSFile(String GTFSFile) {
        this.GTFSFile = GTFSFile;
    }

    public String getGraphFolder() {
        return GraphFolder;
    }

    public void setGraphFolder(String graphFolder) {
        GraphFolder = graphFolder;
    }

    public PointSourceConfig getOriginsData() {
        return OriginsData;
    }

    public void setOriginsData(PointSourceConfig originsData) {
        OriginsData = originsData;
    }

    public PointSourceConfig getDestinationsData() {
        return DestinationsData;
    }

    public void setDestinationsData(PointSourceConfig destinationsData) {
        DestinationsData = destinationsData;
    }

    public TravelTimeRunConfig(String OSMFile, String GTFSFile, String graphFolder, PointSourceConfig originsData, PointSourceConfig destinationsData) {
        this.OSMFile = OSMFile;
        this.GTFSFile = GTFSFile;
        GraphFolder = graphFolder;
        OriginsData = originsData;
        DestinationsData = destinationsData;
    }

    public TravelTimeRunConfig() {
    }

    public SearchType getSearchType(){
        if (this.DestinationsData == null){
            return SearchType.ISOCHRONE_GENERATION;
        }
        else if (this.GTFSFile != null){
            return SearchType.TRANSIT_SEARCH;
        }
        else{
            return SearchType.CAR_SEARCH;
        }
    }
    
    /**
     * Attempts to load the origin and destination points from the configured file(s)
     * @return Set of FromTo points
     * @throws IOException
     */
    public List<FromTo> LoadFromToPoints() throws IOException {
        if (this.OriginsData.getFilePath().equals(this.DestinationsData.getFilePath())){
            return Utils.PointsFromSingleFile(
                    OriginsData.getFilePath(), OriginsData.getLatCol(), OriginsData.getLonCol(), OriginsData.getIdCol(),
                    DestinationsData.getLatCol(), DestinationsData.getLonCol(), DestinationsData.getIdCol());
        }
        else{
            return Utils.PointsFromODFiles(
                    OriginsData.getFilePath(), OriginsData.getLatCol(), OriginsData.getLonCol(), OriginsData.getIdCol(),
                    DestinationsData.getFilePath(), DestinationsData.getLatCol(), DestinationsData.getLonCol(), DestinationsData.getIdCol());
        }
    }
}

class Utils {

    public static TravelTimeRunConfig ParseConfig(String configYamlPath) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        TravelTimeRunConfig config = mapper.readValue(
                new File(configYamlPath),
                TravelTimeRunConfig.class);
        return config;
    }

    /**
     * Gets the cartesian product of From-To point pairs from an origins and a destinations CSV file
     * @param originsFile path to the origins csv
     * @param originLatCol column name of the latitude column in the origins file
     * @param originLonCol column name of the longitude column in the origins file
     * @param destsFile path to the destinations csv
     * @param destLatCol column name of the latitude column in the dests file
     * @param destLonCol column name of the longitude column in the dests file
     * @return
     * @throws IOException, NumberFormatException
     */
    public static List<FromTo> PointsFromODFiles(String originsFile,
                                                 String originLatCol, String originLonCol, String originIDCol,
                                                 String destsFile,
                                                 String destLatCol, String destLonCol, String destIDCol)
            throws IOException , NumberFormatException
    {
        Set<CSVRecord> originRecords = App.getCSVRecords(originsFile);
        Set<CSVRecord> destRecords = App.getCSVRecords(destsFile);
        List<FromTo> fromToPoints = Lists.newArrayList();//Sets.newHashSet();
        for (CSVRecord originRecord: originRecords){
            Double originLat = Double.parseDouble(originRecord.get(originLatCol));
            Double originLon = Double.parseDouble(originRecord.get(originLonCol));
            int originID = Integer.parseInt(originRecord.get(originIDCol));
            LatLonPair origin = new LatLonPair(originLat, originLon, originID);
            for(CSVRecord destRecord : destRecords){
                Double destLat = Double.parseDouble(destRecord.get(destLatCol));
                Double destLon = Double.parseDouble(destRecord.get(destLonCol));
                int destID = Integer.parseInt(destRecord.get(destIDCol));
                LatLonPair dest = new LatLonPair(destLat, destLon, destID);
                fromToPoints.add(new FromTo(origin, dest));
            }
        }
        return fromToPoints;
    }

    /**
     * Parses a CSV file containing two lat/lon pairs on each row into a set of From-To point pairs
     * @param csvFile Path to the csv file
     * @param originLatCol Column name of the start latitude column
     * @param originLonCol Column name of the start longitude column
     * @param destLatCol Column name of the end latitude column
     * @param destLonCol Column name of the end longitude column
     * @return
     * @throws IOException, NumberFormatException
     */
    public static List<FromTo> PointsFromSingleFile(String csvFile,
                                         String originLatCol, String originLonCol, String originIDCol,
                                         String destLatCol, String destLonCol, String destIDCol) throws IOException, NumberFormatException {
        Set<CSVRecord> csvRecords = App.getCSVRecords(csvFile);
        List<FromTo> fromToPoints = Lists.newArrayList();
        for (CSVRecord csvRecord: csvRecords){
            Double originLat = Double.parseDouble(csvRecord.get(originLatCol));
            Double originLon = Double.parseDouble(csvRecord.get(originLonCol));
            int originID = Integer.parseInt(csvRecord.get(originIDCol));
            Double destLat = Double.parseDouble(csvRecord.get(destLatCol));
            Double destLon = Double.parseDouble(csvRecord.get(destLonCol));
            int destID = Integer.parseInt(csvRecord.get(destIDCol));
            fromToPoints.add(new FromTo(originLat, originLon, originID, destLat, destLon, destID));
        }
        return fromToPoints;
    }


}

