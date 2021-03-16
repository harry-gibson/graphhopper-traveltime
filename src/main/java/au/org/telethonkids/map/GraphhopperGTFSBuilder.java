package au.org.telethonkids.map;

import com.graphhopper.reader.gtfs.GraphHopperGtfs;
import com.graphhopper.reader.gtfs.GtfsStorage;
import com.graphhopper.reader.gtfs.PtFlagEncoder;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FootFlagEncoder;
import com.graphhopper.storage.GHDirectory;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.storage.index.LocationIndex;

import java.util.Arrays;
import java.util.Collections;


public class GraphhopperGTFSBuilder {
    public static void main( String[] args ) {
        String osmFile = args[0];
        String gtfsFile = args[1];
        String graphLocation = args[2];

        PtFlagEncoder ptFlagEncoder = new PtFlagEncoder();
        EncodingManager encodingManager = EncodingManager.create(Arrays.asList(ptFlagEncoder, new FootFlagEncoder()), 8);
        GHDirectory directory = GraphHopperGtfs.createGHDirectory(graphLocation);
        GtfsStorage gtfsStorage = GraphHopperGtfs.createGtfsStorage();
        GraphHopperStorage graphHopperStorage = GraphHopperGtfs.createOrLoad(directory, encodingManager, ptFlagEncoder, gtfsStorage, Collections.singleton(gtfsFile), Collections.singleton(osmFile));
        LocationIndex locationIndex = GraphHopperGtfs.createOrLoadIndex(directory, graphHopperStorage);
        GraphHopperGtfs.createFactory(ptFlagEncoder, GraphHopperGtfs.createTranslationMap(), graphHopperStorage, locationIndex, gtfsStorage)
                .createWithoutRealtimeFeed();

        graphHopperStorage.close();
        locationIndex.close();
    }
}
