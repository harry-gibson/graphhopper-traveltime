package au.org.telethonkids.map;

import com.graphhopper.GraphHopper;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.EncodingManager;

public class GraphhopperOSMBuilder {
    public static void main( String[] args ) {
        String osmFile = args[0];
        String graphLocation = args[1];
        String mode = "car";
        GraphHopper hopper = new GraphHopperOSM().setOSMFile(osmFile).
                setStoreOnFlush(true).
                setCHEnabled(true).
                setGraphHopperLocation(graphLocation).
                setEncodingManager(EncodingManager.create(mode)).
                importOrLoad();

        hopper.close();
    }
}
