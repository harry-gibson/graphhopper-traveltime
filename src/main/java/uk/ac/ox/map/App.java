package uk.ac.ox.map;

import com.graphhopper.GraphHopper;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.util.EncodingManager;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        String osmFile = args[0];
        String graphLocation = args[1];
        String mode = args [2]
        GraphHopper hopper = new GraphHopperOSM().setOSMFile(osmFile).
                setStoreOnFlush(true).
                setCHEnabled(true).
                setGraphHopperLocation(graphLocation).
                setEncodingManager(EncodingManager.create(mode)).
                importOrLoad();
    }
}
