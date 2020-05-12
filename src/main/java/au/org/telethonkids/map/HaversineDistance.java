package au.org.telethonkids.map;

/**
 * This is the implementation Haversine Distance Algorithm between two places
 * @author harrygibson from ananth https://gist.github.com/vananth22/888ed9a22105670e7a4092bdcf0d72e4
 * R = earth’s radius (mean radius = 6,371km)
Δlat = lat2− lat1
Δlong = long2− long1
a = sin²(Δlat/2) + cos(lat1).cos(lat2).sin²(Δlong/2)
c = 2.atan2(√a, √(1−a))
d = R.c
 *
 */

public class HaversineDistance {

    /**
     Calculates the _approximate_ distance in km between two lat/lon pairs on Earth using the Haversine formula
     **/
    public static double HaversineDistance(double lat1, double lon1, double lat2, double lon2) {
        // TODO Auto-generated method stub
        final int R = 6371; // Radius of the earth
        Double latDistance = toRad(lat2-lat1);
        Double lonDistance = toRad(lon2-lon1);
        Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
                        Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        Double distance = R * c;
        return distance; // in km
    }

    private static Double toRad(Double value) {
        return value * Math.PI / 180;
    }

}