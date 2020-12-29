package bmstu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;


public class SparkExample {
    public static final int ID_COLUM_ID = 1;
    public static final int AIRPORTS_ID_COLUM_ID = 0;
    public static final String FLIGHT_DELIMETR = ",";
    public static final int ORIGIN_AIROPORT_ID = 11;
    public static final int ORIGIN_DEST_ID = 14;
    public static final int ORIGIN_DELAY_ID = 18;
    public static final int ORIGIN_CANCELD_ID = 19;
    public static final String REGEX_BACKSLASH = "\"";
    public static final String EMPTY_STRING = "";

    public static String removeQuates(String line){
        return line.replace(REGEX_BACKSLASH, EMPTY_STRING);
    }
    public static String concanitate(String [] arrayOfStrings , int firstItemNumber){
        StringBuilder builder = new StringBuilder();
        for ( int i = firstItemNumber; i < arrayOfStrings.length; i++){
            builder.append(arrayOfStrings[i]);
        }
        return builder.toString();
    }
    private static final Function2 REMOVE_HEADER = new Function2<Integer , Iterator<String> , Iterator<String>>() {
        @Override
        public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
            if (ind == 0 && iterator.hasNext()){
                iterator.next();
                return iterator;
            }else {
                return iterator;
            }
        }
    };
    private static Tuple2<Integer,String> makePair(String line) {
        String[] airportsNames = line.split(FLIGHT_DELIMETR);
        String airportsConcatinetedID= concanitate(airportsNames , ID_COLUM_ID);
        String airport = airportsNames[AIRPORTS_ID_COLUM_ID];
        String preParseID = removeQuates(airportsConcatinetedID);
        int airaceID = Integer.parseInt(preParseID);
        return new Tuple2<>(airaceID, airport);
    }

    private static Tuple2<Tuple2<Integer, Integer>, AiroportDataSeriazable>  makeFlightData (String line){
        String[] items = line.split(FLIGHT_DELIMETR);
        double timeDelay = items[ORIGIN_DELAY_ID].isEmpty() ? 0
                : Double.parseDouble(items[ORIGIN_DELAY_ID]);
        Boolean isCanceled = items[ORIGIN_CANCELD_ID].isEmpty();
        return new Tuple2<>(
            new Tuple2<>(Integer.parseInt(items[ORIGIN_AIROPORT_ID]),
                         Integer.parseInt(items[ORIGIN_DEST_ID])),
            new AiroportDataSeriazable(
                    Integer.parseInt(items[ORIGIN_AIROPORT_ID]),
                    Integer.parseInt(items[ORIGIN_DEST_ID]),
                    timeDelay,
                    isCanceled
            )
        );
    }

    public static void main(String args[]) throws Exception {
        if (args.length != 3) {
            System.err.println("SparkApp exception");
            System.exit(1);
        }
        String airoportPath = args[0];
        String flightPath = args[1];
        SparkConf conf = new SparkConf().setAppName("sample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputAiroportRDD = sc.textFile(airoportPath)
                .mapPartitionsWithIndex(REMOVE_HEADER, false);
        JavaRDD<String> inputFlightRDD = sc.textFile(flightPath)
                .mapPartitionsWithIndex(REMOVE_HEADER, false);
        JavaPairRDD<Integer , String> airoportNames = inputAiroportRDD
                .mapToPair(SparkExample::makePair);

        Map<Integer , String> airName = airoportNames.collectAsMap();

        JavaPairRDD<Tuple2<Integer, Integer>, AiroportDataSeriazable> resRDD = inputFlightRDD
                .mapToPair(SparkExample::makeFlightData);

        JavaPairRDD<Tuple2<Integer, Integer>, FlightDataSerializable> reducedRes = resRDD
                .combineByKey(
                    p -> {
                            int delayedCnt = p.getDestAiroportID() > 0 || p.isCanceld() ? 1 : 0;
                            return new FlightDataSerializable(p.getTimeDelay() , delayedCnt , 1);
                    },
                    FlightDataSerializable::addValue,
                    FlightDataSerializable::Add
                );
        final Broadcast<Map<Integer, String>> airportsBroadcasted =
                sc.broadcast(airName);
        JavaRDD<String> resOutput = reducedRes.map(
                item ->{
                    String output = "";
                    output += airportsBroadcasted.value().get(item._1._1) + " "
                            + item._1._1 + " "
                            + airportsBroadcasted.value().get(item._1._2) + " "
                            + item._1._2 + " \n"
                            + "max delay= "
                            + item._2.getMaxDelay() + " \n"
                            + "delayed and canceled procent "
                            + item._2.ReturnProcent();
                    return output;
                }
        );
        resOutput.saveAsTextFile("hdfs://localhost:9000/user/macos/output");
    }
}