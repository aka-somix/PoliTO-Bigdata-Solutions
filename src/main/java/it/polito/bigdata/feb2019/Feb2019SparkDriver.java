package it.polito.bigdata.feb2019;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class Feb2019SparkDriver {

    private static final String name = "Big Data Architectures Exam - February 2019";
    /**
     * TODO: Scrivi la consegna per A
     * @param sourceRDD : Source RDD from text file
     */
    private static void examPartA(JavaRDD<Poi> sourceRDD) {
        JavaRDD<String> itCitiesWithTaxi = sourceRDD
                .filter(poi -> poi.getCountry().equals("Italy"))
                .filter(poi -> poi.getSubcategory().equals("taxi"))
                .map(poi -> poi.getCity());

        JavaRDD<String> itCitiesWithBus = sourceRDD
                .filter(poi -> poi.getCountry().equals("Italy"))
                .filter(poi -> poi.getSubcategory().equals("busstop"))
                .map(poi -> poi.getCity());

        JavaRDD<String> itCitiesWithTaxiWithoutBus = itCitiesWithTaxi.subtract(itCitiesWithBus);

        // TODO (aka-somix) understand why is not working
        //itCitiesWithTaxiWithoutBus.saveAsTextFile(outputPath + "/examPartA");
    }

    /**
     * TODO: Scrivi la consegna per B
     * @param sourceRDD : Source RDD from text file
     */
    private static void examPartB(JavaRDD<Poi> sourceRDD) {

        JavaPairRDD<String,Integer> cityAndMuseumCountPair = sourceRDD
                .filter(poi -> poi.getCountry().equals("Italy"))
                .mapToPair(
                poi -> {
                    if (poi.getSubcategory().equals("museum")) {
                        return new Tuple2<String, Integer>(poi.getCity(), 1);
                    }
                    else {
                        return new Tuple2<String, Integer>(poi.getCity(), 0);
                    }
                }
        );

        // Reduce on every city to compute sum
        JavaPairRDD<String, Integer> museumCountByCity = cityAndMuseumCountPair.reduceByKey(
                (v1, v2) -> v1 + v2
        );

        long cities = museumCountByCity.map(pair -> pair._1()).count();
        double museums = museumCountByCity.map(pair -> pair._2()).reduce((v1, v2) -> v1 + v2);
        double average = museums / cities;

        JavaRDD<String> citiesWithMoreMuseumsThanAvg = museumCountByCity
                .filter(pair -> pair._2() > average)
                .map(pair -> pair._1());

        System.out.println(citiesWithMoreMuseumsThanAvg.collect().toString());
    }

    public static void run(String inputPath, String outputPath) {
        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf()
                .setAppName(Feb2019SparkDriver.name)
                .setMaster("local");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
         *      DATA FORMAT:
         *      +---------+----------+-----------+------+---------+----------+-------------+
         *      | POI ID  | Latitude | Longitude | City | Country | Category | SubCategory |
         *      +---------+----------+-----------+------+---------+----------+-------------+
         */
        JavaRDD<Poi> pois = sc.textFile(inputPath).map(text -> new Poi(text));

        Feb2019SparkDriver.examPartA(pois);

        Feb2019SparkDriver.examPartB(pois);
    } //run

}
