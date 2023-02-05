package it.polito.bigdata.feb2021;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.List;

public class Feb2021SparkDriver {
    private static final String name = "Big Data Architectures Exam - February 2021";

    private static final int MIN_FAILS = 50;

    /**
     * Production plants with at least one robot with at least 50 failures in year 2020.
     * The application considers only the failures occurred in year 2020 and selects the identifiers (PlantIDs)
     * of the production plants with at least one robot associated with at least 50 failures in year 2020.
     * The PlantIDs of the selected production plants are stored in the first HDFS output folder (one PlantID per line).
     */
    private static void examPartA(JavaRDD<Robot>robots, JavaRDD<Failure> failures) {
        JavaPairRDD<String, Integer> failuresPerRobot = failures
                .filter(f -> {
                    String year = f.getDate().split("/")[0];
                    return year.equals("2020");
                })
                .mapToPair(
                        f -> {
                            return new Tuple2<String, Integer>(f.getRid(), 1);
                        }
                )
                .reduceByKey((v1, v2) -> v1+v2);

        // PAIR with tuples <Robot ID, Plant ID>
        JavaPairRDD<String, String> robotPlantPairs = robots.mapToPair(r -> new Tuple2(r.getId(), r.getPid()));

        JavaRDD<String> resultingProductionPlantIds = failuresPerRobot
                .filter(pair -> pair._2() >= MIN_FAILS)
                .join(robotPlantPairs)
                .map(pair -> pair._2()._2())
                .distinct();

        System.out.println(resultingProductionPlantIds.collect().toString());

        // TODO (aka-somix) understand why is not working
        //resultingProductionPlantIds.saveAsTextFile(outputPath + "/examPartA");
    }

    /**
     * For each production plant compute the number of robots with at least one failure in year 2020.
     * The application considers only the failures occurred in year 2020 and computes for each production plant
     * the number of its robots associated with at least one failure in year 2020.
     * The application stores in the second HDFS output folder, for each production plant,
     * its PlantID and the computed number of robots with at least one failure in year 2020.
     * Pay attention that the output folder must contain also one line for each of the production plants
     * for which all robots had no failures in year 2020.
     * For those production plants the associated output line is (PlantID, 0).
     */
    private static void examPartB(JavaRDD<ProductionPlant> plants, JavaRDD<Robot>robots, JavaRDD<Failure> failures) {
        JavaPairRDD<String, Integer> failuresPerRobot = failures
                .filter(f -> {
                    String year = f.getDate().split("/")[0];
                    return year.equals("2020");
                })
                .mapToPair(
                        f -> {
                            return new Tuple2<String, Integer>(f.getRid(), 1);
                        }
                )
                .reduceByKey((v1, v2) -> v1+v2);

        System.out.println(failuresPerRobot.collect().toString());

        // PAIR with tuples <Robot ID, Plant ID>
        JavaPairRDD<String, String> robotPlantPairs = robots.mapToPair(r -> new Tuple2(r.getId(), r.getPid()));

        JavaPairRDD<String, Integer> plantsWithRobotFailures = failuresPerRobot
                .join(robotPlantPairs)
                .mapToPair(pair -> new Tuple2<String, Integer>(pair._2()._2(), 1))
                .reduceByKey((v1, v2) -> v1+v2);

        JavaPairRDD<String, Integer> plantsWithoutRobotFailures = plants
                .mapToPair(p -> new Tuple2<String, Integer>(p.getId(), 0))
                .subtractByKey(plantsWithRobotFailures);

        JavaPairRDD<String, Integer> resultingPlantIds = plantsWithRobotFailures.union(plantsWithoutRobotFailures);

        System.out.println(resultingPlantIds.collect().toString());

        // TODO (aka-somix) understand why is not working
        //resultingPlantIds.saveAsTextFile(outputPath + "/examPartB");
    }

    public static void run(String prodPlantsPath, String robotsPath, String failuresPath, String outputPath) {
        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf()
                .setAppName(Feb2021SparkDriver.name)
                .setMaster("local");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<ProductionPlant> plants = sc.textFile(prodPlantsPath).map(text -> new ProductionPlant(text));
        JavaRDD<Robot> robots = sc.textFile(robotsPath).map(text -> new Robot(text));
        JavaRDD<Failure> failures = sc.textFile(failuresPath).map(text -> new Failure(text));

        //Feb2021SparkDriver.examPartA(robots, failures);

        Feb2021SparkDriver.examPartB(plants, robots, failures);
    } //run

}
