package it.polito.bigdata.feb2019;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Feb2019SparkDriver {

    private static final String name = "Spark Exam - February 2019";

    public static void run(String inputPath, String outputPath) {

        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf()
                .setAppName(Feb2019SparkDriver.name)
                .setMaster("local");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession session = SparkSession
                .builder()
                .appName(Feb2019SparkDriver.name)
                .config(conf)
                .getOrCreate();


        Dataset<Row> pois = session.read().format("csv")
                .option("header", false)
                .option("inferSchema", false)
                .load(inputPath)
                .filter("_c4=Italy");

    }
}
