package it.polito.bigdata;

import it.polito.bigdata.feb2019.Feb2019SparkDriver;
import it.polito.bigdata.feb2021.Feb2021SparkDriver;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hi!, welcome to my Solutions Collection for the PoliTO BigData Exam");
        System.out.println("--------------------------------------------------------------------");

        if (args.length < 2) {
            System.out.println("Error with input. You should always provide at least input and output path");
            return;
        }

        //String inputPath = args[0];
        String outputPath = args[1];

        // Run exam
        Feb2021SparkDriver.run(args[0], args[1], args[2], outputPath);
    }
}