package it.polito.bigdata;

import it.polito.bigdata.feb2019.Feb2019SparkDriver;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hi!, welcome to my Solution Collection for the PoliTO BigData Exam");
        System.out.println("--------------------------------------------------------------------");

        if (args.length < 2) {
            System.out.println("Error with input. You should always provide at least input and output path");
            return;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        System.out.println(String.format("Passing: INPUT PATH: %s, OUTPUT_PATH: %s", inputPath, outputPath));

        // Run exam
        Feb2019SparkDriver.run(inputPath, outputPath);
    }
}