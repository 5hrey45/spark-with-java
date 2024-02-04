package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;

public class Filter {
    public static void main(String[] args) {
        ArrayList<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> sentences = sc.parallelize(inputData);
//        JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//
//        // filter takes in a lambda which needs to return either true or false
//        // the values which return true will be kept in the new RDD and the values
//        // which return false will be omitted form the resulting RDD
//        JavaRDD filteredWords = words.filter(word -> word.length() > 1 ? true : false);
//        filteredWords.foreach(value -> System.out.println(value));


        // doing all the operations in Fluent API
        sc.parallelize(inputData)
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(value -> value.length() > 1)
                .foreach(value -> System.out.println(value));

        // the filter function by default has true value
        // so the below two lines of code has the same effect
        // filter(value -> value.length() > 1 ? true : false)
        // filter(value -> value.length() > 1)

        sc.close();
    }
}
