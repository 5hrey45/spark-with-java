package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(36);
        inputData.add(9);
        inputData.add(25);
        inputData.add(81);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));

        // only used for testing
//        sqrtRdd.foreach(value -> System.out.println(value));

        // syntactic sugar for achieving the above result
        sqrtRdd.collect().forEach(System.out::println);

        // the above line of code originally is sqrtRdd.foreach(System.out::println);
        // but it crashes on some machines with sophisticated CPU's and the println method
        // is not serializable (remember that the driver serializes the function and sends it
        // to worker nodes through the network
        // we use collect() which collects the data from all the nodes (partitions) and returns
        // a java list on which we can use the original line of code.

        // node, in spark, the foreach method on RDD is all lowercase, but in java it's forEach

        System.out.println(result);

        // how many elements in sqrtRdd
//        System.out.println(sqrtRdd.count());

        // using just map and reduce
        JavaRDD<Long> countRdd = sqrtRdd.map(value -> 1L);
        Long count = countRdd.reduce((value1, value2) -> value1 + value2);

        System.out.println(count);

        sc.close();
    }
}
