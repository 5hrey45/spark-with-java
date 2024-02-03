package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Tuples {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(36);
        inputData.add(9);
        inputData.add(25);
        inputData.add(81);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> OriginalIntegers = sc.parallelize(inputData);

        // use tuples to store multiple data in a single RDD
        // we can use up to Tuple22 form scala
        // in this case, we are using Tuple2 class which can store 2 values inside the tuple
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = OriginalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value)));

        sc.close();
    }
}
