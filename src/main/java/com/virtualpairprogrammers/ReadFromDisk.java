package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ReadFromDisk {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("D:/Practicals/Starting Workspace/Project/src/main/resources/subtitles/input.txt");

        initialRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .foreach(value -> System.out.println(value));

        sc.close();
    }
}
