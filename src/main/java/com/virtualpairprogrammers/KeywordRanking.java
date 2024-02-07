package com.virtualpairprogrammers;

import jdk.jfr.internal.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class KeywordRanking {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("D:/Practicals/Starting Workspace/Project/src/main/resources/subtitles/input-spring.txt");

        JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentences -> sentences.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> wordsRdd = lettersOnlyRdd.flatMap(sentences -> Arrays.asList(sentences.split(" ")).iterator());

        JavaRDD<String> noBlankWords = wordsRdd.filter(words -> words.trim().length() > 0);

        JavaRDD<String> interestingWords = noBlankWords.filter(word -> Util.isNotBoring(word));
        interestingWords.mapToPair(value -> new Tuple2<>(value, 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10)
                .forEach(System.out::println);
        sc.close();
    }
}
