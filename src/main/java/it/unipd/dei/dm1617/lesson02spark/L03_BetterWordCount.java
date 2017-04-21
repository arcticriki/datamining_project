package it.unipd.dei.dm1617.lesson02spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class L03_BetterWordCount {

  // A better implementation of WordCount, using the algorithm
  // seen in class
  public static void main(String[] args) {

    // Parse the path to the data
    String path = args[0];

    // Initialize Spark
    SparkConf sparkConf = new SparkConf(true).setAppName("Compute primes");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // Read the data from the text file
    int numPartitions = sc.defaultParallelism();
    System.err.println("Splitting data from " + path + " in " + numPartitions + " partitions");
    JavaRDD<String> dDocs = sc.textFile(path, numPartitions);

    // Split into words. The method `flatMap` requires the lambda to return an
    // iterator. We use Arrays.stream(...).iterator() to avoid unnecessary copying
    // of the underlying data.
    JavaRDD<String> dWords = dDocs.flatMap((doc) -> Arrays.stream(doc.split(" ")).iterator());

    // Now we can apply the MR algorithm for word count.
    // Note that we are using `mapToPair` instead of `map`, since
    // it returns a `JavaPairRDD` object, which has methods specialized
    // to work on key-value pairs, like the `reduceByKey` operation we use here.
    JavaPairRDD<String, Integer> dCounts = dWords
      .mapToPair((w) -> new Tuple2<>(w, 1))
      .reduceByKey((x, y) -> x + y);

    // Instead of sorting and collecting _all_ the values on the master
    // machine, we take only the top 100 words by count.
    // In general this operation is safer, since we can bound the number
    // of elements that are collected by the master, thus avoiding OutOfMemory errors
    List<Tuple2<String, Integer>> lTopCounts = dCounts.top(100, (t1, t2) -> t1._2().compareTo(t2._2()));
    lTopCounts.forEach((tuple) -> {
      String word = tuple._1();
      int count = tuple._2();
      System.out.println(word + " :: " + count);
    });
  }

}
