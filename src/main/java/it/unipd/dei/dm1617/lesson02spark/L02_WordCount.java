package it.unipd.dei.dm1617.lesson02spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class L02_WordCount {

  // The classic word count example, using data from a given file
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

    // Now we are ready to count all the occurences. We use the
    // datatype Tuple2<T1, T2> to work with key-value pairs.
    JavaRDD<Tuple2<String, Integer>> dCounts = dWords
      .groupBy((x) -> x) // First, we group all the occurences of the same work on the same reducer
      .map((tuple) -> {
        // The groupby operation results in a RDD of Tuple2 objects, where the key
        // is the word, and the value is an `Iterable` of occurrences.
        // we unwind the iterator to count its elements.
        String word = tuple._1();
        Iterator<String> occurrences = tuple._2().iterator();
        int cnt = 0;
        while(occurrences.hasNext()) {
          occurrences.next();
          cnt++;
        }
        return new Tuple2<>(word, cnt);
      }).sortBy((tuple) -> tuple._2(), true, numPartitions); // Optionally, we sort the output vector by increasing value of the second element of the tuple, that is the count of occurrences.

    // we can then collect locally the result to print it on the console
    List<Tuple2<String, Integer>> lCounts = dCounts.collect();
    lCounts.forEach((tuple) -> {
      String word = tuple._1();
      int count = tuple._2();
      System.out.println(word + " :: " + count);
    });
  }
}
