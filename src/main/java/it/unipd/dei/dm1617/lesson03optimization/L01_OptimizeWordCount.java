package it.unipd.dei.dm1617.lesson03optimization;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.nio.file.*;
import java.util.*;

// In this lesson, we see how to optimize the classic word-count example.
public class L01_OptimizeWordCount {

  // This is the first version we saw yesterday, where we group
  // together all occurences of the same word.
  static JavaPairRDD<String, Integer> wordCount1(JavaRDD<String> docs, int nTop) {
    return docs
      .flatMap((doc) -> Arrays.stream(doc.split(" ")).iterator())
      .groupBy((w) -> w)
      .mapToPair((tuple) -> {
        String word = tuple._1();
        Iterator<String> occurrences = tuple._2().iterator();
        int cnt = 0;
        while(occurrences.hasNext()) {
          occurrences.next();
          cnt++;
        }
        return new Tuple2<>(word, cnt);
      });
  }

  // This is the second version we saw yesterday, which corresponds to
  // the algorithm presented in class by Professor Pietracaprina
  static JavaPairRDD<String, Integer> wordCount2(JavaRDD<String> docs, int nTop) {
    return docs
      .flatMap((doc) -> Arrays.stream(doc.split(" ")).iterator())
      .mapToPair((word) -> new Tuple2<>(word, 1))
      .reduceByKey((x, y) -> x + y);
  }

  // In this version, we are explicitly doing map-side
  // aggregation. That is, instead of returning tuples like
  //
  //     ("word", 1)
  //
  // from every mapper, we consider partitions as a whole, manually
  // accumulating the counts for each partition. This amounts to doing
  // the same work that Spark performs behind the scenes in version 2
  // of this code. However, since it is specialized, our code is
  // slightly more general.
  //
  // The function `mapPartitionsToPair` allows us to work on all the
  // data within a partition at once.
  static JavaPairRDD<String, Integer> wordCount3(JavaRDD<String> docs, int nTop) {
    return docs
      .flatMap((doc) -> Arrays.stream(doc.split(" ")).iterator())
      .mapPartitionsToPair((partitionWords) -> {
        // This map holds the counts for each word in each partition.
        HashMap<String, Integer> partitionCounts = new HashMap<>();
        while (partitionWords.hasNext()) {
          String word = partitionWords.next();
          int oldCnt = partitionCounts.getOrDefault(word, 0);
          partitionCounts.put(word, oldCnt + 1);
        }
        return partitionCounts
          .entrySet().stream()
          .map((entry) -> new Tuple2<>(entry.getKey(), entry.getValue()))
          .iterator();
      })
      .reduceByKey((x, y) -> x + y);
  }


  // Same as above, but instead of using a generic HashMap, we use a
  // specialized data structure to store unboxed integers as values.
  // In fact, a generic jaa.util.HashMap stores the integer values as
  // boxed Integer instances, rather than primitive `int`. This leads
  // to many short-lived objects that put much pressure on the garbage
  // collector.
  static JavaPairRDD<String, Integer> wordCount4(JavaRDD<String> docs, int nTop) {
    return docs
      .flatMap((doc) -> Arrays.stream(doc.split(" ")).iterator())
      .mapPartitionsToPair((partitionWords) -> {
        // The map defined below, from the it.unimi.dsi.fastutil library,
        // stores integer values as unboxed `int`s. The rest of the code is
        // almost identical.
        Object2IntOpenHashMap<String> partitionCounts = new Object2IntOpenHashMap<>();
        partitionCounts.defaultReturnValue(0);
        while (partitionWords.hasNext()) {
          String word = partitionWords.next();
          // Since we are using a specialized data structure, we have some
          // specialized methods at our disposal, like the one below.
          partitionCounts.addTo(word, 1);
        }
        return partitionCounts
          .entrySet().stream()
          .map((entry) -> new Tuple2<>(entry.getKey(), entry.getValue()))
          .iterator();
      })
      .reduceByKey((x, y) -> x + y);
  }


  public static void main(String[] args) throws IOException {
    int version = Integer.parseInt(args[0]);
    String path = args[1];

    int nTop = 100;

    // Initialize Spark
    SparkConf sparkConf = new SparkConf(true).setAppName("Word count optimization");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // Read the data from the text file
    int numPartitions = sc.defaultParallelism();
    System.err.println("Splitting data from " + path + " in " + numPartitions + " partitions");
    JavaRDD<String> dDocs = sc.textFile(path, numPartitions).cache();
    // Here we invoke dDocs.count() so to force the loading of data,
    // otherwise we would also time this operation.
    System.out.println("Loaded " + dDocs.count() + " documents");

    JavaPairRDD<String, Integer> dWordCounts = null;
    long start = System.currentTimeMillis();
    switch (version) {
      case 1:
        dWordCounts = wordCount1(dDocs, nTop);
        break;
      case 2:
        dWordCounts = wordCount2(dDocs, nTop);
        break;
      case 3:
        dWordCounts = wordCount3(dDocs, nTop);
        break;
      case 4:
        dWordCounts = wordCount4(dDocs, nTop);
        break;
    }
    // We _must_ call an action before taking the end time, otherwise
    // we are measuring the wrong thing.
    long cnt = dWordCounts.count();
    long end = System.currentTimeMillis();
    long elapsed = end - start;
    System.out.println("Counted " + cnt + " words overall");
    System.err.println("Time elapsed: " + elapsed + " milliseconds");

    // The stuff below is just to save the results in a CSV, don't mind it.
    Path csvFile = Paths.get("bench-word-count.csv");
    if (!Files.exists(csvFile)) {
      Files.createFile(csvFile);
      Files.write(csvFile, "input,version,time\n".getBytes());
    }
    PrintWriter out = new PrintWriter(Files.newOutputStream(csvFile, StandardOpenOption.APPEND));
    out.println(path + ", " + version + ", " + elapsed);
    out.close();
  }

}
