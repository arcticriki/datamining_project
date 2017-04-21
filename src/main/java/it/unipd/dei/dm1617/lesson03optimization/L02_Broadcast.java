package it.unipd.dei.dm1617.lesson03optimization;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.SizeEstimator;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

// In this second example, we study another optimization technique,
// namely "broadcast variables".
//
// The toy problem here is to obtain a collection of documents containing
// only the top-2000 words of the entire corpus, with the others removed.
public class L02_Broadcast {

  // This class allows to compare tuples by value. We need to define it
  // explicitly instead of using a lambda function because otherwise Spark
  // complains that the lambda function is not serializable, rather misteriously.
  // Doing this way, we can force our comparator to implement the Serializable
  // interface, working around the problem.
  public static class TupleValueComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
      return t1._2().compareTo(t2._2());
    }
  }

  // This helper function returns a set of the top words in a
  // corpus of documents. To make the task more challenging, we add
  // to the output, along with each word, also multiple concatenated
  // replicas of the same word.
  //
  // Conceptually, this code is equivalent to the word count example
  // we have seen before.
  //
  // The reason we add the replicas is that we want to simulate a
  // situation where we need to send large data structures to tasks.
  static Set<String> topWords(JavaRDD<String> dDocs, int nTop) {
    Set<String> result = new HashSet<>();
    List<Tuple2<String, Integer>> lTopList = dDocs
      .flatMap((doc) -> Arrays.stream(doc.split(" ")).iterator())
      .mapToPair((w) -> new Tuple2<>(w, 1))
      .reduceByKey((c1, c2) -> c1 + c2)
      .top(nTop, new TupleValueComparator());

    lTopList.forEach((tup) -> {
      String word = tup._1();
      StringBuilder repetitions = new StringBuilder();
      // Add to the output multiple concatenated replicas of
      // the same word, from 1 to 50.
      for (int i=0; i<50; i++) {
        repetitions.append(word);
        result.add(repetitions.toString());
      }
    });

    return result;
  }

  // This first version of the algorithm references the `topWords`
  // set directly from the lambda function. This causes the set
  // to be serialized along with the code of the lambda function and
  // sent to _all_ the partitions.
  static JavaRDD<String> topWordsOnly1(JavaRDD<String> docs,
                                       Set<String> topWords) {
    return docs.map((doc) -> {
      StringBuilder result = new StringBuilder();
      for (String word : doc.split(" ")) {
        if (topWords.contains(word)) {
          result.append(' ').append(word);
        }
      }
      return result.toString();
    });
  }

  // Wrapping the set in a broadcast variable, as in this second example,
  // allows Spark to send the set to each _machine_ rather than all the
  // partitions. This reduces both network traffic and memory consumption.
  static JavaRDD<String> topWordsOnly2(JavaSparkContext sc,
                                       JavaRDD<String> docs,
                                       Set<String> topWords) {
    Broadcast<Set<String>> bStopWords = sc.broadcast(topWords);

    return docs.map((doc) -> {
      Set<String> sw = bStopWords.getValue();
      StringBuilder result = new StringBuilder();
      for (String word : doc.split(" ")) {
        if (sw.contains(word)) {
          result.append(' ').append(word);
        }
      }
      return result.toString();
    });
  }

  public static void main(String[] args) throws IOException {

    int version = Integer.parseInt(args[0]);

    String dataPath = args[1];

    // Initialize Spark
    SparkConf sparkConf = new SparkConf(true).setAppName("Broadcast example");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    System.err.println("Load data");
    int numPartitions = sc.defaultParallelism();
    JavaRDD<String> dDocs = sc.textFile(dataPath, numPartitions).cache();
    dDocs.count();

    System.err.println("Compute top words");
    Set<String> lTopWords = topWords(dDocs, 2000);
    long lTopWordsSize = SizeEstimator.estimate(lTopWords) / 1024;
    System.out.println("local set of top words " + lTopWordsSize + " kB");


    System.err.println("Run algorithm version " + version);
    long start = System.currentTimeMillis();
    JavaRDD<String> dFiltered = null;
    switch(version) {
      case 1:
        dFiltered = topWordsOnly1(dDocs, lTopWords);
        break;
      case 2:
        dFiltered = topWordsOnly2(sc, dDocs, lTopWords);
        break;
    }
    dFiltered.count();
    long end = System.currentTimeMillis();
    long elapsed = end - start;


    System.err.println("Time elapsed: " + elapsed + " milliseconds");
    Path csvFile = Paths.get("bench-broadcast.csv");
    if (!Files.exists(csvFile)) {
      Files.createFile(csvFile);
      Files.write(csvFile, "input,version,time\n".getBytes());
    }
    PrintWriter out = new PrintWriter(Files.newOutputStream(csvFile, StandardOpenOption.APPEND));
    out.println(dataPath + ", " + version + ", " + elapsed);
    out.close();
  }

}
