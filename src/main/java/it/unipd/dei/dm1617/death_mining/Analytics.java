package it.unipd.dei.dm1617.death_mining;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by tonca on 11/05/17.
 *
 * This is a class for generic statistical analysis of the dataset
 *
 */
public class Analytics {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";
        Broadcast<FieldDecoder> fd = sc.broadcast(new FieldDecoder());
        double sampleProbability = 1;

        System.out.println("Sampling with probability " + sampleProbability + " and importing data");

        JavaRDD<List<Property>> transactions = sc.textFile(filename)
                .sample(false, sampleProbability)
                .map(
                        line -> {
                            List<Property> transaction = new ArrayList<>();
                            String[] fields = line.split(",");

                            for (int i = 0; i < fields.length; i++) {

                                String columnContent = fields[i];

                                Property prop = new Property(
                                        fd.value().decodeColumn(i),
                                        fd.value().decodeValue(i,columnContent)
                                );

                                transaction.add(prop);
                            }

                            return transaction;
                        }
                );
        long transactionsCount = transactions.count();
        Broadcast<Long> bCount = sc.broadcast(transactionsCount);

        List<Tuple2<String,List<String>>> outputs = transactions
            .map((itemset) -> itemset.stream())
            .mapPartitionsToPair((itemset) -> {
                // This map holds the counts for each word in each partition.
                Object2IntOpenHashMap<Property> partitionCounts = new Object2IntOpenHashMap<>();
                partitionCounts.defaultReturnValue(0);
                while (itemset.hasNext()) {
                    Property item = itemset.next().findAny().get();
                    int oldCnt = partitionCounts.getOrDefault(item, 0);
                    partitionCounts.put(item, oldCnt + 1);
                }
                return partitionCounts
                        .entrySet().stream()
                        .map((entry) -> new Tuple2<>(entry.getKey(), entry.getValue()))
                        .iterator();
            })
            .reduceByKey((x, y) -> x + y)
            .groupBy(item -> item._1()._1())
            .map((item) -> new Tuple2<>(item._1(), StreamSupport
                    .stream(item._2().spliterator(), false)
                    .map((elem) -> elem._1() + ", " + ((float)elem._2()/bCount.value()))
                    .collect(Collectors.toList())))
            .collect();


        File directory = new File("results/stats/");
        if (! directory.exists()){
            directory.mkdirs();
        }
        for(Tuple2<String,List<String>> colList : outputs ) {

            Path file = Paths.get("results/stats/"+fd.value().decodeColumn(colList._1())+"_stats.txt");
            try {
                Files.write(file, colList._2(), Charset.forName("UTF-8"));
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
