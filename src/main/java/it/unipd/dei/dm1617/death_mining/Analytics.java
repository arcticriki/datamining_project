package it.unipd.dei.dm1617.death_mining;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Array;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by tonca on 11/05/17.
 */
public class Analytics {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";
        Broadcast<FieldDecoder> fd = sc.broadcast(new FieldDecoder());
        double sampleProbability = 0.6;

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
                                        i,
                                        columnContent,
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

        List<Tuple2<Integer,ArrayList<String>>> outputs = transactions
                .flatMap(itemset -> itemset.iterator())
                .mapToPair(item -> new Tuple2<>(item, 1))
                .reduceByKey((a, b) -> a + b)
                .groupBy(item -> item._1()._1())
                .map((Tuple2<Integer, Iterable<Tuple2<Property, Integer>>> item) -> {
                    ArrayList<String> lines = new ArrayList<>();
                    for( Tuple2<Property,Integer> counted : item._2()) {
                        String line = counted._1() + ", " + ((float)counted._2()/bCount.value());
                        lines.add(line);
                    }
                    return new Tuple2<>(item._1(),lines);
                })
                .collect();

        for(Tuple2<Integer,ArrayList<String>> colList : outputs ) {

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
