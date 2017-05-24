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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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

        JavaRDD<List<Property>> transactions = sc.objectFile("results/preprocessed");

        long transactionsCount = transactions.count();
        Broadcast<Long> bCount = sc.broadcast(transactionsCount);

        List<Tuple2<String,ArrayList<String>>> outputs = transactions
                .flatMap(itemset -> itemset.iterator())
                .mapToPair(item -> new Tuple2<>(item, 1))
                .reduceByKey((a, b) -> a + b)
                .groupBy(item -> item._1()._1())
                .map((item) -> {
                    ArrayList<String> lines = new ArrayList<>();
                    for( Tuple2<Property,Integer> counted : item._2()) {
                        String line = counted._1().toString().replace(",","") + "," + ((float)counted._2()/bCount.value());
                        lines.add(line);
                    }
                    return new Tuple2<>(item._1(),lines);
                })
                .collect();

        String dirName = "results/stats_preprocessing/";
        File directory = new File(dirName);

        if (! directory.exists()){
            directory.mkdirs();
        }
        for(Tuple2<String,ArrayList<String>> colList : outputs ) {

            Path file = Paths.get(dirName+colList._1()+"_stats.csv");

            try {
                Files.write(file, colList._2(), Charset.forName("UTF-8"));
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
