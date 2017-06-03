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

    public static JavaRDD<List<Property>> dataImport(JavaSparkContext sc, String filename, double sampleProbability) {

        Broadcast<FieldDecoder> fd = sc.broadcast(new FieldDecoder());

        return sc.textFile(filename)
                .sample(false, sampleProbability)
                .map(
                        line -> {
                            List<Property> transaction = new ArrayList<>();
                            String[] fields = line.split(",");

                            for (int i = 1; i < fields.length; i++) {

                                String columnContent = fields[i];
                                Property prop = new Property(
                                        fd.value().decodeColumn(i),
                                        fd.value().decodeValue(i,columnContent),
                                        columnContent
                                );

                                transaction.add(prop);

                            }

                            return transaction;
                        }
                );
    }

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";
        double sampleProbability = 1;

        // Managing input args
        JavaRDD<List<Property>> transactions = null;
        String option;
        if(args.length>0)
            option = args[0];
        else
            option = "";

        if(option == "original")
                transactions = dataImport(sc, filename, sampleProbability);
        else {
            transactions = Preprocessing.dataImport(sc, filename, sampleProbability);
            option = "";
        }
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

        String dirName = "results/stats_"+option+"/";
        File directory = new File(dirName);

        if (! directory.exists()){
            directory.mkdirs();
        }
        for(Tuple2<String,ArrayList<String>> colList : outputs ) {

            Path file = Paths.get(dirName+colList._1()+"_stats.csv");

            try {
                System.out.println("Saving results in " + dirName + file.getFileName());
                Files.write(file, colList._2(), Charset.forName("UTF-8"));
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
