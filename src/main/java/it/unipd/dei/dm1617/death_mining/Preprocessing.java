package it.unipd.dei.dm1617.death_mining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tonca on 23/05/17.
 */
public class Preprocessing {
    private static JavaRDD<List<Property>> dataImport(JavaSparkContext sc, String filename, double sampleProbability) {

        Broadcast<FieldDecoder> fd = sc.broadcast(new FieldDecoder());

        System.out.println("Sampling with probability " + sampleProbability + " and importing data");

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

                                //Insert here PropertyFilters.binningColumns(prop) method
                                prop = PropertyFilters.binningProperties(prop);

                                // Excluding useless items and verifying that they are unique
                                if (!PropertyFilters.rejectUselessAndFrequent(prop) && !transaction.contains(prop)) {
                                    transaction.add(prop);
                                }
                            }

                            return transaction;
                        }
                );

    }


    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";
        double sampleProbability = 1;

        JavaRDD<List<Property>> transactions = dataImport(sc, filename, sampleProbability);

        long transactionsCount = transactions.count();

        String outputDirName = "results/preprocessed/out";
        File outputDirectory = new File(outputDirName);
        while (outputDirectory.exists()) {
            outputDirName = outputDirName+"_";
            outputDirectory = new File(outputDirName);
        }
        transactions.saveAsObjectFile(outputDirName);


        System.out.println("Number of transactions after sampling: " + transactionsCount);

        long import_data = System.currentTimeMillis();

        System.out.println("[read dataset] Elapsed time: "+ ((import_data-start)/1000.0) + " s" );


    }
}
