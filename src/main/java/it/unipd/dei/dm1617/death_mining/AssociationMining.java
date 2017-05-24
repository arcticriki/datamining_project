package it.unipd.dei.dm1617.death_mining;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.codehaus.janino.Java;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tonca on 05/05/17.
 */
public class AssociationMining {


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Import preprocessed data
        JavaRDD<List<Property>> transactions = sc.objectFile("results/preprocessed");

        long transactionsCount = transactions.count();

        double minSup = 0.1;

        // FREQUENT ITEMSETS MINING
        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSup)
                .setNumPartitions(10);
        FPGrowthModel<Property> model = fpg.run(transactions);

        // OUTPUT FREQUENT ITEMSETS
        ArrayList<String> outputLines = new ArrayList<>();

        for (FPGrowth.FreqItemset<Property> itemset: model.freqItemsets().toJavaRDD().collect()) {
            String line = "[" + itemset.javaItems() + "], " + ((float) itemset.freq() / (float) transactionsCount);
            System.out.println(line);
            outputLines.add(line);
        }
        File directory = new File("results/");
        if (! directory.exists()){
            directory.mkdir();
        }
        // Writing output to a file
        Path file = Paths.get("results/frequent-itemsets_Confidence.txt");
        try {
            Files.write(file, outputLines, Charset.forName("UTF-8"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }


        // ASSOCIATION RULES MINING (all metrics)
        double minConfidence = 0.3;
        outputLines.clear();
        for (AssociationRules.Rule<Property> rule
                : model.generateAssociationRules(minConfidence)
                .toJavaRDD()
                .sortBy((rule) -> rule.confidence(), false, 1)
                .collect())
        {
            if (rule.confidence() > 0.8) {
                String line = rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence();
                System.out.println(line);
                outputLines.add(line);
            }
        }

        // OUTPUT ASSOCIATION RULES
        file = Paths.get("results/association-rules_Confidence.txt");
        try {
            Files.write(file, outputLines, Charset.forName("UTF-8"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

}
