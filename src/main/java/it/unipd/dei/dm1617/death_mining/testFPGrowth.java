package it.unipd.dei.dm1617.death_mining;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.codehaus.janino.Java;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tonca on 05/05/17.
 */
public class testFPGrowth {

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
        double sampleProbability = 0.3;
        double minSup = 0.1;

        JavaRDD<List<Property>> transactions = dataImport(sc, filename, sampleProbability);

        long transactionsCount = transactions.count();

        System.out.println("Number of transactions after sampling: " + transactionsCount);

        long import_data = System.currentTimeMillis();

        System.out.println("[read dataset] Elapsed time: "+ ((import_data-start)/1000.0) + " s" );

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

        long end = System.currentTimeMillis();

        System.out.println("[frequent itemsets] Elapsed time: " + ((end-import_data)/1000.0) + " s" );

        // ASSOCIATION RULES MINING
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
