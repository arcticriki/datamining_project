package it.unipd.dei.dm1617.death_mining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tonca on 26/05/17.
 *
 * Classe per analizzare sottogruppi (es. Maschi, Femmine, Vecchi, Neri, Suicidi, cose così insomma... )
 *
 * Per selezionare le classi da isolare bisogna passare per linea di comando in ordine:
 * - il nome della colonna (es. Sex, "Binned Age")
 * - il valore (es. M, Adult)
 *
 */
public class SubgroupMining {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Import preprocessed data
        JavaRDD<List<Property>> transactions = sc.objectFile("results/preprocessed");

        // Removing too frequent items
        transactions = transactions.map(
            itemset -> {
                List<Property> newSet = new ArrayList<>();
                for(Property item : itemset) {
                    if (!PropertyFilters.rejectUselessAndFrequent(item)) {
                        newSet.add(item);
                    }
                }
                return newSet;
            }
        ).filter( itemset -> {
            boolean contains = false;
            for (Property item : itemset) {
                if (item._1().equals(args[0]) && item._2().equals(args[1]))
                    contains = true;
            }
            return Boolean.valueOf(contains);
        })
        .map(transaction -> {
            Property reject = null;
            for (Property item : transaction) {
                if (item._1().equals(args[0]))
                    reject = item;
            }
            transaction.remove(reject);
            return transaction;
        });

        long transactionsCount = transactions.count();

        double minSup = 0.1;

        // FREQUENT ITEMSETS MINING
        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSup)
                .setNumPartitions(10);
        FPGrowthModel<Property> model = fpg.run(transactions);

        // OUTPUT FREQUENT ITEMSETS
        ArrayList<String> outputLines = new ArrayList<>();
        outputLines.add("Item,Support");
        for (FPGrowth.FreqItemset<Property> itemset: model.freqItemsets().toJavaRDD().collect()) {
            String line = itemset.javaItems().toString().replace(",", "")
                    +"," + ((float) itemset.freq() / (float) transactionsCount);
            System.out.println(line);
            outputLines.add(line);
        }
        File directory = new File("results/");
        if (! directory.exists()){
            directory.mkdir();
        }
        // Writing output to a file
        Path file = Paths.get("results/frequent-itemsets-"+args[0]+":"+args[1]+".csv");
        try {
            Files.write(file, outputLines, Charset.forName("UTF-8"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }


        // ASSOCIATION RULES MINING (all metrics)
        double minConfidence = 0.5;
        outputLines.clear();
        outputLines.add("Antecedent,Consequent,Confidence");
        for (AssociationRules.Rule<Property> rule
                : model.generateAssociationRules(minConfidence)
                .toJavaRDD()
                .sortBy((rule) -> rule.confidence(), false, 1)
                .collect())
        {
            String line = rule.javaAntecedent().toString().replace(",","")
                    + "," + rule.javaConsequent().toString().replace(",","")
                    + "," + rule.confidence();
            System.out.println(line);
            outputLines.add(line);
        }

        // OUTPUT ASSOCIATION RULES
        file = Paths.get("results/association-rules-"+args[0]+":"+args[1]+".csv");
        try {
            Files.write(file, outputLines, Charset.forName("UTF-8"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
