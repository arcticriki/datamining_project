package it.unipd.dei.dm1617.death_mining;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by tonca on 05/05/17.
 */
public class testFPGrowth {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        List<String> columns = null;
        // read column names
        try{
            BufferedReader reader = new BufferedReader(new FileReader("data/columns.csv"));
            String line = reader.readLine();
            columns = Arrays.asList(line.split(","));

        } catch (Exception e)
        {
            System.exit(0);
        }

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";
        double sampleProbability = 0.3;
        double minSup = 0.7;

        System.out.println("Sampling with probability " + sampleProbability + " and importing data");
        List<String> finalColumns = columns;
        JavaRDD<List<Property>> transactions = sc.textFile(filename)
                .sample(false, sampleProbability)
                .map(
                    line -> {
                        List<Property> transaction = new ArrayList<>();
                        String[] fields = line.split(",");

                        for (int i = 0; i < fields.length; i++) {
                            String columnName = finalColumns.get(i);
                            String columnContent = fields[i];
                            boolean reject = false;

                            switch (columnName){
                                
                                case "ActivityCode":
                                    reject = columnContent.equals("99");
                                    break;
                                    
                                case "InjuryAtWork":
                                    reject = columnContent.equals("U");
                                    break;

                                case "RaceImputationFlag":
                                        reject = true;
                                    break;

                                case "AgeType":
                                    reject = true;
                                    break;

                                case "AgeRecode27":
                                    reject = true;
                                    break;

                                case "AgeRecode52":
                                    reject = true;
                                    break;

                                case "BridgedRaceFlag":
                                    reject = true;
                                    break;

                                case "AgeSubstitutionFlag":
                                    reject = true;
                                    break;

                                case "InfantAgeRecode22":
                                    reject = true;
                                    break;

                                case "InfantCauseRecode130":
                                    reject = true;
                                    break;

                                case "EducationReportingFlag":
                                    reject = true;
                                    break;
                            }

                            if (!reject)
                                transaction.add(new Property(i, columnName, columnContent));
                        }
                        return transaction;
                    }
                );



        long transactionsCount = transactions.count();
        //transactions.map(t ->  t.stream().filter(p -> !p.getValue().equals("0")).collect(Collectors.toList()));

        System.out.println("Number of transactions after sampling: " + transactionsCount);

        long import_data = System.currentTimeMillis();

        System.out.println("[read dataset] Elapsed time: "+ ((import_data-start)/1000.0) + " s" );

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSup)
                .setNumPartitions(10);
        FPGrowthModel<Property> model = fpg.run(transactions);


        for (FPGrowth.FreqItemset<Property> itemset: model.freqItemsets().toJavaRDD().collect()) {
            System.out.println("[" + itemset.javaItems() + "], " + ((float) itemset.freq() / (float) transactionsCount));
        }

        long end = System.currentTimeMillis();

        System.out.println("[frequent itemsets] Elapsed time: " + ((end-import_data)/1000.0) + " s" );
        double minConfidence = 0.1;
        for (AssociationRules.Rule<Property> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            if (rule.confidence() < 0.8)
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }


    }



}
