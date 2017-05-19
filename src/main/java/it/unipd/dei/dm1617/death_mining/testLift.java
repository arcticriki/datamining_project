package it.unipd.dei.dm1617.death_mining;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;


import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by Marco on 14/05/2017.
 */
public class testLift {
    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";
        Broadcast<FieldDecoder> fd = sc.broadcast(new FieldDecoder());
        double sampleProbability = 0.3;
        double minSup = 0.1;

        //Randomly select interesting columns from file columns.csv
        List<String> interestingColumns = new ArrayList<>();
        Random rand = new Random();

        try {
            CSVReader reader = new CSVReader(new FileReader("data/columns.csv"));
            String[] columns = reader.readNext();

            for (String c: columns) {
                int temp = rand.nextInt(2);
                if (temp == 1) interestingColumns.add(c);
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }

        /*
        Save selected interesting columns in a txt file. Just for testing ;)

        Path columnsFile = Paths.get("results/random_interestingColumns.txt");
        try {
            Files.write(columnsFile, interestingColumns, Charset.forName("UTF-8"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        */

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

                                if (!PropertyFilters.rejectByColumn(prop, interestingColumns)) {
                                    transaction.add(prop);
                                }
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



        List<FPGrowth.FreqItemset<Property>> freqItemset = model.freqItemsets().toJavaRDD().collect();


        double deltaLift = 0.1;
        JavaRDD<Tuple2<AssociationRules.Rule<Property>, Double>> rules = model.generateAssociationRules(0).toJavaRDD()
                .map((rule) ->
                {
                    double conf = rule.confidence();
                    long freq = 0;
                    for(FPGrowth.FreqItemset<Property> itemset : freqItemset){
                        if(itemset.javaItems().toString().equals(rule.javaConsequent().toString())){
                            freq = itemset.freq();
                            break;
                        }
                    }
                    if(freq == 0){
                        System.err.println("Error rules filter lift");
                    }
                    double suppConseq = (double) freq / transactionsCount;
                    double lift = conf / suppConseq;
                    return new Tuple2<>(rule, lift);
                })
                .filter(tuple -> (tuple._2 > 1+deltaLift || tuple._2 < 1-deltaLift));



        List<Tuple2<AssociationRules.Rule<Property>, Double>> filterRules = rules.collect();

//        JavaRDD<AssociationRules.Rule<Property>> rules = model.generateAssociationRules(0).toJavaRDD();
//        List<AssociationRules.Rule<Property>> filterRules = new ArrayList<AssociationRules.Rule<Property>>();
//        double minlift = 1;
//        for(AssociationRules.Rule<Property> rule : rules.collect()){
//            double conf = rule.confidence();
//            long freq = 0;
//            for(FPGrowth.FreqItemset<Property> itemset : freqItemset.collect()){
//                if(itemset.javaItems().toString().equals(rule.javaConsequent().toString())){
//                    freq = itemset.freq();
//                    break;
//                }
//            }
//            if(freq == 0){
//                System.err.println("Error rules filter lift");
//            }
//            double suppConseq = (double) freq / transactionsCount;
//            if((conf / suppConseq) > minlift){
//                filterRules.add(rule);
//            }
//        }

        for(Tuple2<AssociationRules.Rule<Property>, Double> filterRule : filterRules){
            String line = filterRule._1.javaAntecedent() + " => " + filterRule._1.javaConsequent() + " lift:" + filterRule._2;
            System.out.println(line);
        }


    }
}
