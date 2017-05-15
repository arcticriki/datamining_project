package it.unipd.dei.dm1617.death_mining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;


import java.util.ArrayList;
import java.util.List;

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
        double minSup = 0.2;

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

                                if (!PropertyFilters.reject(prop))
                                    transaction.add(prop);
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


        double minlift = 1;
        JavaRDD<AssociationRules.Rule<Property>> rules = model.generateAssociationRules(0).toJavaRDD().filter(
                rule ->
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
                    return ((conf / suppConseq) > minlift);
                }
        );
        List<AssociationRules.Rule<Property>> filterRules = rules.collect();

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

        for(AssociationRules.Rule<Property> filterRule : filterRules){
            String line = filterRule.javaAntecedent() + " => " + filterRule.javaConsequent();
            System.out.println(line);
        }


    }
}
