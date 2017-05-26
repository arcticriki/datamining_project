package it.unipd.dei.dm1617.death_mining;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * Created by gianluca on 22/05/2017.
 *
 */
public class testAlternateConviction {
    public static void main(String[] args) {

        double sampleProbability = 0.3;
        double minSup = 0.1;

        long start = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf(true).setAppName("Death Mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";

        System.out.println("[read dataset] Sampling with probability " + sampleProbability + " and importing data");
        JavaRDD<List<Property>> transactions = Preprocessing.dataImport(sc, filename, sampleProbability);

        long transactionsCount = transactions.count();

        System.out.println("[read dataset] Number of transactions after sampling: " + transactionsCount);
        long timeImport = System.currentTimeMillis();

        System.out.println("[read dataset] Elapsed time: "+ ((timeImport-start)/1000.0) + " s" );


        // extract frequent itemsets with FP-Growth algorithm
        System.out.println("[freq itemsets] Started mining with minimum support = " + minSup);
        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSup)
                .setNumPartitions(sc.defaultParallelism());
        FPGrowthModel<Property> model = fpg.run(transactions);
        JavaRDD<FPGrowth.FreqItemset<Property>> rddfreqItem = model.freqItemsets().toJavaRDD();

        long timeItemsetMining = System.currentTimeMillis();
        System.out.println("[freq itemsets] Number of frequent itemsets: " + rddfreqItem.count() );
        System.out.println("[freq itemsets] Elapsed time: " + ((timeItemsetMining-timeImport)/1000.0));


        // generate association rules from frequent itemsets
        System.out.println("[association rules] Started mining");

        // collect frequent itemsets and their support in a RDD of KeyValue pair, where the key is the itemset
        JavaPairRDD<List<Property>, Double> rddFreqItemAndSupport = rddfreqItem.mapToPair(item ->
                new Tuple2<>(item.javaItems(), item.freq()*1.0/transactionsCount));

       // compute association rules with minConf = 0: filtering is done at a later stage
        JavaRDD<AssociationRules.Rule<Property>> rules = model.generateAssociationRules(0).toJavaRDD();

        // compute extra metrics
        JavaRDD<ExtendedRule> rddResult = rules
                .mapToPair(r -> new Tuple2<>(r.javaConsequent(), r))
                .join(rddFreqItemAndSupport)
                .mapToPair(i -> new Tuple2<>(i._2._1, i._2._2)) // <Rule, YSupport>
                .map(i -> {
                    AssociationRules.Rule<Property> rule = i._1;
                    Double suppY = i._2;
                    Double conviction = (1 - suppY) / (1 - rule.confidence());
                    Double lift = rule.confidence()/suppY;
                    return new ExtendedRule(rule, lift, conviction);
                });

        long timeRuleMining = System.currentTimeMillis();
        System.out.println("[association rules] Number of mined rules: " + rules.count());
        System.out.println("[association rules] Elapsed time: " + ((timeRuleMining-timeItemsetMining)/1000.0));

        // save results in dedicated folder structure
        String outputdir = new SimpleDateFormat("'results/'yyyyMMdd-HHmmss").format(new Date());
        System.out.println("[saving results] Ouput path: " + outputdir);

        rddResult.sortBy(ExtendedRule::getConfidence, false, 1)
                .map(ExtendedRule::CSVformat)
                .saveAsTextFile(outputdir + "/rules");

        rddFreqItemAndSupport
                .mapToPair(Tuple2::swap)
                .sortByKey(false, 1)
                .map(i -> i._2.toString() + ";" + i._1)
                .saveAsTextFile(outputdir + "/freq-itemsets");

    }
}
