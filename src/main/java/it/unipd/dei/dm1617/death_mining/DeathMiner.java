package it.unipd.dei.dm1617.death_mining;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;

import java.util.List;

/**
 * Created by gianluca on 26/05/17.
 */
public class DeathMiner {

    private JavaSparkContext sc;
    private FPGrowthModel<Property> model;
    private JavaRDD<List<Property>> transactions;
    private long transactionCount;

    public DeathMiner(JavaSparkContext sc, JavaRDD<List<Property>> transactions){
        this.sc = sc;
        this.transactions = transactions;
        this.transactionCount = transactions.count();
    }

    public JavaPairRDD<List<Property>, Double> mineFrequentItemsets(double minSup){
        long timeStart = System.currentTimeMillis();

        // extract frequent itemsets with FP-Growth algorithm
        System.out.println("[freq itemsets] Started mining with minimum support = " + minSup);
        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSup)
                .setNumPartitions(sc.defaultParallelism());

        this.model = fpg.run(transactions);
        JavaRDD<FPGrowth.FreqItemset<Property>> freqItemsets = model.freqItemsets().toJavaRDD();

        long timeItemsetMining = System.currentTimeMillis();
        System.out.println("[freq itemsets] Number of frequent itemsets: " + freqItemsets.count() );
        System.out.println("[freq itemsets] Elapsed time: " + ((timeItemsetMining-timeStart)/1000.0));
        long tc = this.transactionCount;

        return freqItemsets.mapToPair(item ->
                new Tuple2<>(item.javaItems(), item.freq()*1.0/tc));
    }

    public JavaRDD<ExtendedRule> mineAssociationRules(){

        long timeStart = System.currentTimeMillis();

        JavaRDD<FPGrowth.FreqItemset<Property>> rddfreqItem = this.model.freqItemsets().toJavaRDD();

        // generate association rules from frequent itemsets
        System.out.println("[association rules] Started mining");

        long tc = this.transactionCount;
        // collect frequent itemsets and their support in a RDD of KeyValue pair, where the key is the itemset
        JavaPairRDD<List<Property>, Double> rddFreqItemAndSupport = rddfreqItem.mapToPair(item ->
                new Tuple2<>(item.javaItems(), item.freq()*1.0/tc));

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
        System.out.println("[association rules] Elapsed time: " + ((timeRuleMining-timeStart)/1000.0));

        return rddResult;
    }
}
