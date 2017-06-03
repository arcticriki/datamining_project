package it.unipd.dei.dm1617.death_mining;

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

    public List<Property> filterFrequentItemsets(double maxFreq) {

        long transactionsCount = transactions.count();
        Broadcast<Long> bCount = sc.broadcast(transactionsCount);

        // FILTERING TOO FREQUENT ITEMS
        List<Property> outputs = transactions
                .flatMap(itemset -> itemset.iterator())
                .mapToPair(item -> new Tuple2<>(item, 1))
                .reduceByKey((a, b) -> a + b)
                .filter(item -> Boolean.valueOf((1.0*item._2())/bCount.getValue()>maxFreq))
                .map(Tuple2::_1)
                .collect();

        System.out.println("Too frequent items: "+outputs);

        transactions = transactions.map(itemset -> {
            List<Property> tmp = new ArrayList<>();
            for (Property item : itemset) {
                if (!outputs.contains(item)) {
                    tmp.add(item);
                }
            }
            return tmp;
        });

        return outputs;
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
        // compute extra metrics
        JavaRDD<ExtendedRule> rddResult =  model.generateAssociationRules(0).toJavaRDD()
                .mapToPair(r -> new Tuple2<>(r.javaConsequent(), r))
                .join(rddFreqItemAndSupport)
                .mapToPair(i -> new Tuple2<>(i._2._1, i._2._2)) // <Rule, YSupport>
                .mapToPair(i -> {
                    AssociationRules.Rule<Property> rule = i._1;
                    Double suppY = i._2;
                    return new Tuple2<>(rule.javaAntecedent(), new Tuple2<>(rule, suppY));
                })
                .join(rddFreqItemAndSupport)
                .map(i -> {
                    AssociationRules.Rule<Property> rule = i._2._1._1;
                    Double suppX = i._2._2;
                    Double suppY = i._2._1._2;
                    Double suppXUY = rule.confidence()*suppX;
                    Double conviction = (1 - suppY) / (1 - rule.confidence());
                    Double lift = rule.confidence()/suppY;
                    return new ExtendedRule(rule, suppXUY, lift, conviction);
                });

        long timeRuleMining = System.currentTimeMillis();
        System.out.println("[association rules] Number of mined rules: " + rddResult.count());
        System.out.println("[association rules] Elapsed time: " + ((timeRuleMining-timeStart)/1000.0));

        return rddResult;
    }
}
