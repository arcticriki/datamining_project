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

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by Gianluca on 22/05/2017.
 *
 */
public class testAlternateConviction {
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

        System.out.println("Sampling with probability " + sampleProbability + " and importing data");

        JavaRDD<List<Property>> transactions = sc.textFile(filename)
                .sample(false, sampleProbability)
                .map(line -> {
                    List<Property> transaction = new ArrayList<>();
                    String[] fields = line.split(",");

                    for (int i = 0; i < fields.length; i++) {

                        String columnContent = fields[i];
                        Property prop = new Property(
                                fd.value().decodeColumn(i),
                                fd.value().decodeValue(i,columnContent),
                                columnContent
                        );

                        transaction.add(prop);
                    }

                    return PropertyFilters.itemsetFilter(transaction);

                });

        long transactionsCount = transactions.count();
        //transactions.map(t ->  t.stream().filter(p -> !p.getValue().equals("0")).collect(Collectors.toList()));

        System.out.println("Number of transactions after sampling: " + transactionsCount);

        long import_data = System.currentTimeMillis();

        System.out.println("[read dataset] Elapsed time: "+ ((import_data-start)/1000.0) + " s" );

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSup)
                .setNumPartitions(10);
        FPGrowthModel<Property> model = fpg.run(transactions);


        JavaRDD<FPGrowth.FreqItemset<Property>> rddfreqItem = model.freqItemsets().toJavaRDD();

        // collect frequent itemsets and their support in a RDD of KeyValue pair, where the key is the itemset
        JavaPairRDD<List<Property>, Double> rddFreqItemAndSupport = rddfreqItem.mapToPair(item ->
                new Tuple2<>(item.javaItems(), item.freq()*1.0/transactionsCount));

        // save a local copy of frequent itemsets
        List<FPGrowth.FreqItemset<Property>> localFreqItemset = rddfreqItem.collect();

        // compute association rules
        JavaRDD<AssociationRules.Rule<Property>> rules = model.generateAssociationRules(0).toJavaRDD();
        System.out.println("Number of mined rules: " + rules.count());

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

        rddResult.sortBy(i -> i.getConfidence(), false, 1)
                .take(40)
                .forEach(i -> System.out.println(i));

    }
}
