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
import scala.Serializable;
import scala.Tuple2;


import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * Created by Marco on 14/05/2017.
 *
 */
public class testJoin {
    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf(true).setAppName("Frequent itemsets mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";
        Broadcast<FieldDecoder> fd = sc.broadcast(new FieldDecoder());
        double sampleProbability = 0.3;
        double minSup = 0.05;

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
                                        fd.value().decodeValue(i,columnContent),
                                        columnContent
                                );

                                transaction.add(prop);
                            }

                            return PropertyFilters.itemsetFilter(transaction);
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


        JavaRDD<FPGrowth.FreqItemset<Property>> rddfreqItem = model.freqItemsets().toJavaRDD();

        // collect frequent itemsets and their support in a RDD of KeyValue pair, where the key is the itemset
        JavaPairRDD<List<Property>, Double> rddFreqItemAndSupport = rddfreqItem.mapToPair(item ->
                new Tuple2<>(item.javaItems(), item.freq()*1.0/transactionsCount));

        // save a local copy of frequent itemsets
        List<FPGrowth.FreqItemset<Property>> localFreqItemset = rddfreqItem.collect();

        // compute association rules
        JavaRDD<AssociationRules.Rule<Property>> rules = model.generateAssociationRules(0).toJavaRDD();

        // map association rules in a RDD of key-value pairs, where the key is the consequent of the rule and the value is the rule itself
        // the aim is to combine this RDD with the one previously computed to extract the support of the consequent itemset
        JavaPairRDD<List<Property>, AssociationRules.Rule<Property>> rddConsequents = rules
                .mapToPair(r -> new Tuple2<>(r.javaConsequent(), r));

        System.out.println("Total rule count " + rules.count());

        // create PairRDD consisting of Rule and its Lift
        JavaPairRDD<AssociationRules.Rule, Double> rddRuleLift = rddConsequents
                .join(rddFreqItemAndSupport)
                .mapToPair(item ->{
                    AssociationRules.Rule<Property> rule = item._2._1;
                    Double consequentSupport = item._2._2;
                    return new Tuple2<>(rule, rule.confidence()/consequentSupport);
                })
                .map(rule -> rule.swap())
                .sortBy(rule -> rule._1(), false, 1)
                .mapToPair(rule -> new Tuple2<>(rule._2(), rule._1()));

        // print rules and their lift
        rddRuleLift.foreach(r -> System.out.println(r._1 + " " + "Lift = " + r._2));
    }

}
