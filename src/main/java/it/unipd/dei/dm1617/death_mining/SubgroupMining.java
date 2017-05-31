package it.unipd.dei.dm1617.death_mining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by tonca on 26/05/17.
 *
 * This class lets us analyze single subgroups in the dataset depending on a specified values
 * (e.g Male, Female, Old, Black, Suicides and so on...)
 *
 * The subgroups are specified in a map in the following code (subgroups)
 */
public class SubgroupMining {

    private static final Map<String, List<String>> subgroups;
    // It can take some time to compute a lot of subgroups together,
    // Comment some of the lines below for a shorter analysis
    static {
        Map<String, List<String>> tmp = new HashMap<>();
        //tmp.put("Sex", Arrays.asList("M", "F"));
        //tmp.put("Binned Age", Arrays.asList("Baby", "Child", "Teenager", "Adult", "Old"));
        tmp.put("RaceRecode3", Arrays.asList("White", "Black", "Races other than White or Black"));
        //tmp.put("Death Category", Arrays.asList("Homicide", "Suicide", "Accident"));
        subgroups = Collections.unmodifiableMap(tmp);
    }

    public static void singleGroupMining(JavaSparkContext sc,
                                         JavaRDD<List<Property>> transactions,
                                         String colSelection,
                                         String valSelection,
                                         double minSup,
                                         double maxFreq
    ) {

        System.out.println(String.format("[filtering frequent] %s : %s","maxFreq",maxFreq));

        // Removing too frequent items and selecting subgroup
        transactions = transactions.filter( itemset -> {
                boolean contains = false;
                for (Property item : itemset) {
                    if (item._1().equals(colSelection) && item._2().equals(valSelection))
                        contains = true;
                }
                return Boolean.valueOf(contains);
            })
            .map(transaction -> {
                Property reject = null;
                for (Property item : transaction) {
                    if (item._1().equals(colSelection))
                        reject = item;
                }
                transaction.remove(reject);
                return transaction;
            });



        // mine frequent itemsets and association rules
        DeathMiner dm = new DeathMiner(sc, transactions);
        List<Property> topFrequent = dm.filterFrequentItemsets(maxFreq);

        JavaPairRDD<List<Property>, Double> rddFreqItemAndSupport = dm.mineFrequentItemsets(minSup);
        JavaRDD<ExtendedRule> rddResult = dm.mineAssociationRules();

        // save results in dedicated folder structure
        String subgroupdir = "results/"+colSelection+":"+valSelection+"/";
        String outputdir = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
        System.out.println("[saving results] Ouput path: " + outputdir);

        DeathSaver.saveLog(subgroupdir+outputdir+"/log", 1, minSup, maxFreq, topFrequent.toString());
        DeathSaver.saveItemsets(subgroupdir+outputdir+"/freq-itemsets", rddFreqItemAndSupport);
        DeathSaver.saveRules(subgroupdir+outputdir+"/rules", rddResult);

    }

    public static void main(String[] args) {

        double sampleProbability = 1;
        double minSup = 0.001;
        double maxFreq = 0.01;

        SparkConf sparkConf = new SparkConf(true).setAppName("Death Mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";

        // import data
        System.out.println("[read dataset] Sampling with probability " + sampleProbability + " and importing data");
        JavaRDD<List<Property>> transactions = Preprocessing.dataImport(sc, filename, sampleProbability);

        for(Map.Entry<String, List<String>> entry : subgroups.entrySet())
        {
            for(String val : entry.getValue()) {

                long timeStart = System.currentTimeMillis();
                System.out.println(String.format("[subgroup selection] %s : %s",entry.getKey(),val));

                singleGroupMining(sc, transactions, entry.getKey(), val, minSup, maxFreq);

                long timeSubgroup = System.currentTimeMillis();
                System.out.println(String.format("[Subgroup selection] Elapsed time for %s : %s, %s s",
                        entry.getKey(),
                        val,
                        ((timeSubgroup-timeStart)/1000.0)));
            }
        }

    }
}

