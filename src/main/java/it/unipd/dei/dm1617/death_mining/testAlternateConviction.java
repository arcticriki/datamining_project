package it.unipd.dei.dm1617.death_mining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by gianluca on 22/05/2017.
 *
 */
public class testAlternateConviction {
    public static void main(String[] args) {

        double sampleProbability = 0.1;
        double minSup = 0.001;
        double maxFreq = 1;

        SparkConf sparkConf = new SparkConf(true).setAppName("Death Mining");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/DeathRecords.csv";

        // import data
        System.out.println("[read dataset] Sampling with probability " + sampleProbability + " and importing data");
        JavaRDD<List<Property>> transactions = Preprocessing.dataImport(sc, filename, sampleProbability);

        // mine frequent itemsets and association rules
        DeathMiner dm = new DeathMiner(sc, transactions);
        List<Property> topFrequent = dm.filterFrequentItemsets(maxFreq);
        JavaPairRDD<List<Property>, Double> rddFreqItemAndSupport = dm.mineFrequentItemsets(minSup);
        JavaRDD<ExtendedRule> rddResult = dm.mineAssociationRules();

        // save results in dedicated folder structure
        String outputdir = new SimpleDateFormat("'results/'yyyyMMdd-HHmmss").format(new Date());
        System.out.println("[saving results] Ouput path: " + outputdir);

        DeathSaver.saveItemsets(outputdir+"/freq-itemsets", rddFreqItemAndSupport);
        DeathSaver.saveRules(outputdir+"/rules", rddResult);
        DeathSaver.saveLog(outputdir+"/log",sampleProbability, minSup,maxFreq,topFrequent.toString());
    }
}
