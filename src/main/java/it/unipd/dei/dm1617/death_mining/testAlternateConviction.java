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

        // import data
        System.out.println("[read dataset] Sampling with probability " + sampleProbability + " and importing data");
        JavaRDD<List<Property>> transactions = Preprocessing.dataImport(sc, filename, sampleProbability);

        // mine frequent itemsets and association rules
        DeathMiner dm = new DeathMiner(sc, transactions);
        JavaPairRDD<List<Property>, Double> rddFreqItemAndSupport = dm.mineFrequentItemsets(minSup);
        JavaRDD<ExtendedRule> rddResult = dm.mineAssociationRules();

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
