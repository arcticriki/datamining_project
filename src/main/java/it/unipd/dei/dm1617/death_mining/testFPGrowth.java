package it.unipd.dei.dm1617.death_mining;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.Tuple2;
import scala.Tuple3;
//import org.apache.spark.sql.api.java.JavaSQLContext;

/**
 * Created by tonca on 05/05/17.
 */
public class testFPGrowth {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        SparkConf sparkConf = new SparkConf(true).setAppName("Word count optimization");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        String filename = "data/data.csv";
        JavaRDD<List<Tuple2<Integer, String>>> transactions = sc.textFile(filename).map(
            line -> {

                List<Tuple2<Integer, String>> transaction = new ArrayList<Tuple2<Integer, String>>();
                String[] fields = line.split(",");

                for (int i = 0; i < fields.length-10; i++) {
                    Tuple2<Integer, String> tuple = new Tuple2<>(i, fields[i]);
                    transaction.add(tuple);
                }
                return transaction;
            }
        );

        long end = System.currentTimeMillis();

        System.out.println("Elapsed time: "+ ((end-start)/1000.0) + " s" );

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.2)
                .setNumPartitions(10);
        FPGrowthModel<Tuple2<Integer,String>> model = fpg.run(transactions);

        //Qua comincia il casino




        //Qua finisce

        /*
        for (FPGrowth.FreqItemset<Tuple2<Integer,String>> itemset: model.freqItemsets().toJavaRDD().collect()) {
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }
        */
        end = System.currentTimeMillis();

        System.out.println("Elapsed time: "+ ((end-start)/1000.0) + " s" );
        /*double minConfidence = 0.8;
        for (AssociationRules.Rule<Tuple2<Integer,String>> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            System.out.println(
                    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        }*/




    }



}
