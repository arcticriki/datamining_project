package it.unipd.dei.dm1617.death_mining;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gianluca on 27/05/17.
 */
public class DeathSaver {
    public static void saveItemsets(String path, JavaPairRDD<List<Property>, Double> freqItemsetsSupport){


        try {
            Path outputFile = Paths.get(path);
            if (!Files.exists(outputFile.getParent()))
                Files.createDirectory(outputFile.getParent());

            List<String> lines = new ArrayList<>();
            lines.add("itemset;support");
            lines.addAll(
                    freqItemsetsSupport
                    .map(i -> i._1.toString() + ";" + i._2.toString())
                    .collect()
            );

            Files.write(outputFile, lines, Charset.forName("UTF-8"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void saveRules(String path, JavaRDD<ExtendedRule> rules){
        try {
            Path outputFile = Paths.get(path);
            if (!Files.exists(outputFile.getParent()))
                Files.createDirectory(outputFile.getParent());

            List<String> lines = new ArrayList<>();
            lines.add("rule;support;confidence;lift;conviction");
            lines.addAll(rules.map(ExtendedRule::CSVformat).collect());

            Files.write(outputFile, lines, Charset.forName("UTF-8"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void saveLog(String path, double minSup, double maxFreq) {
        try {
            Path outputFile = Paths.get(path);
            if (!Files.exists(outputFile.getParent()))
                Files.createDirectory(outputFile.getParent());

            List<String> lines = new ArrayList<>();
            lines.add(String.format("maxFreq: %s",maxFreq));
            lines.add(String.format("minSup: %s",minSup));

            Files.write(outputFile, lines, Charset.forName("UTF-8"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
