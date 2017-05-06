package it.unipd.dei.dm1617.death_mining;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by tonca on 06/05/17.
 */
public class FieldDecoder {

    List<String> columns;
    List<Map<String, String>> colMap;

    //When an instance is created all lookup tables are loaded
    public FieldDecoder() {

        colMap = new ArrayList<>();

        try {
            String[] colNames = Files.lines(Paths.get("data/columns.csv"), StandardCharsets.UTF_8)
                    .toArray()[0]
                    .toString()
                    .split(",");

            columns = new ArrayList<String>(Arrays.asList(colNames));

            for (String temp : columns) {

                Map<String,String> map = new HashMap<>();

                Files.lines(Paths.get("data/"+temp+".csv"), StandardCharsets.UTF_8)
                        .forEach(
                                (line) -> {
                                    String[] row = line.split(",");
                                    map.put(row[0],row[1]);
                                }
                        );
                colMap.add(map);
            }
        }

        catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String decodeColumn(int colIndex) {

        return columns.get(colIndex);
    }

    public String decodeValue(int colIndex, String key) {

        return colMap.get(colIndex).get(key);
    }

}


/*
for (FPGrowth.FreqItemset<Tuple2<Integer,String>> itemset: model.freqItemsets().toJavaRDD().collect()) {

    List<Tuple2<String,String>> converted = new ArrayList<>();
    itemset.javaItems().forEach(
            (tuple) -> {
                converted.add(new Tuple2<>(colNames[tuple._1], tuple._2));
            }
    );
    System.out.println("[" + converted + "], " + itemset.freq());
}
 */