package it.unipd.dei.dm1617.death_mining;

import scala.Serializable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by tonca on 06/05/17.
 *
 * This class is meant to load lookup tables and use
 * them to convert the values from the main table DeathRecords
 *
 */


public class FieldDecoder implements Serializable {

    private List<String> columns;
    private Map<Integer, Map<String, String>> colMap;

    // When an instance is created all lookup tables are loaded
    public FieldDecoder() {

        colMap = new HashMap<>();

        // Here we try to load column names
        try {
            String[] colNames = Files.lines(Paths.get("data/columns.csv"), StandardCharsets.UTF_8)
                    .toArray()[0]
                    .toString()
                    .split(",");

            columns = new ArrayList<>(Arrays.asList(colNames));
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // For each column we try to load the relative lookup table
        // If it doesn't exist the exception is caught without doing anything
        for (int i=0; i<columns.size();i++) {
            try {

                HashMap<String, String> map = new HashMap<>();

                Files.lines(Paths.get("data/" + columns.get(i) + ".csv"), StandardCharsets.UTF_8)
                        .forEach(
                                (line) -> {
                                    String[] row = line.split(",");
                                    map.put(row[0], row[1].replace("\"", ""));
                                }
                        );
                colMap.put(i, map);
            }
            catch (IOException e) {
                System.err.println(e);
                //e.printStackTrace();
            }
        }
    }

    public String decodeColumn(int colIndex) {
        return columns.get(colIndex);
    }

    // If the table is not in the map, then the value
    // is returned back given

    public String decodeValue(int colIndex, String key) {
        String result;
        try {
            result = colMap.get(colIndex).get(key);
        }
        catch (NullPointerException e) {
            result = key;
        }
        return result;
    }

}
