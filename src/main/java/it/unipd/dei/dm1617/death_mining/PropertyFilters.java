package it.unipd.dei.dm1617.death_mining;


import scala.Tuple2;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Created by tonca on 07/05/17.
 *
 * This is a static class which contains static functions
 * for filtering properties during preprocessing
 *
 */

public final class PropertyFilters {

    private PropertyFilters() {
    }

    private static final String[] uselessColumns = {"AgeRecode27", "AgeRecode52", "AgeSubstitutionFlag", "AgeType",
            "BridgedRaceFlag", "CurrentDataYear", "EducationReportingFlag", "HispnaicOrigin", "HispanicOriginRaceRecode",
            "Id", "InfantAgeRecode22", "InfantCauseRecode130", "RaceImputationFlag", "RaceRecode5", "RaceRecode3"};

    private static final Map<String, String[]> uselessItems = new Hashtable<String, String[]>() {{
      put("ActivityCode", new String[] {"99", "Not applicable"} );
      put("Autopsy", new String[] {"N", "n", "U"});
      put("DayOfTheWeekOfDeath", new String[] {"Unknown"});
      put("Education1989", new String[] {});
      put("", new String[] {});
    }};


    /*
    This function tells if a property has to be rejected.
    It removes Property with useless values and too much frequent items.
    Better implementation needed (especially regarding frequent items). Following Tonca's suggestions ;)
    */

    public static boolean reject(Property prop) {

        boolean reject = false;

        switch (prop.getColName()) {

            //Not applicable value brings no information 0.9243531
            case "ActivityCode":
                reject = prop.getClassName().equals("99") ||
                         prop.getClassName().equals("Not applicable");
                break;

            case "AgeRecode27":
                reject = true;
                break;

            case "AgeRecode52":
                reject = true;
                break;

            case "AgeSubstitutionFlag":
                reject = true;
                break;

            case "AgeType":
                reject = true;
                break;

            //No autopsy (N/n) value too much frequent 0.8584129
            //No information from unknown (U) values
            case "Autopsy":
                reject = prop.getClassName().equalsIgnoreCase("n") ||
                         prop.getClassName().equals("U");
                break;

            case "BridgedRaceFlag":
                reject = true;
                break;

            case "CurrentDataYear":
                reject = true;
                break;

            //No useful information from unknown values
            case "DayOfTheWeekOfDeath":
                reject = prop
                         .getClassName()
                         .equals("Unknown");
                break;

            //No formal education value too much frequent 0.9093273
            case "Education1989Revision":
                reject = prop
                         .getClassName()
                         .equals("No formal education");
                break;

            case "EducationReportingFlag":
                reject = true;
                break;

            case "HispanicOrigin":
                reject = true;
                break;

            case "HispanicOriginRaceRecode":
                reject = true;
                break;

            case "Id":
                reject = true;
                break;

            case "InfantAgeRecode22":
                reject = true;
                break;

            case "InfantCauseRecode130":
                reject = true;
                break;

            //No useful information from unknown values 0.92023873
            case "InjuryAtWork":
                reject = prop
                        .getClassName()
                        .equals("U");
                break;

            //Natural manner of death too much frequent 0.78309494
            //No useful information from not specified or pending investigation values
            case "MannerOfDeath":
                reject = prop.getClassName().equals("Natural") ||
                         prop.getClassName().equals("Not specified")  ||
                         prop.getClassName().equals("Pending investigation");
                break;

            //No useful information from unknown marital status
            case "MaritalStatus":
                reject = prop
                     .getClassName()
                     .equals("Marital Status unknown");
                break;

            //No useful information from unknown values
            case "PlaceOfDeathAndDecedentsStatus":
                reject = prop
                         .getClassName()
                         .equals("Place of death unknown");
                break;

            //No useful information from unspecified values
            //Causes other than W00-Y34 value too much frequent 0.9259508
            case "PlaceOfInjury":
                reject = prop.getClassName().equals("Unspecified place") ||
                         prop.getClassName().equals("Other Specified Places") ||
                         prop.getClassName().equals("Causes other than W00-Y34");
                break;

            //White race too much frequent 0.8520658
            case "Race":
                reject = prop
                         .getClassName()
                         .equals("White");
                break;

            case "RaceImputationFlag":
                reject = true;
                break;

            case "RaceRecode5":
                reject = true;
                break;

            case "RaceRecode3":
                reject = true;
                break;

            //Resident value too much frequent 0.793026
            case "ResidentStatus":
                reject = prop
                         .getClassName()
                         .equals("RESIDENTS");
                break;
        }

        return reject;
    }

    public static boolean rejectByColumn (Property prop, List<String> additionalUselessColumns) {

        String colName = prop.getColName();
        List<String> totalUselessColumns = new ArrayList<>();

        for (String uselessColumn: uselessColumns) {
            totalUselessColumns.add(uselessColumn);
        }

        for (String uselessColumn: additionalUselessColumns) {
            totalUselessColumns.add(uselessColumn);
        }

        //Remove Properties with useless items
        if (PropertyFilters.reject(prop)) return true;

        for (String column: totalUselessColumns) {
            if (colName.equals(column)) return true;
        }

        return false;
    }
    
    public static boolean rejectByValue (Property prop, List<Tuple2<String, List<String>>> uselessValues) {
        String colName = prop.getColName();
        String colValue = prop.getClassName();

        if (PropertyFilters.reject(prop)) return true;

        for (Tuple2<String, List<String>> tuple: uselessValues) {
            if (colName.equals(tuple._1()))
                for (String value: tuple._2) {
                    if (colValue.equals(value)) return true;
                }
        }

        return false;
    }

}
