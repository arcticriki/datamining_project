package it.unipd.dei.dm1617.death_mining;


import scala.Tuple2;
import java.util.*;

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

    //Private class used to create the array of useless items.
    //It represents a container of all the values to be rejected with respect to the given column.
    //In particular, the variable 'reference' represents the column name, while the array 'values'
    // contains all the useless values.
    private static class Couple {
        private String[] values = null;
        private String reference;

        //Constructor
        public Couple (String s, String[] array) {
            this.values = new String[array.length];
            this.reference = s;
            for (int i=0; i<array.length; i++) {
                this.values[i] = array[i];
            }
        }

        //Simple methods in order to get respectively the reference and the values of a couple
        public String getReference() {return this.reference;}

        public String[] getValues() {return this.values;}
    }


    //This constant array stores all the useless columns. It is used in preprocessing in order to
    //delete items (=property) with useless or not interesting columns.
    private static final String[] uselessColumns = {"Age","AgeRecode12", "AgeRecode52", "AgeSubstitutionFlag", "AgeType",
            "BridgedRaceFlag", "CurrentDataYear", "EducationReportingFlag","Education1989Revision", "HispanicOrigin", "HispanicOriginRaceRecode",
            "Id", "InfantAgeRecode22", "InfantCauseRecode130", "RaceImputationFlag", "RaceRecode5", "RaceRecode3"};


    //This constant array stores all the couples used for preprocessing. A couple is inserted for each column
    //that presents either useless/not interesting items' values or too much frequent values.
    //In particular, these last ones produce dirty outputs both in frequent itemsets and in association rules
    //computation, because they are repeated in several itemsets.
    private static final Couple[] uselessItems = {new Couple("ActivityCode", new String[] {"99", "Not applicable"}),
            new Couple("Autopsy", new String[] {"n", "N", "U"}),
            new Couple("DayOfTheWeekOfDeath", new String[] {"Unknown"}),
            new Couple("Education1989Revision", new String[] {"No formal education"}),
            new Couple("InjuryAtWork", new String[] {"U"}),
            new Couple("MannerOfDeath", new String[] {"Natural", "Not specified"}),
            new Couple("MaritalStatus", new String[] {"Marital Status unknown"}),
            new Couple("PlaceOfDeathAndDecedentsStatus", new String[] {"Place of death unknown"}),
            new Couple("PlaceOfInjury", new String[] {"Unspecified place", "Other Specifided Places", "Causes other than W00-Y34"}),
            new Couple("Race", new String[] {"White"}),
            new Couple("ResidentStatus", new String[] {"RESIDENTS"})};


    //Principal method used for preprocessing. Given an item (=property), it is declared as
    //rejectable (=true output) by evaluating its attributes (=column and value) which are compared with
    //the values stored in 'uselessColumns' and 'uselessItems'.
    public static boolean rejectUseless(Property prop) {
        String column = prop.getColName();
        String value = prop.getClassName();

        for (String currentUselessColumn: uselessColumns) {
            if (currentUselessColumn.equals(column)) return true;
        }

        for (Couple c: uselessItems) {
            String currentUselessColumn = c.getReference();
            String[] currentUselessValues = c.getValues();

            if (currentUselessColumn.equals(column)) {
                for (String currentUselessValue : currentUselessValues) {
                    if (currentUselessValue.equals(value)) return true;
                }
                break;
            }
        }

        return false;
    }


    //Auxiliar method used to perform additional filtering. It is particularly useful in case of a
    //computation of frequent itemsets and association rules which is limited to few specific columns.
    //This can be useful, for example, in order to compute fancy association rules like the one that involves Education and Autopsy.
    public static boolean rejectAdditionalColumns (Property prop, List<String> additionalUselessColumns) {

        //Remove Properties with useless items
        if (PropertyFilters.rejectUseless(prop)) return true;

        String colName = prop.getColName();

        for (String column: additionalUselessColumns) {
            if (column.equals(colName)) return true;
        }

        return false;
    }


    //Auxiliar method used to perform additional filtering. It is particularly useful in case of a
    //computation of frequent itemsets and association rules which is focuses on few specific values of a columns.
    //This can be useful, for example, if we want to compute the association rules that involve only few specific classes of deseases.
    public static boolean rejectAdditionalValues (Property prop, List<Tuple2<String, List<String>>> additionalUselessValues) {

        //Remove Properties with useless items
        if (PropertyFilters.rejectUseless(prop)) return true;

        String colName = prop.getColName();
        String colValue = prop.getClassName();

        for (Tuple2<String, List<String>> tuple: additionalUselessValues) {
            if (tuple._1().equals(colName)) {
                for (String uselessValue : tuple._2()) {
                    if (uselessValue.equals(colValue)) return true;
                }
                break;
            }
        }

        return false;
    }


    //This constant array stores all the columns that need a binning process
    private static final String[] binningColumns = {"AgeRecode27", "CauseRecode39"};

    private static final NavigableMap<Integer, String> AGE_MAP;
    static
    {
        NavigableMap<Integer, String> map = new TreeMap();
        map.put(1, "Baby");     // 1..2 => Baby
        map.put(3, "Child");    // 3..8 => Child
        map.put(9, "Teenager"); // 9 => Teenager
        map.put(10, "Young");   // 10..12 => Young
        map.put(13, "Adult");   // 13..19 => Adult
        map.put(20, "Old");     // 20..26 => Old
        map.put(27, "Not Specified");// 27 => Not Specified
        AGE_MAP = Collections.unmodifiableNavigableMap(map);
    }

    private static final NavigableMap<Integer, String> CAUSE_MAP;
    static
    {
        NavigableMap<Integer, String> map = new TreeMap();
        map.put(0, "ciao");    // 0..4     => 0
        map.put(5, "ciao");    // 5..10    => 1
        map.put(11, "dioboe");   // 11..200  => 2
        CAUSE_MAP = Collections.unmodifiableNavigableMap(map);
    }

    private static final NavigableMap<Integer, String> EDUREV_MAP;
    static
    {
        NavigableMap<Integer, String> map = new TreeMap();
        map.put(0, "ciao");    // 0..4     => 0
        map.put(5, "ciao");    // 5..10    => 1
        map.put(11, "dioboe");   // 11..200  => 2
        EDUREV_MAP = Collections.unmodifiableNavigableMap(map);
    }

    //Auxiliar method used for the binning. The information along some categories can be grouped into some
    //binned categories, for a better understanding of the results. In particular, the CauseRecoded39 has been
    //reduced to 20 possible death categories
    public static Property binningProperties(Property prop)
    {

        String colName = prop.getColName();
        String valIndex = prop.getClassIndex();

        String newCol = "BINNING ERROR!";
        String newVal = "BINNING ERROR!";
        if (colName.equals(binningColumns[0])) //Binning the age
        {

            int code = Integer.parseInt(valIndex);
            newCol = "Binned Age";
            newVal = AGE_MAP.floorEntry(code).getValue();

        }
        else if (colName.equals(binningColumns[1])) {
            int code = Integer.parseInt(valIndex);
            newCol = "Binned Age";
            newVal = CAUSE_MAP.floorEntry(code).getValue();

            /*if(code == 28)
            {Property binnedValue = new Property("DeathCategory","Respiratory diseases");
                return binnedValue;}
            else if (1 <= code <= 3)
            {Property binnedValue = new Property("DeathCategory","Infections (Sifilidis, HIV, Tubercolosis included");
                return binnedValue;}
            else if (5 <= code <= 12 || code == 15)
            {Property binnedValue = new Property("DeathCategory","Malignant Neoplasm in some organ");
                return binnedValue;}
            else if (code == 13)
            {Property binnedValue = new Property("DeathCategory","Cancer");
                return binnedValue;}
            else if(code == 14)
            {Property binnedValue = new Property("DeathCategory","Leukemia");
                return binnedValue;}
            else if(code == 16)
            {Property binnedValue = new Property("DeathCategory","Diabetis");
                return binnedValue;}
            else if(code == 17)
            {Property binnedValue = new Property("DeathCategory","Alzahimer");
                return binnedValue;}
            else if(20 <= code <= 22)
            {Property binnedValue = new Property("DeathCategory","Heart disease");
                return binnedValue;}
            else if (code==23)
            {Property binnedValue = new Property("DeathCategory","Endocrinal disease");
                return binnedValue;}
            else if(24 <= code <= 26)
            {Property binnedValue = new Property("DeathCategory","Circolatory system");
                return binnedValue;}
            else if(code == 27)
            {Property binnedValue = new Property("DeathCategory","Influenza and pneumonia");
                return binnedValue;}
            else if(code == 37 || code == 38)
            {Property binnedValue = new Property("DeathCategory","Accident");
                return binnedValue;}
            else if (code == 39)
            {Property binnedValue = new Property("DeathCategory","Suicide");
                return binnedValue;}
            else if (code == 40)
            {Property binnedValue = new Property("DeathCategory","Homicide");
                return binnedValue;}
            else if (code == 35 || code == 41)
            {Property binnedValue = new Property("DeathCategory","All other causes");
                return binnedValue;}
            else if (code == 36)
            {Property binnedValue = new Property("DeathCategory","Other Diseases");
                return binnedValue;}
            else if (code == 29)
            {Property binnedValue = new Property("DeathCategory","Gastroenterology disease (Peptic ulcer included)");
                return binnedValue;}
            else if (code == 30 || code == 31)
            {Property binnedValue = new Property("DeathCategory","Nephrology disease");
                return binnedValue;}
            else if (code == 32 || code == 34)
            {Property binnedValue = new Property("DeathCategory","Complicancies occured in perinatal period or Sudden Infant Death Syndrome");
                return binnedValue;}
            else (code == 33)
            {Property binnedValue = new Property("DeathCategory","Congenital malformations, deformations and chromosomal abnormalities");
             return binnedValue;}*/
        }
        else if (colName.equals(binningColumns[1])) {
            int code = Integer.parseInt(valIndex);
            newCol = "Binned Age";
            newVal = CAUSE_MAP.floorEntry(code).getValue();
        }

        return new Property(newCol, newVal);
    }


}
