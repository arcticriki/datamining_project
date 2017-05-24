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

public class PropertyFilters {

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
    //delete items (=property) with useless or not interesting columns. This array has been built thanks to the
    //a-priori knowledge fo the dataset.
    private static final String[] uselessColumns = {
            "Id",
            "Education1989Revision",
            "Education2003Revision",
            "EducationReportingFlag",
            "AgeType",
            "Age",
            "AgeSubstitutionFlag",
            "AgeRecode52",
            "AgeRecode27",
            "AgeRecode12",
            "InfantAgeRecode22",
            "CurrentDataYear",
            "Icd10Code",
            "CauseRecode358",
            "CauseRecode113",
            "InfantCauseRecode130",
            "CauseRecode39",
            "NumberOfEntityAxisConditions",
            "NumberOfRecordAxisConditions",
            "Race",
            "BridgedRaceFlag",
            "RaceImputationFlag",
            "RaceRecode5",
            "HispanicOrigin",
            "HispanicOriginRaceRecode"
    };


    //This constant array stores all the couples used for preprocessing. A couple is inserted for each column
    //that presents either useless or not interesting items' values. This array has been built thanks to the
    //a-priori knowledge fo the dataset.
    private static final Couple[] uselessItems = {
            new Couple("ActivityCode", new String[] {"99", "Not applicable", "During unspecified activity"}),
            new Couple("Autopsy", new String[] {"n", "U"}),
            new Couple("DayOfTheWeekOfDeath", new String[] {"Unknown"}),
            new Couple("InjuryAtWork", new String[] {"U"}),
            new Couple("MannerOfDeath", new String[] {"Not specified"}),
            new Couple("MaritalStatus", new String[] {"Marital Status unknown"}),
            new Couple("MethodOfDisposition", new String[] {"Other", "null", "Unknown"}),
            new Couple("PlaceOfDeathAndDecedentsStatus", new String[] {"Place of death unknown"}),
            new Couple("PlaceOfInjury", new String[] {"Unspecified place", "Other Specifided Places"})
    };


    //This constant array stores all the couples used as a second-step preprocessing. After a first computation with
    //rejectUseless() method, we obtained dirty outputs both in frequent itemsets and in association rules.
    //This array has been built with a careful observation of the output and thanks to the Analytics class.
    private static final Couple[] frequentItems = {new Couple("ActivityCode", new String[] {"Not applicable"}),
            new Couple("Autopsy", new String[] {"N"}),
            new Couple("Education1989Revision", new String[] {"No formal education"}),
            new Couple("MannerOfDeath", new String[] {"Natural"}),
            new Couple("PlaceOfInjury", new String[] {"Causes other than W00-Y34"}),
            //new Couple("Race", new String[] {"White"}),
            new Couple("RaceRecode3", new String[] {"White"}),
            //new Couple("InjuryAtWork", new String[] {"N"}),
            new Couple("ResidentStatus", new String[] {"RESIDENTS"})};


    //First method used for preprocessing. Given an item (=property), it is declared as
    //rejectable (=true output) by evaluating its attributes (=column and value) which are compared with
    //the values stored in 'uselessColumns' and 'uselessItems'. This method uses a-priori knowledge of
    //dataset's information.
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


    //Second-step method used for preprocessing. Given an item (=property), it is declared as
    //rejectable (=true output) by evaluating its attributes (=column and value) which are compared with
    //the values stored in 'uselessColumns', 'uselessItems' and 'frequentItems'. This method uses a-posteriori knowledge
    //of the output, which has been gained after a first computation based on rejectUseless() filtering.
    //In particular, it rejects some too-much-frequent items that produce dirty output.
    public static boolean rejectUselessAndFrequent(Property prop) {

        //reject useless values
        if(PropertyFilters.rejectUseless(prop)) return true;

        String column = prop.getColName();
        String value = prop.getClassName();

        for (Couple c: frequentItems) {
            String currentFrequentColumn = c.getReference();
            String[] currentFrequentValues = c.getValues();

            if (currentFrequentColumn.equals(column)) {
                for (String currentUselessValue : currentFrequentValues) {
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

        //Remove Properties with useless and frequent items
        if (PropertyFilters.rejectUselessAndFrequent(prop)) return true;

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

        //Remove Properties with useless and frequent items
        if (PropertyFilters.rejectUselessAndFrequent(prop)) return true;

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
    private static final String[] binningColumns = {
            "AgeRecode27",
            "CauseRecode39",
            "Education1989Revision",
            "Education2003Revision",
            "Autopsy"
    };

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
        map.put(1, "Infections caused (HIV, Tubercolosis and Sifilidis)");    // 1,2,3
        map.put(5, "Malignant Neoplasm in some organ");    // (4 doesn't exist) 5..12
        map.put(13, "Cancer");
        map.put(14, "Leukemia");
        map.put(15, "Malignant Neoplasm in some organ");
        map.put(16, "Diabetis");
        map.put(17, "Alzheimer");
        map.put(20, "Heart disease"); //(18,19 don't exist) 20,21,22
        map.put(23, "Endocrinal disease");
        map.put(24, "Circoulatory system disease"); //24,25,26
        map.put(27,"Influenza and pneuomonia");
        map.put(28, "Breathing system disease");
        map.put(29, "Gastroenterology disease (Peptic ulcer)");
        map.put(30, "Nephrology disease"); //30,31
        map.put(32, "Complicancies occured in perinatal period or Sudden Infant Death Syndrome");
        map.put(33, "Congenital malformations, deformations and chromosomal abnormalities");
        map.put(34, "Complicancies occured in perinatal period or Sudden Infant Death Syndrome");
        map.put(35, "Other causes, different from diseases");
        map.put(36, "Other diseases");
        map.put(37,"Accident"); //37,38
        map.put(39, "Suicide");
        map.put(40,"Homicide");
        map.put(41,"Other causes, different from diseases");

        CAUSE_MAP = Collections.unmodifiableNavigableMap(map);
    }

    private static final NavigableMap<Integer, String> EDUREV_MAP; //-> to be completed
    static
    {
        NavigableMap<Integer, String> map = new TreeMap();
        map.put(0, "8th grade or less");    // 0..8
        map.put(9, "9 - 12th grade, no diploma");    // 9..11
        map.put(13, "high school graduate or GED completed");
        map.put(14, "some college credit"); //14..16

        EDUREV_MAP = Collections.unmodifiableNavigableMap(map);
    }

    //Auxiliar method used for the binning. The information along some categories can be grouped into some
    //binned categories, for a better understanding of the results. In particular, the CauseRecoded39 has been
    //reduced to 20 possible death categories
     public static Property binningProperties(Property prop)
    {

        String colName = prop.getColName();
        String className = prop.getClassName();
        String valIndex = prop.getClassIndex();

        String newCol = colName;
        String newVal = className;
        if (colName.equals(binningColumns[0])) //Binning the age
        {

            int code = Integer.parseInt(valIndex);
            newCol = "Binned Age";
            newVal = AGE_MAP.floorEntry(code).getValue();

        }
        else if (colName.equals(binningColumns[1])) {
            int code = Integer.parseInt(valIndex);
            newCol = "Death Category";
            newVal = CAUSE_MAP.floorEntry(code).getValue();
        }
        else if (colName.equals(binningColumns[2])) {
            int code = Integer.parseInt(valIndex);
            newCol = "Education";                           //How to call it? Should it be exactly: "Education2003Revision"?
            newVal = EDUREV_MAP.floorEntry(code).getValue();
        }
        else if (colName.equals(binningColumns[3])) {
            newCol = "Education";
            newVal = className;
        }
        else if (colName.equals(binningColumns[4]) && className.equals("y")) {
            newCol = colName;
            newVal = "Y";
        }

        return new Property(newCol, newVal);
    }


}
