package it.unipd.dei.dm1617.death_mining;

/**
 * Created by tonca on 07/05/17.
 *
 * This is a static class which contains static functions
 * for filtering properties during preprocessing
 *
 */
public final class PropertyFilters {

    private PropertyFilters() {}

    // this function tells if a property has to be rejected
    public static boolean reject(Property prop) {

        boolean reject = false;

        switch (prop.getColName()){

            case "ActivityCode":
                reject = prop
                        .getClassName()
                        .equals("99");
                break;

            case "InjuryAtWork":
                reject = prop
                        .getClassName()
                        .equals("U");
                break;

            case "RaceImputationFlag":
                reject = true;
                break;

            case "AgeType":
                reject = true;
                break;

            case "AgeRecode27":
                reject = true;
                break;

            case "AgeRecode52":
                reject = true;
                break;

            case "BridgedRaceFlag":
                reject = true;
                break;

            case "AgeSubstitutionFlag":
                reject = true;
                break;

            case "InfantAgeRecode22":
                reject = true;
                break;

            case "InfantCauseRecode130":
                reject = true;
                break;

            case "EducationReportingFlag":
                reject = true;
                break;

            case "HispanicOriginRaceRecode":
                reject = true;
                break;

            case "HispanicOrigin":
                reject = true;
                break;

            case "RaceRecode5":
                reject = true;
                break;

            case "RaceRecode3":
                reject = true;
                break;

            case "MannerOfDeath":
                reject = prop
                        .getClassName()
                        .equals("Natural");
                break;

            case "CurrentDataYear":
                reject = true;
                break;
        }

        return reject;
    }
}
