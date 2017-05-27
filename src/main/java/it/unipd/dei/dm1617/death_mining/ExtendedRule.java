package it.unipd.dei.dm1617.death_mining;

import org.apache.spark.mllib.fpm.AssociationRules;
import scala.Serializable;

/**
 * Created by gianluca on 25/05/17.
 */
public class ExtendedRule implements Serializable{


    public double getLift() {
        return lift;
    }

    public double getConviction() {
        return conviction;
    }

    public double getConfidence(){
        return rule.confidence();
    }

    public double getSupport() {
        return support;
    }

    private double lift;
    private double conviction;
    private double support;
    private AssociationRules.Rule rule;


    public ExtendedRule(AssociationRules.Rule rule, double support, double lift, double conviction) {
        this.conviction = Double.isFinite(conviction) ? 1e9 : conviction;
        this.lift = lift;
        this.rule = rule;
        this.support = support;
    }

    public String CSVformat(){
        return this.rule.javaAntecedent().toString() + " => " + this.rule.javaConsequent().toString() +
                ";" + this.getSupport() + ";" + this.getConfidence() + ";" + this.getLift() + ";" + this.getConviction();
    }

    public String toString(){
        return this.rule.javaAntecedent().toString() + " => " + this.rule.javaConsequent().toString() +
                " - Support: " + this.getSupport() + " Confidence: " + this.getConfidence() + " Lift: "
                + this.getLift() + " Conviction: " + this.getConviction();
    }
}
