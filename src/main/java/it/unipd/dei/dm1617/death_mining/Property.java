package it.unipd.dei.dm1617.death_mining;

import scala.Tuple2;

/**
 * Created by gianluca on 05/05/17.
 */
public class Property extends Tuple2<String,String>{

    public Property(String colName, String className){
        super(colName, className);
    }

    public String toString(){
        return format();
    }

    public String format(){
        return "(" + this._1() + ", " + this._2() + ")";
    }

    public String getColName() {
        return this._1();
    }

    public String getClassName() { return this._2(); }

}
