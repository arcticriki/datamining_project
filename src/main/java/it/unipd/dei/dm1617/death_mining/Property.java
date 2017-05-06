package it.unipd.dei.dm1617.death_mining;

import scala.Tuple2;

/**
 * Created by gianluca on 05/05/17.
 */
public class Property extends Tuple2<Integer,String>{

    private String colName;
    private String className;

    public Property(int key, String value, String colName, String className){
        super(key, value);
        this.className = className;
        this.colName = colName;
    }

    public String toString(){
        return format();
    }

    public String format(){
        return "(" + this.colName + ", " + this.className + ")";
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getClassName() { return className; }

    public void setClassName(String className) { this.className = className; }

    public int getName(){
        return this._1;
    }

    public String getValue(){
        return this._2;
    }
}
