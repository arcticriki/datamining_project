package it.unipd.dei.dm1617.death_mining;

import scala.Tuple2;

/**
 * Created by gianluca on 05/05/17.
 */
public class Property extends Tuple2<String,String>{

    private int id;

    public Property(int id, String key, String value){
        super(key, value);
        this.id = id;
    }

    public String toString(){
        //return "(" + this.id + ", " + this._2 + ")";
        return format();
    }

    public String format(){
        return "(" + this._1 + ", " + this._2 + ")";
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName(){
        return this._1;
    }

    public String getValue(){
        return this._2;
    }
}
