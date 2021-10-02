package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TallestMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private final static DoubleWritable final_height = new DoubleWritable(0);
    private Text word = new Text();
    private String header = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        if(value.toString().equals(header))
            return;

        String[] columns = value.toString().split(";");
        String kind = columns[2];
        String height = columns[6];

        word.set(kind);

        try {
            final_height.set(Double.parseDouble(height));
            context.write(word, final_height);
        } catch(Exception e) {
            return;
        }
    }
}

