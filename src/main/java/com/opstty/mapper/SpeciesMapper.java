package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text word = new Text();
    private final static NullWritable nw = NullWritable.get();
    private String header = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Ignoring the header
        if(value.toString().equals(header))
            return;

        // Selecting the "GENRE" and "ESPECE" columns (3rd and 4th)
        String[] columns = value.toString().split(";");
        String str = columns[2]+" " + columns[3];

        try {
            word.set(str);
            context.write(word, nw);
        } catch (Exception e) {
            word.set(str);
            context.write(word, nw);
        }


    }
}
