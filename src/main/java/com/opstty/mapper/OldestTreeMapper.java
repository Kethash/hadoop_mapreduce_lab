package com.opstty.mapper;

import com.opstty.writable.DistrictAgeWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, Text, Writable> {
    private final static DistrictAgeWritable da = new DistrictAgeWritable();
    private Text word = new Text();
    private NullWritable nw = NullWritable.get();
    private String header = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Ignoring the header
        if(value.toString().equals(header))
            return;

        // Selecting the "ADRESSE" column (8th)
        String espece = value.toString().split(";")[3];
        String adresse = value.toString().split(";")[8];
        String annee_plantation = value.toString().split(";")[5];

        try {
            word.set(espece);
            da.setAdress(new Text(adresse));
            da.setValue(Integer.parseInt(annee_plantation));
            context.write(word, da);
        } catch (Exception e) {
            word.set(espece);
            context.write(word, nw);
        }

    }
}
