package com.opstty.mapper;

import com.opstty.writable.DistrictAgeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, NullWritable, DistrictAgeWritable> {
    private final static DistrictAgeWritable da = new DistrictAgeWritable();
    private final NullWritable nw = NullWritable.get();
    private final String header = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Ignoring the header
        if(value.toString().equals(header))
            return;

        // Selecting the "ARONDISSEMENT" column (2th)
        String district = value.toString().split(";")[1];
        // Selecting the "ANNEE PLANTATION" column (6th)
        String annee_plantation = value.toString().split(";")[5];

        try {
            da.setDistrict(Integer.parseInt(district));
            da.setValue(Integer.parseInt(annee_plantation));
            context.write(nw, da);
        } catch (Exception e) {
            da.setValue(9999);
            context.write(nw, da);
        }

    }
}
