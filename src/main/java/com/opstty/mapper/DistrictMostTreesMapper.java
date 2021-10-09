package com.opstty.mapper;

import com.opstty.writable.DistrictCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DistrictMostTreesMapper extends Mapper<Object, Text, NullWritable, DistrictCountWritable> {
    private final DistrictCountWritable dcw = new DistrictCountWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] pairs = value.toString().split("\t");

        try {
            dcw.setDistrict(Integer.parseInt(pairs[0]));
            dcw.setValue(Integer.parseInt(pairs[1]));
            context.write(NullWritable.get(), dcw);
        } catch (Exception e)
        {
            context.write(NullWritable.get(), dcw);
        }

    }
}
