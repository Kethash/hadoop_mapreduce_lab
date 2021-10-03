package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class SmallestLargestReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
    private final Text result = new Text();

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


        for (Text val : values) {
            // Ignoring null values
            if (val != null) {
                result.set(val);
                context.write(key,val);
            }
        }

    }
}
