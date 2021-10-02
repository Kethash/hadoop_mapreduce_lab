package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TallestReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private final DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double heigh = 0;
        for (DoubleWritable val : values) {
            // Ignoring null values
            if (val == null) {
            }
            else if (val.get() > heigh)
                heigh = val.get();
        }
        result.set(heigh);
        context.write(key, result);
    }
}
