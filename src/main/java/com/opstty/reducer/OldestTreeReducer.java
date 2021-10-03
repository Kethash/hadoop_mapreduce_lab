package com.opstty.reducer;

import com.opstty.writable.DistrictAgeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<Text, DistrictAgeWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<DistrictAgeWritable> values, Context context)
            throws IOException,InterruptedException{
        int sum=0;
        for(DistrictAgeWritable val: values){
            // Ignoring null values
            if (val == null)
                continue;
            sum+= val.getValue();
        }
        result.set(sum);
        context.write(key,result);
    }
}
