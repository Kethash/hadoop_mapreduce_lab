package com.opstty.reducer;

import com.opstty.writable.DistrictCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictMostTreesReducer extends Reducer<NullWritable, DistrictCountWritable, NullWritable, DistrictCountWritable> {
    private final DistrictCountWritable result = new DistrictCountWritable(0,0);

    public void reduce(NullWritable key, Iterable<DistrictCountWritable> values, Context context)
            throws IOException, InterruptedException {

        for (DistrictCountWritable val : values) {
            if (val.getValue() > result.getValue()) {
                result.setDistrict(val.getDistrict());
                result.setValue(val.getValue());
            }
        }
        context.write(NullWritable.get(), result);
    }
}
