package com.opstty.reducer;

import com.opstty.writable.DistrictAgeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<NullWritable, DistrictAgeWritable, NullWritable, DistrictAgeWritable> {
    private final DistrictAgeWritable result = new DistrictAgeWritable(55, 99999);

    public void reduce(NullWritable key, Iterable<DistrictAgeWritable> values, Context context)
            throws IOException,InterruptedException{
       for (DistrictAgeWritable val: values) {
           /*if(val.getValue() == 0)
               continue;*/
           if (val.getValue() < this.result.getValue()) {
               this.result.setDistrict(val.getDistrict());
               this.result.setValue(val.getValue());
           }
        }

        context.write(NullWritable.get(), result);
    }
}
