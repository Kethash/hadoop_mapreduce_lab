package com.opstty.reducer;

import com.opstty.writable.DistrictAgeWritable;
import com.opstty.writable.DistrictCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DistrictMostTreesReducerTest {
    @Mock
    private Reducer.Context context;
    private DistrictMostTreesReducer districtMostTreesReducer;

    @Before
    public void setup() {
        this.districtMostTreesReducer = new DistrictMostTreesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        DistrictCountWritable value1 = new DistrictCountWritable(1,21);
        DistrictCountWritable value2 = new DistrictCountWritable(2,22);
        DistrictCountWritable value3 = new DistrictCountWritable(3,23);
        Iterable<DistrictCountWritable> values = Arrays.asList(value1, value2, value3);
        this.districtMostTreesReducer.reduce(NullWritable.get(), values, this.context);
        verify(this.context).write(NullWritable.get(), new DistrictCountWritable(3,23));
    }
}
