package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
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
public class TallestReducerTest {
    @Mock
    private Reducer.Context context;
    private TallestReducer tallestReducer;

    @Before
    public void setup() {
        this.tallestReducer = new TallestReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        DoubleWritable value = new DoubleWritable(2);
        DoubleWritable tallestvalue = new DoubleWritable(45);
        Iterable<DoubleWritable> values = Arrays.asList(value, tallestvalue, null);
        this.tallestReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new DoubleWritable(45));
    }
}
