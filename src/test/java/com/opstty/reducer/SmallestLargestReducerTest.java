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
public class SmallestLargestReducerTest {
    @Mock
    private Reducer.Context context;
    private SmallestLargestReducer smallestLargestReducer;

    @Before
    public void setup() {
        this.smallestLargestReducer = new SmallestLargestReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String value = "value";
        Iterable<Text> values = Arrays.asList(new Text("value"));
        this.smallestLargestReducer.reduce(new DoubleWritable(45), values, this.context);
        verify(this.context).write(new DoubleWritable(45), new Text(value));
    }
}
