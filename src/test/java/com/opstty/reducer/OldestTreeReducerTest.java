package com.opstty.reducer;

import com.opstty.writable.DistrictAgeWritable;
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
public class OldestTreeReducerTest {
    @Mock
    private Reducer.Context context;
    private OldestTreeReducer oldestTreeReducer;

    @Before
    public void setup() {
        this.oldestTreeReducer = new OldestTreeReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        Iterable<DistrictAgeWritable> values = Arrays.asList(new DistrictAgeWritable(1, 2001), new DistrictAgeWritable(2,2002), new DistrictAgeWritable(3,2003));
        this.oldestTreeReducer.reduce(NullWritable.get(), values, this.context);
        verify(this.context).write(NullWritable.get(), new DistrictAgeWritable(1, 2001));
    }
}
