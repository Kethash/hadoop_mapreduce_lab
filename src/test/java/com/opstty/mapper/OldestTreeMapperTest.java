package com.opstty.mapper;

import com.opstty.writable.DistrictAgeWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class OldestTreeMapperTest {
    @Mock
    private Mapper.Context context;
    private OldestTreeMapper oldestTreeMapper;

    @Before
    public void setup() {
        this.oldestTreeMapper = new OldestTreeMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "foo;2;tux;foo;bar;1984";
        this.oldestTreeMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(NullWritable.get(), new DistrictAgeWritable(2,1984));
    }
}
