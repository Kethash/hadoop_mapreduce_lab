package com.opstty.mapper;

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
public class SpeciesMapperTest {
    @Mock
    private Mapper.Context context;
    private SpeciesMapper speciesMapper;

    @Before
    public void setup() {
        this.speciesMapper = new SpeciesMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "foo;bar;tux;foo;bar;tux";
        this.speciesMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("tux foo"), NullWritable.get());
    }
}
