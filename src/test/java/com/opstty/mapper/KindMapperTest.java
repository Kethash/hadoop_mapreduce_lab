package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
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
public class KindMapperTest {
    @Mock
    private Mapper.Context context;
    private KindMapper kindMapper;

    @Before
    public void setup() {
        this.kindMapper = new KindMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "pyro;anemo;hydro;electro;cryo";
        this.kindMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("hydro"), new IntWritable(1));
    }
}
