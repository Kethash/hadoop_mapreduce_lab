package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
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
public class TreeHeightMapperTest {
    @Mock
    private Mapper.Context context;
    private TreeHeightMapper treeHeightMapper;

    @Before
    public void setup() {
        this.treeHeightMapper = new TreeHeightMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "anemo;lux;geo;cryo;electro;pyro;45.0";
        this.treeHeightMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new DoubleWritable(45.0), new Text("geo"));
    }
}
