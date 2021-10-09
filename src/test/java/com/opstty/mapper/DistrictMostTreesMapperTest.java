package com.opstty.mapper;

import com.opstty.writable.DistrictCountWritable;
import org.apache.hadoop.io.IntWritable;
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
public class DistrictMostTreesMapperTest {
    @Mock
    private Mapper.Context context;
    private DistrictMostTreesMapper districtMostTreesMapper;

    @Before
    public void setup() {
        this.districtMostTreesMapper = new DistrictMostTreesMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        // Check if the 8th column is selected (wood)
        String value = "20\t36";
        this.districtMostTreesMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(NullWritable.get(), new DistrictCountWritable(20,36));
    }
}
