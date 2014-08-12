package org.fidlr.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class PrintKeysMapperTest {
    @Mock 
    private Mapper<Text, BytesWritable, Text, IntWritable>.Context context;
    private PrintKeysMapper mapper;
    
    @Before
    public void setUp() {
        mapper = new PrintKeysMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        mapper.map(new Text("foo"),
                   new BytesWritable(new byte[] {3,5,32,3}),
                   context);
        mapper.map(new Text("bar"),
                new BytesWritable(new byte[] {3,5,32,3}),
                context);
        mapper.map(new Text("bar"),
                new BytesWritable(new byte[] {3,5,32,3}),
                context);

        verify(context, times(1)).write(new Text("foo"), new IntWritable(1));
        verify(context, times(2)).write(new Text("bar"), new IntWritable(1));
        
        verifyNoMoreInteractions(context);
    }
}
