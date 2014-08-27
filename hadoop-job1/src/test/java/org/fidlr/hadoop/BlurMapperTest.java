package org.fidlr.hadoop;

import junit.framework.TestCase;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.*;

@RunWith(MockitoJUnitRunner.class)
public class BlurMapperTest extends TestCase {
    @Mock
    private Mapper<Text, BytesWritable, Text, BytesWritable>.Context context;
    private BlurMapper mapper;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mapper = new BlurMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String testFilename = new String("/home/omer/Downloads/nk/www.eng.tau.ac.il/~nk/computer-vision/ball/ball/MVC-001F.JPG");
        String outFilename = new String("/tmp/pics/blurTest.jpg");

        FileInputStream in = new FileInputStream(testFilename);
        DataInputStream buf = new DataInputStream(in);
        byte[] data = new byte[in.available()];
        buf.readFully(data);
        in.close();

        FileOutputStream out = new FileOutputStream(outFilename);
        DataOutputStream outStream = new DataOutputStream(out);

        mapper.setup(context);
        mapper.map(new Text(testFilename),
                   new BytesWritable(data), context);

        outStream.write(context.getCurrentValue().getBytes());
        out.close();
//        verify(context, times(1)).write(new Text("foo"), new IntWritable(1));
//        verify(context, times(2)).write(new Text("bar"), new IntWritable(1));
//
//        verifyNoMoreInteractions(context);
    }
}