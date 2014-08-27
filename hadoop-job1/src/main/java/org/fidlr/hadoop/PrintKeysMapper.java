package org.fidlr.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Turns lines from a text file into (word, 1) tuples.
 */
public class PrintKeysMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);

    protected void map(Text filename, BytesWritable value, Context context)
            throws IOException, InterruptedException {

        context.write(/* File Contents */filename, one);
    }
}
