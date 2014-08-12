package org.fidlr.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Turns lines from a text file into (word, 1) tuples.
 */
public class PrintKeysMapper extends Mapper<LongWritable, BytesWritable, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);

    protected void map(LongWritable line_index, BytesWritable value, Context context)
            throws IOException, InterruptedException {

        // TODO: Break up 'value' into (Filename, File Contents)

        context.write(/* File Contents */, one);
    }
}
