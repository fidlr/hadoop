// To run copy to the cluster and execute:
// hadoop jar hadoop-job1.jar org.fidlr.hadoop.WordCount /falcon/demo/bcp/processed/enron/2014-02-28-00 /user/hue/wordcount


package org.fidlr.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * The famous MapReduce word count example for Hadoop.
 */
public class ImageJob extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ImageJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: hadoop jar hadoop-job1-1.0-SNAPSHOT-job.jar"
                                       + " [generic options] <in> <out>");
            System.out.println();
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }
        
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        job.setJobName("ImageJob");
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
/*      // Print Filename Job -
        // Using PrintKeysMapper and IntSumReducer (as reducer and combiner)

        job.setMapperClass(PrintKeysMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
*/

        // Print Filename Job -
        // Using PrintKeysMapper and IntSumReducer (as reducer and combiner)
        job.setMapperClass(BlurMapper.class);
        job.setCombinerClass(PassThroughReducer.class);
        job.setReducerClass(PassThroughReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean success = job.waitForCompletion(true);
        
        return success ? 0 : 1;
    }
}
