package org.fidlr.hadoop.utils;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

/**
 * Created by omer on 7/15/14.
 */
public class UploadFiles {
    public static void print_usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("uploadfiles [OPTIONS]... SRC_DIR DEST_DIR", options);
    }

    public static void main(String[] args) throws IOException {
        String src_dir = null;
        String dest_uri = null;
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        String[] fargs;

        Options options = new Options();
        options.addOption("r", "recurse", false,"recurse subdirectories");
        options.addOption("R", false,"same as \'-r\'");
        options.addOption("v", false,"print progress reports");

        // Parse command line
        try {
            cmd = parser.parse( options, args);
            fargs = cmd.getArgs(); // get the remainder of the command line
            if (fargs.length != 2) {
                System.out.println("Wrong number of arguments.");
                print_usage(options);
                System.exit(1);
            }


            src_dir = fargs[0];
            dest_uri = fargs[1];

        } catch (ParseException e) {

            e.printStackTrace();
            print_usage(options);
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem src_fs = FileSystem.getLocal(conf);
        Path dest_path = new Path(dest_uri);
        Path src_path = new Path(src_dir);
        
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(dest_path),
                                               SequenceFile.Writer.keyClass(key.getClass()),
                                               SequenceFile.Writer.valueClass(value.getClass()));
            
            FileStatus[] status = src_fs.listStatus(src_path);
            for (int i=0; i < status.length; i++) {

                Path cur_path = status[i].getPath();
                if (cmd.hasOption("v")) {
                    System.out.printf("Uploading %s", src_path.toString());
                }

                FSDataInputStream stream = new FSDataInputStream(src_fs.open(cur_path));
                byte[] file_content = org.apache.commons.io.IOUtils.toByteArray(stream);

                value.set(new BytesWritable(file_content));
                key.set(cur_path.getName());

                writer.append(key, value);
            }
            writer.sync();

        } finally {
            IOUtils.closeStream(writer);
        }
    }

}