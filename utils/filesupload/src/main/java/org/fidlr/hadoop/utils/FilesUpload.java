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
import java.util.List;

/**
 * Created by omer on 7/15/14.
 */
public class FilesUpload {
    public static void print_usage() {
        System.out.printf("Usage: filesupload SRC_DIR DEST_DIR\n");
    }

    public static void main(String[] args) throws IOException {
        String src_uri = null;
        String dest_uri = null;
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        String[] fargs;

        Options options = new Options();
        options.addOption("r", "recurse", false,"recurse subdirectories");
        options.addOption("v", "verbose", false,"Print progress report to stdout");

        // Parse command line
        try {
            cmd = parser.parse( options, args);
            fargs = cmd.getArgs(); // get the remainder of the command line
            if (fargs.length != 2)
                throw new ParseException("Not enough arguments");

            src_uri = fargs[0];
            dest_uri = fargs[1];

        } catch (ParseException e) {

            e.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("filesupload", options);
            System.exit(1);
        }

        Configuration conf = new Configuration();
//        FileSystem dest_fs = FileSystem.get(URI.create(dest_uri), conf);
        FileSystem src_fs = FileSystem.get(URI.create(src_uri), conf);
        Path dest_path = new Path(dest_uri);
        Path src_path = new Path(src_uri);
        
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
                    System.out.printf("Uploading %s", src_path);
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