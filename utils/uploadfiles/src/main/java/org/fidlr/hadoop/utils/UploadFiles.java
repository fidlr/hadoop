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

/**
 * Created by omer on 7/15/14.
 */
public class UploadFiles {
    private static CommandLine cmd = null; /* CmdLine arguments */
    private static String srcDir; /* Directory with files to upload */
    private static String destUri; /* Destination folder to upload to */

    /**
     * Takes in the command line, parses it and places the
     * results in the member variables cmd, srcDir and destUri for later
     * reference by the file iterator. Prints usage if requested.
     *
     * @param args The argv passed to main()
     * @throws ParseException that will contain the message that should be printed
     */
    private static void parse_cmdline(String[] args)
            throws ParseException {

        CommandLineParser parser = new PosixParser();
        String[] fargs;

        Options options = new Options();
        options.addOption("r", "recurse", false,"recurse subdirectories");
        options.addOption("R", false,"same as \'-r\'");
        options.addOption("h", "help", false,"print usage");
        options.addOption("v", "verbose", false,"output progress");

        // Parse command line
        try {
            cmd = parser.parse( options, args);
            fargs = cmd.getArgs(); // get the remainder of the command line
            if (cmd.hasOption('h'))
            {
                print_usage(options);
                throw new ParseException(""); /* Exit the function and the program without a special message */
            }
            else if (fargs.length != 2) {
                print_usage(options);
                throw new ParseException("Wrong number of arguments.");
            }

            srcDir = fargs[0];
            destUri = fargs[1];


        } catch (ParseException e) {

            print_usage(options);
            throw e;
        }
    }

    public static void print_usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("uploadfiles [OPTION]... SRC_DIR DEST_URL", options);
    }

    public static void main(String[] args) throws IOException {

        /* Parse command line arguments */
        try {

            parse_cmdline(args);

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }

        /* Initialize variables */
        Configuration conf = new Configuration();
        conf.set("hadoop.job.ugi", "hdfs");
        FileSystem srcFs = FileSystem.getLocal(conf).getRawFileSystem();
        Path destPath = new Path(destUri);
        Path srcPath = new Path(srcDir);

        Text key = new Text();
        BytesWritable value = new BytesWritable();
        SequenceFile.Writer writer = null;

        /* Read input files and write to remote SequenceFile */
        try {
            writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(destPath),
                                               SequenceFile.Writer.keyClass(key.getClass()),
                                               SequenceFile.Writer.valueClass(value.getClass()));

            FileStatus[] status = srcFs.listStatus(srcPath);
            for (int i=0; i < status.length; i++) {

                Path cur_path = status[i].getPath();
                if (cmd.hasOption("v")) {
                    System.out.printf("Uploading %s\n", cur_path.toString());
                }

                FSDataInputStream stream = srcFs.open(cur_path);
                byte[] file_content = org.apache.commons.io.IOUtils.toByteArray(stream);

                value.set(new BytesWritable(file_content));
                key.set(cur_path.getName());

                writer.append(key, value);
            }
            writer.hflush();
            writer.close();

        } catch(Exception exp) {
            exp.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }

}