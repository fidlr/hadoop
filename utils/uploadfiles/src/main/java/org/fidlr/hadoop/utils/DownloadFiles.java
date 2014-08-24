package org.fidlr.hadoop.utils;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by omer on 8/24/14.
 */
public class DownloadFiles {
    private static CommandLine cmd = null; /* CmdLine arguments */
    private static String srcUri; /* Directory with files to upload */
    private static String destDir; /* Destination folder to upload to */

    /**
     * Takes in the command line, parses it and places the
     * results in the member variables cmd, srcUri and destDir for later
     * reference by the file iterator. Prints usage if requested.
     *
     * @param args The argv passed to main()
     * @throws org.apache.commons.cli.ParseException that will contain the message that should be printed
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
                throw new ParseException("\ne.g. java -jar downloadfiles.jar -v " +
                                         "hdfs://namenode:8020/user/me/pics.raw " +
                                         "/home/me/Pictures \"");
            }
            else if (fargs.length != 2) {
                throw new ParseException("Wrong number of arguments.");
            }

            srcUri = fargs[0];
            destDir = fargs[1];


        } catch (ParseException e) {

            print_usage(options);
            throw e;
        }
    }

    public static void print_usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("downloadfiles [OPTION]... SRC_URL DEST_DIR", options);
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
        FileSystem destFs = FileSystem.getLocal(conf).getRawFileSystem();
        Path destPath = new Path(destDir);
        Path srcPath = new Path(srcUri);

        Text key = new Text();
        BytesWritable value = new BytesWritable();
        SequenceFile.Reader reader = null;
        FSDataOutputStream stream = null;

        /* Read remote input SequenceFile and write to local files */
        try {
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(srcPath));

            while (reader.next(key, value))
            {
                if (cmd.hasOption("v")) {
                    System.out.printf("Downloading %s\n", key.toString());
                }

                // Concat the paths. HDFS doesn't support ':' in the filename so I
                // temporarily replaced it with '.'. The real solution should probably
                // use local file system APIs instead of HDFS, or at least fix all the
                // errors that might occur from using HDFS, and not just ':'.
                Path p = new Path(destPath, key.toString().replace(':','.'));
                stream = destFs.create(p);
                stream.write(value.getBytes());
                stream.close();
                stream = null;
            }

        } catch(Exception exp) {
            exp.printStackTrace();
        } finally {
            if (null != stream) {
                stream.close();
                stream = null;
            }
            IOUtils.closeStream(reader);
        }
    }

}


























