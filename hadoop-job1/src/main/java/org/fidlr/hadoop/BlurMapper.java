package org.fidlr.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;

import java.io.IOException;

/**
 * Turns lines from a text file into (word, 1) tuples.
 */
public class BlurMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

    /**
     * Called once at the beginning of the task.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            // Load OpenCV into the task
            System.out.println(System.getenv().toString());
            //System.loadLibrary("opencv_java248");
            //System.load("/usr/share/OpenCV/java/opencv-248.jar");
            System.load("/usr/lib/jni/libopencv_java248.so");
//            File file  = new File("/usr/share/OpenCV/java/opencv-248.jar");
//            URL url = file.toURI().toURL();
//            URL[] urls = new URL[]{url};
//            ClassLoader cl = new URLClassLoader(urls);
//
//            Class clsMat = cl.loadClass("org.opencv.core.Mat");

        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    protected void map(Text filename, BytesWritable value, Context context)
            throws IOException, InterruptedException {

        MatOfByte buf = new MatOfByte(value.getBytes());

        Mat img = Highgui.imdecode(buf, 0);
        if (img.empty())
        {
            System.out.println("can not open " + filename);
            return;
        }
        Mat cimg = new Mat();
        Imgproc.medianBlur(img, cimg, 5);

        Highgui.imwrite("/tmp/fd.jpg", cimg);

        if (!cimg.empty()/*cimg.asByteBuffer().hasArray()*/) {
            Highgui.imencode(".jpg",cimg,buf);
            context.write(filename, new BytesWritable(buf.toArray()));
        }

    }
}
