package ivt.oas;

import com.sun.media.jai.codec.ByteArraySeekableStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.AppendTestUtil;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;


/**
 * Created by alex on 02.11.15.
 */
public class ConTiff {

    public static Configuration conf;
    public static FileSystem hdfs;
    public static String str_path;
    public static void main(String[] args) throws Exception
    {
        //String infilename = "/user/alex/tiff/Байкал/";
        if (args.length<1)
        {
            System.exit(0);

        }
        else {

            //BDVaribles bdVaribles = new BDVaribles();
            //bdVaribles.setConf(new Configuration());
            //bdVaribles.setInputDir(args[0].toString());

            conf = new Configuration();
            hdfs = FileSystem.get(conf);
            str_path = args[0].toString();
            Path inFile = new Path(str_path+"tiff");
            Path output = null;
            //BufferedWriter br = null;



            FileStatus[] status = hdfs.listStatus(inFile);

            for (int i = 0; i < status.length; i++)
                if (status[i].getPath().getName().indexOf("B3") > 0 ||
                        status[i].getPath().getName().indexOf("B4") > 0) {

                    getdatatiff(status[i]);
/*
                    output = new Path(args[0].toString() + "datatiff/" +
                            status[i].getPath().getName().toString().substring(0,
                                    status[i].getPath().getName().toString().length() - 4) + ".pd");
                    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(hdfs.create(output, true)));

                    br.write(status[i].getPath().getName().toString());
                    br.close();*/
                }

        }
    }
    public static void getdatatiff(FileStatus _fileStatus) throws IOException {


        FSDataInputStream _Stream = null;
        try {
            _Stream = hdfs.open(new Path(str_path + "tiff/" +
                    _fileStatus.getPath().getName().toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }


        ByteArrayOutputStream fimb = new ByteArrayOutputStream();//*


        byte[] buffer = new byte[1000];
        int readBytes = _Stream.read(buffer);
        while (readBytes > 0) {
            fimb.write(buffer, 0, readBytes);
            readBytes = _Stream.read(buffer);
        }

/*
        try {
            while((readBytes = _Stream.read(buffer)) > 0)
            {
                fimb.write(buffer,0,300);
                buffer = new byte[300];
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
*/


        byte[] formattedImageBytes; //*
        formattedImageBytes = fimb.toByteArray();
        fimb = null;
        //fimb.close();


        ByteArraySeekableStream stream = null;//*


        try {
            stream = new ByteArraySeekableStream(formattedImageBytes, 0, formattedImageBytes.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedImage bi = null;
        try {
            bi = ImageIO.read(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        stream = null;


        _Stream = null;


        Raster raster = bi.getData();
        bi = null;
        int width = raster.getWidth();
        int height = raster.getHeight();

        Path output = new Path(str_path + "datatiff/" +
                _fileStatus.getPath().getName().toString().substring(0,
                        _fileStatus.getPath().getName().toString().length() - 4) + ".pd");
        /*

        */


        int[] data  = new int[width];
        raster.getPixels(0, 4, width, 1, data);
        bi = null;
        String line = "";

        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(hdfs.create(output, true)));

        for (int indF = 0; indF < height; indF++) {

            raster.getPixels(0, 0 + indF, width, 1, data );

            line = Arrays.toString(data);

            line = line.replace("]", "");
            line = line.replace("[", "");
            line = line + "\n";

            br.write((indF + ";" + _fileStatus.getPath().getName().toString().substring
                    (0, _fileStatus.getPath().getName().toString().length() - 4) + ";" + line));

            line = "";

        }
        br.close();

    }

}


