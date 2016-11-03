package ivt.oas;

import com.sun.media.jai.codec.ByteArraySeekableStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;

/**
 * Created by alex on 02.11.15.
 */
public class GetDataTiff {

    public void GetDT(FileStatus _fileStatus, FileSystem _fs) throws IOException {


        FSDataInputStream _Stream = null;
        try {
            _Stream = _fs.open(new Path("/user/alex/NDVI/"+"tiff/" +
                    _fileStatus.getPath().getName().toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }



        ByteArrayOutputStream fimb = new ByteArrayOutputStream();//*


        byte[] buffer = new byte[100000];
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
        Path output = new Path("/user/alex/NDVI/"+"datatiff/" +
                _fileStatus.getPath().getName().toString().substring(0,
                        _fileStatus.getPath().getName().toString().length()-4)+".pd");
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(_fs.create(output,true)));

        //br.write(_fileStatus.getPath().getName().toString());
        br.write(String.valueOf(readBytes));
        //br.write(fimb.toByteArray().length);
        br.close();

        byte[] formattedImageBytes; //*
        formattedImageBytes = fimb.toByteArray();




        ByteArraySeekableStream stream = null;//*




        try {
            stream = new ByteArraySeekableStream(formattedImageBytes,0,formattedImageBytes.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedImage bi = null;
        try {
            bi = ImageIO.read(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }


        int width = bi.getData().getWidth();
        int height = bi.getData().getHeight();

        int[] data = new int[width*height];

        bi.getData().getPixels(0,0,width,height,data);



    }

}

/*
        buffer = null;
        fimb = null;
        formattedImageBytes = null;
        stream = null;
        bi = null;
*/