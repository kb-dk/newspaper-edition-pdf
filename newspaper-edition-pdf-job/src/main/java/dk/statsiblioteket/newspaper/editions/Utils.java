package dk.statsiblioteket.newspaper.editions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Simple mapper to get the edition as Key, so that the reducer works
 */
public class Utils  {


    public static Text getEdition(Text filePath) {
        // B400022028241-RT1/400022028241-1/1795-06-15-02/adresseavisen1759-1795-06-15-02-0005A.jp2
        return new Text(filePath.toString().split("/")[2]);
    }


    public static String getEdition(String filePath) {
        // B400022028241-RT1/400022028241-1/1795-06-15-02/adresseavisen1759-1795-06-15-02-0005A.jp2
        return filePath.split("/")[2];
    }
}
