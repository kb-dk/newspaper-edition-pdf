package dk.statsbiblioteket.newspaper.editions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Simple mapper to get the edition as Key, so that the reducer works
 */
public class Utils  {


    public static Text getEdition(Text filePath) {
        return new Text(getEdition(filePath.toString()));
    }


    public static String getEdition(String filePath) {
        if (filePath.contains("_")){
            return filePath.split("_")[2];
        }
        return filePath.split("/")[2];
    }
}
