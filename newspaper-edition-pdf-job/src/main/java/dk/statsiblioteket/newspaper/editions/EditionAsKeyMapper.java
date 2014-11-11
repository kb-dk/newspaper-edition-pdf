package dk.statsiblioteket.newspaper.editions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Simple mapper to get the edition as Key, so that the reducer works
 */
public class EditionAsKeyMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(getEdition(value),value);
    }

    public static Text getEdition(Text filePath) {
        // B400022028241-RT1/400022028241-1/1795-06-15-02/adresseavisen1759-1795-06-15-02-0005A.jp2
        return new Text(filePath.toString().split("/")[2]);
    }
}
