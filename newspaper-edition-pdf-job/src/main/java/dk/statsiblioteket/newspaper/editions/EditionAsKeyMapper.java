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

    private Text getEdition(Text filePath) {
        //TODO
        return filePath;
    }
}
